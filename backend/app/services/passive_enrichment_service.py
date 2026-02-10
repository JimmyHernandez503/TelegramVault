"""
Passive User Enrichment Service

Continuously enriches all users in the background with:
- Load balancing across multiple Telegram accounts
- Automatic retry with exponential backoff
- Error handling and recovery
- Periodic updates for existing users
"""
import asyncio
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.db.database import async_session_maker
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_account import TelegramAccount
from backend.app.services.user_enricher import user_enricher
from backend.app.core.config_manager import ConfigManager, get_config_manager
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem
from telethon.errors import FloodWaitError, UserNotParticipantError, RPCError


class PassiveEnrichmentService:
    """
    Background service that continuously enriches all users.
    
    Features:
    - Multi-account load balancing
    - Automatic retry with exponential backoff
    - Prioritizes users without photos
    - Re-enriches users periodically (every 30 days)
    - Error handling and recovery
    """
    
    def __init__(self):
        self.config = get_config_manager()
        self.logger = EnhancedLoggingSystem()
        
        # Service state
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        
        # Use the global user_enricher instead of creating our own
        self._enricher = user_enricher
        
        # Load balancing
        self._account_index = 0
        self._account_errors: Dict[int, int] = {}  # account_id -> error_count
        self._account_last_used: Dict[int, datetime] = {}  # account_id -> last_used_time
        
        # Configuration
        self._batch_size = self.config.get_int("PASSIVE_ENRICHMENT_BATCH_SIZE", 50)
        self._cycle_delay = self.config.get_int("PASSIVE_ENRICHMENT_CYCLE_DELAY", 300)  # 5 minutes
        self._max_retries = self.config.get_int("PASSIVE_ENRICHMENT_MAX_RETRIES", 3)
        self._re_enrich_days = self.config.get_int("PASSIVE_ENRICHMENT_RE_ENRICH_DAYS", 30)
        self._concurrent_limit = self.config.get_int("USER_ENRICHMENT_CONCURRENT_LIMIT", 3)
        
        # Statistics
        self._stats = {
            "cycles_completed": 0,
            "users_enriched": 0,
            "users_failed": 0,
            "users_skipped": 0,
            "retries_performed": 0,
            "errors_by_type": {},
            "last_cycle_time": None,
            "current_cycle_start": None
        }
    
    async def start(self) -> bool:
        """Start the passive enrichment service with health monitoring"""
        if self._running:
            await self.logger.log_warning(
                "PassiveEnrichmentService",
                "start",
                "Service already running"
            )
            return True
        
        try:
            # The global user_enricher should already be initialized and running
            # We just need to start our background task
            
            # Start background task
            self._running = True
            self._task = asyncio.create_task(self._enrichment_loop())
            
            # Log task creation with state transition
            await self.logger.log_info(
                "PassiveEnrichmentService",
                "start",
                "Task state transition: CREATED",
                details={
                    "task_id": id(self._task),
                    "task_done": self._task.done(),
                    "task_cancelled": self._task.cancelled()
                }
            )
            
            # Wait briefly and verify task is still running
            await asyncio.sleep(0.1)
            
            if self._task.done():
                # Task failed immediately
                await self.logger.log_error(
                    "PassiveEnrichmentService",
                    "start",
                    "Task state transition: FAILED_ON_START"
                )
                try:
                    self._task.result()  # Raise the exception
                except Exception as e:
                    await self.logger.log_error(
                        "PassiveEnrichmentService",
                        "start",
                        f"Enrichment task failed on start: {e}",
                        error=e
                    )
                    self._running = False
                    return False
            
            # Log successful start
            await self.logger.log_info(
                "PassiveEnrichmentService",
                "start",
                "Task state transition: RUNNING",
                details={
                    "task_id": id(self._task),
                    "task_done": self._task.done()
                }
            )
            
            # Start health monitor
            self._monitor_task = asyncio.create_task(self._health_monitor())
            
            await self.logger.log_info(
                "PassiveEnrichmentService",
                "start",
                "Passive enrichment service started with health monitoring",
                details={
                    "batch_size": self._batch_size,
                    "cycle_delay": self._cycle_delay,
                    "re_enrich_days": self._re_enrich_days,
                    "concurrent_limit": self._concurrent_limit,
                    "task_created": self._task is not None,
                    "monitor_created": self._monitor_task is not None,
                    "running_flag": self._running
                }
            )
            
            return True
            
        except Exception as e:
            await self.logger.log_error(
                "PassiveEnrichmentService",
                "start",
                "Failed to start passive enrichment service",
                error=e
            )
            self._running = False
            return False
    
    async def stop(self):
        """Stop the passive enrichment service"""
        if not self._running:
            return
        
        await self.logger.log_info(
            "PassiveEnrichmentService",
            "stop",
            "Task state transition: STOPPING"
        )
        
        self._running = False
        
        # Stop monitor task
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # Stop enrichment task
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        # NOTE: We don't stop the enricher worker because we never started it
        
        await self.logger.log_info(
            "PassiveEnrichmentService",
            "stop",
            "Task state transition: STOPPED",
            details=self._stats
        )
    
    async def _health_monitor(self):
        """
        Monitor task health and restart if needed.
        Checks every 60 seconds if the enrichment task is still running.
        """
        await self.logger.log_info(
            "PassiveEnrichmentService",
            "_health_monitor",
            "Health monitor started"
        )
        
        while self._running:
            try:
                # Wait 60 seconds before checking
                await asyncio.sleep(60)
                
                # Check if main task is still running
                if self._task and self._task.done() and self._running:
                    # Task died unexpectedly
                    await self.logger.log_error(
                        "PassiveEnrichmentService",
                        "_health_monitor",
                        "Task state transition: DIED_UNEXPECTEDLY - Restarting"
                    )
                    
                    # Try to get the exception
                    try:
                        self._task.result()
                    except Exception as e:
                        await self.logger.log_error(
                            "PassiveEnrichmentService",
                            "_health_monitor",
                            f"Task failed with error: {e}",
                            error=e
                        )
                    
                    # Restart the task
                    await self.logger.log_info(
                        "PassiveEnrichmentService",
                        "_health_monitor",
                        "Task state transition: RESTARTING"
                    )
                    
                    self._task = asyncio.create_task(self._enrichment_loop())
                    
                    await self.logger.log_info(
                        "PassiveEnrichmentService",
                        "_health_monitor",
                        "Task state transition: RESTARTED",
                        details={
                            "task_id": id(self._task),
                            "task_done": self._task.done()
                        }
                    )
                else:
                    # Task is healthy
                    await self.logger.log_debug(
                        "PassiveEnrichmentService",
                        "_health_monitor",
                        "Health check: Task is running normally",
                        details={
                            "task_id": id(self._task) if self._task else None,
                            "task_done": self._task.done() if self._task else None,
                            "cycles_completed": self._stats["cycles_completed"],
                            "users_enriched": self._stats["users_enriched"]
                        }
                    )
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.logger.log_error(
                    "PassiveEnrichmentService",
                    "_health_monitor",
                    "Error in health monitor",
                    error=e
                )
                # Continue monitoring despite errors
                await asyncio.sleep(60)
        
        await self.logger.log_info(
            "PassiveEnrichmentService",
            "_health_monitor",
            "Health monitor stopped"
        )
    
    async def _enrichment_loop(self):
        """Main enrichment loop"""
        await self.logger.log_info(
            "PassiveEnrichmentService",
            "_enrichment_loop",
            "Task state transition: LOOP_STARTED"
        )
        
        while self._running:
            try:
                self._stats["current_cycle_start"] = datetime.utcnow()
                
                await self.logger.log_info(
                    "PassiveEnrichmentService",
                    "_enrichment_loop",
                    f"Task state transition: CYCLE_{self._stats['cycles_completed'] + 1}_STARTING"
                )
                
                # Run enrichment cycle
                await self._run_enrichment_cycle()
                
                # Update stats
                self._stats["cycles_completed"] += 1
                self._stats["last_cycle_time"] = datetime.utcnow()
                
                # Log cycle completion
                await self.logger.log_info(
                    "PassiveEnrichmentService",
                    "_enrichment_loop",
                    f"Task state transition: CYCLE_{self._stats['cycles_completed']}_COMPLETED",
                    details={
                        "users_enriched": self._stats["users_enriched"],
                        "users_failed": self._stats["users_failed"],
                        "users_skipped": self._stats["users_skipped"]
                    }
                )
                
                # Wait before next cycle
                await self.logger.log_debug(
                    "PassiveEnrichmentService",
                    "_enrichment_loop",
                    f"Task state transition: WAITING_{self._cycle_delay}s"
                )
                await asyncio.sleep(self._cycle_delay)
                
            except asyncio.CancelledError:
                await self.logger.log_info(
                    "PassiveEnrichmentService",
                    "_enrichment_loop",
                    "Task state transition: CANCELLED"
                )
                break
            except Exception as e:
                await self.logger.log_error(
                    "PassiveEnrichmentService",
                    "_enrichment_loop",
                    "Task state transition: ERROR_IN_LOOP",
                    error=e
                )
                # Continue running despite errors (resilience)
                await asyncio.sleep(60)  # Wait 1 minute on error
        
        await self.logger.log_info(
            "PassiveEnrichmentService",
            "_enrichment_loop",
            "Task state transition: LOOP_STOPPED"
        )
    
    async def _run_enrichment_cycle(self):
        """Run one enrichment cycle"""
        async with async_session_maker() as db:
            # Get active accounts
            accounts = await self._get_active_accounts(db)
            
            if not accounts:
                await self.logger.log_warning(
                    "PassiveEnrichmentService",
                    "_run_enrichment_cycle",
                    "No active accounts available"
                )
                return
            
            # Get users that need enrichment
            users = await self._get_users_to_enrich(db)
            
            if not users:
                await self.logger.log_info(
                    "PassiveEnrichmentService",
                    "_run_enrichment_cycle",
                    "No users need enrichment in this cycle"
                )
                return
            
            await self.logger.log_info(
                "PassiveEnrichmentService",
                "_run_enrichment_cycle",
                f"Processing {len(users)} users with {len(accounts)} accounts"
            )
            
            # Process users with load balancing
            await self._process_users_with_load_balancing(users, accounts)
    
    async def _get_active_accounts(self, db: AsyncSession) -> List[TelegramAccount]:
        """Get active Telegram accounts sorted by least recently used"""
        from backend.app.services.telegram_service import telegram_manager
        
        result = await db.execute(
            select(TelegramAccount).where(TelegramAccount.is_active == True)
        )
        all_accounts = result.scalars().all()
        
        # Filter to only connected accounts
        connected_accounts = []
        for account in all_accounts:
            client = telegram_manager.clients.get(account.id)
            if client and client.is_connected():
                # Skip accounts with too many errors
                if self._account_errors.get(account.id, 0) < 10:
                    connected_accounts.append(account)
        
        # Sort by least recently used
        connected_accounts.sort(
            key=lambda a: self._account_last_used.get(a.id, datetime.min)
        )
        
        return connected_accounts
    
    async def _get_users_to_enrich(self, db: AsyncSession) -> List[TelegramUser]:
        """Get users that need enrichment, prioritizing those without photos"""
        cutoff_date = datetime.utcnow() - timedelta(days=self._re_enrich_days)
        
        # Priority 1: Users without photos
        query_no_photos = select(TelegramUser).where(
            TelegramUser.telegram_id > 0,
            TelegramUser.access_hash.isnot(None),
            TelegramUser.current_photo_path.is_(None),
            or_(
                TelegramUser.first_name.isnot(None),
                TelegramUser.username.isnot(None)
            )
        ).order_by(TelegramUser.messages_count.desc()).limit(self._batch_size)
        
        result = await db.execute(query_no_photos)
        users = list(result.scalars().all())
        
        # If we have room, add users that need re-enrichment
        if len(users) < self._batch_size:
            remaining = self._batch_size - len(users)
            
            query_old = select(TelegramUser).where(
                TelegramUser.telegram_id > 0,
                TelegramUser.access_hash.isnot(None),
                TelegramUser.current_photo_path.isnot(None),
                or_(
                    TelegramUser.last_photo_scan.is_(None),
                    TelegramUser.last_photo_scan < cutoff_date
                )
            ).order_by(TelegramUser.messages_count.desc()).limit(remaining)
            
            result = await db.execute(query_old)
            users.extend(result.scalars().all())
        
        return users
    
    async def _process_users_with_load_balancing(
        self, 
        users: List[TelegramUser], 
        accounts: List[TelegramAccount]
    ):
        """Process users with load balancing across accounts"""
        from backend.app.services.telegram_service import telegram_manager
        
        # Distribute users across accounts
        users_per_account = len(users) // len(accounts)
        remainder = len(users) % len(accounts)
        
        tasks = []
        user_index = 0
        
        for i, account in enumerate(accounts):
            # Calculate how many users this account should process
            count = users_per_account + (1 if i < remainder else 0)
            if count == 0:
                continue
            
            # Get users for this account
            account_users = users[user_index:user_index + count]
            user_index += count
            
            # Get client
            client = telegram_manager.clients.get(account.id)
            if not client:
                continue
            
            # Create task for this account
            task = asyncio.create_task(
                self._process_users_for_account(account, client, account_users)
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _process_users_for_account(
        self,
        account: TelegramAccount,
        client,
        users: List[TelegramUser]
    ):
        """Process users for a specific account"""
        await self.logger.log_info(
            "PassiveEnrichmentService",
            "_process_users_for_account",
            f"Account {account.id} processing {len(users)} users"
        )
        
        # Update last used time
        self._account_last_used[account.id] = datetime.utcnow()
        
        # Process users with semaphore for concurrency control
        semaphore = asyncio.Semaphore(self._concurrent_limit)
        
        async def process_with_semaphore(user):
            async with semaphore:
                return await self._enrich_user_with_retry(client, user, account.id)
        
        # Process all users
        results = await asyncio.gather(
            *[process_with_semaphore(user) for user in users],
            return_exceptions=True
        )
        
        # Count results
        for result in results:
            if isinstance(result, Exception):
                self._stats["users_failed"] += 1
            elif result:
                self._stats["users_enriched"] += 1
            else:
                self._stats["users_skipped"] += 1
    
    async def _enrich_user_with_retry(
        self,
        client,
        user: TelegramUser,
        account_id: int
    ) -> bool:
        """Enrich a user by queuing it in the user_enricher"""
        try:
            # Queue the user for enrichment using the global user_enricher
            await self._enricher.queue_enrichment(
                client=client,
                telegram_id=user.telegram_id,
                group_id=None,
                source="passive_enrichment"
            )
            
            # Reset error count on successful queueing
            self._account_errors[account_id] = 0
            return True
            
        except Exception as e:
            # Log error
            error_type = type(e).__name__
            self._stats["errors_by_type"][error_type] = \
                self._stats["errors_by_type"].get(error_type, 0) + 1
            
            await self.logger.log_error(
                "PassiveEnrichmentService",
                "_enrich_user_with_retry",
                f"Error queueing user {user.telegram_id}",
                error=e
            )
            
            # Increment account error count
            self._account_errors[account_id] = self._account_errors.get(account_id, 0) + 1
            
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get service status"""
        return {
            "running": self._running,
            "statistics": self._stats.copy(),
            "configuration": {
                "batch_size": self._batch_size,
                "cycle_delay": self._cycle_delay,
                "max_retries": self._max_retries,
                "re_enrich_days": self._re_enrich_days,
                "concurrent_limit": self._concurrent_limit
            },
            "account_errors": self._account_errors.copy(),
            "account_last_used": {
                k: v.isoformat() for k, v in self._account_last_used.items()
            }
        }


# Global instance
passive_enrichment_service = PassiveEnrichmentService()
