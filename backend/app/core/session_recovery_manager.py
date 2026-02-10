import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from dataclasses import dataclass
from enum import Enum

from telethon import TelegramClient
from telethon.errors import (
    AuthKeyUnregisteredError, 
    SessionPasswordNeededError,
    FloodWaitError,
    RPCError
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.app.models.telegram_account import TelegramAccount
from backend.app.core.api_rate_limiter import APIRateLimiter


class SessionHealth(Enum):
    """Enumeration for session health status."""
    HEALTHY = "healthy"
    DISCONNECTED = "disconnected"
    UNAUTHORIZED = "unauthorized"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class SessionStatus:
    """Data class for session status information."""
    account_id: int
    health: SessionHealth
    last_check: datetime
    error_message: Optional[str] = None
    reconnect_attempts: int = 0
    last_successful_operation: Optional[datetime] = None
    rate_limit_until: Optional[datetime] = None


class SessionRecoveryManager:
    """
    Manages Telegram session health monitoring, disconnection detection,
    and automatic recovery with fallback account rotation.
    """
    
    def __init__(self, rate_limiter: Optional[APIRateLimiter] = None):
        self.logger = logging.getLogger(__name__)
        self.rate_limiter = rate_limiter or APIRateLimiter()
        
        # Session status tracking
        self.session_status: Dict[int, SessionStatus] = {}
        
        # Configuration
        self.max_reconnect_attempts = 3
        self.reconnect_delay_base = 5  # seconds
        self.health_check_interval = 60  # seconds
        self.session_timeout = 300  # seconds
        
        # Background tasks
        self._health_monitor_task: Optional[asyncio.Task] = None
        self._is_monitoring = False
        
        # Account rotation
        self._backup_accounts: List[int] = []
        self._primary_accounts: Dict[int, int] = {}  # failed_account_id -> backup_account_id
        
        # CRITICAL FIX: Reconnection locks to prevent concurrent reconnection storms
        self._reconnection_locks: Dict[int, asyncio.Lock] = {}
    
    async def ensure_session_active(self, account_id: int) -> Optional[TelegramClient]:
        """
        Ensures that a Telegram session is active and healthy.
        
        Args:
            account_id: The account ID to check
            
        Returns:
            TelegramClient if session is active, None otherwise
        """
        try:
            # Import here to avoid circular imports
            from backend.app.services.telegram_service import telegram_manager
            
            # Check if client exists and is connected
            client = telegram_manager.clients.get(account_id)
            if not client:
                self.logger.warning(f"No client found for account {account_id}")
                return await self._attempt_reconnection(account_id)
            
            # Check connection status
            if not client.is_connected():
                self.logger.info(f"Client {account_id} is disconnected, attempting reconnection")
                return await self._attempt_reconnection(account_id)
            
            # Check authorization status
            try:
                if not await client.is_user_authorized():
                    self.logger.warning(f"Client {account_id} is not authorized")
                    await self._update_session_status(
                        account_id, 
                        SessionHealth.UNAUTHORIZED,
                        "Session not authorized"
                    )
                    return None
            except Exception as e:
                self.logger.error(f"Error checking authorization for account {account_id}: {e}")
                return await self._attempt_reconnection(account_id)
            
            # Update status as healthy
            await self._update_session_status(account_id, SessionHealth.HEALTHY)
            return client
            
        except Exception as e:
            self.logger.error(f"Error ensuring session active for account {account_id}: {e}")
            await self._update_session_status(
                account_id, 
                SessionHealth.ERROR,
                str(e)
            )
            return None
    
    async def handle_disconnection(self, account_id: int, error: Exception) -> Optional[TelegramClient]:
        """
        Handles session disconnection with intelligent recovery.
        
        Args:
            account_id: The account ID that disconnected
            error: The error that caused the disconnection
            
        Returns:
            TelegramClient if recovery successful, None otherwise
        """
        self.logger.warning(f"Handling disconnection for account {account_id}: {error}")
        
        # Update session status
        error_msg = str(error)
        if isinstance(error, FloodWaitError):
            health = SessionHealth.RATE_LIMITED
            # Calculate rate limit end time
            rate_limit_until = datetime.utcnow() + timedelta(seconds=error.seconds)
            status = self.session_status.get(account_id, SessionStatus(
                account_id=account_id,
                health=health,
                last_check=datetime.utcnow()
            ))
            status.rate_limit_until = rate_limit_until
            self.session_status[account_id] = status
        elif isinstance(error, AuthKeyUnregisteredError):
            health = SessionHealth.UNAUTHORIZED
        elif isinstance(error, (ConnectionError, OSError)):
            health = SessionHealth.DISCONNECTED
        else:
            health = SessionHealth.ERROR
        
        await self._update_session_status(account_id, health, error_msg)
        
        # Handle rate limiting
        if isinstance(error, FloodWaitError):
            self.logger.info(f"Rate limited for {error.seconds} seconds, waiting...")
            await asyncio.sleep(min(error.seconds, 300))  # Cap at 5 minutes
        
        # Attempt recovery
        return await self._attempt_reconnection(account_id)
    
    async def rotate_to_backup_account(self, failed_account_id: int) -> Optional[TelegramClient]:
        """
        Rotates to a backup account when the primary account fails.
        
        Args:
            failed_account_id: The account ID that failed
            
        Returns:
            TelegramClient of backup account if available, None otherwise
        """
        self.logger.info(f"Attempting to rotate from failed account {failed_account_id}")
        
        # Check if we already have a backup for this account
        if failed_account_id in self._primary_accounts:
            backup_id = self._primary_accounts[failed_account_id]
            backup_client = await self.ensure_session_active(backup_id)
            if backup_client:
                self.logger.info(f"Successfully rotated to existing backup account {backup_id}")
                return backup_client
        
        # Find an available backup account
        for backup_id in self._backup_accounts:
            if backup_id == failed_account_id:
                continue  # Don't use the failed account as backup
            
            # Check if this backup is already in use
            if backup_id in self._primary_accounts.values():
                continue
            
            # Try to activate the backup account
            backup_client = await self.ensure_session_active(backup_id)
            if backup_client:
                self._primary_accounts[failed_account_id] = backup_id
                self.logger.info(f"Successfully rotated to backup account {backup_id}")
                return backup_client
        
        self.logger.error(f"No available backup accounts for failed account {failed_account_id}")
        return None
    
    async def monitor_session_health(self) -> Dict[int, SessionStatus]:
        """
        Monitors the health of all active sessions.
        
        Returns:
            Dictionary of account_id -> SessionStatus
        """
        from backend.app.services.telegram_service import telegram_manager
        
        current_time = datetime.utcnow()
        
        for account_id, client in telegram_manager.clients.items():
            try:
                # Skip if recently checked
                if account_id in self.session_status:
                    last_check = self.session_status[account_id].last_check
                    if (current_time - last_check).seconds < self.health_check_interval:
                        continue
                
                # Perform health check
                if not client.is_connected():
                    await self._update_session_status(
                        account_id, 
                        SessionHealth.DISCONNECTED,
                        "Client not connected"
                    )
                    continue
                
                # Check authorization with timeout
                try:
                    authorized = await asyncio.wait_for(
                        client.is_user_authorized(),
                        timeout=10.0
                    )
                    if not authorized:
                        await self._update_session_status(
                            account_id, 
                            SessionHealth.UNAUTHORIZED,
                            "Session not authorized"
                        )
                        continue
                except asyncio.TimeoutError:
                    await self._update_session_status(
                        account_id, 
                        SessionHealth.ERROR,
                        "Health check timeout"
                    )
                    continue
                
                # Session is healthy
                await self._update_session_status(account_id, SessionHealth.HEALTHY)
                
            except Exception as e:
                self.logger.error(f"Error checking health for account {account_id}: {e}")
                await self._update_session_status(
                    account_id, 
                    SessionHealth.ERROR,
                    str(e)
                )
        
        return self.session_status.copy()
    
    async def start_health_monitoring(self):
        """Starts the background health monitoring task."""
        if self._is_monitoring:
            return
        
        self._is_monitoring = True
        self._health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        self.logger.info("Started session health monitoring")
    
    async def stop_health_monitoring(self):
        """Stops the background health monitoring task."""
        self._is_monitoring = False
        if self._health_monitor_task:
            self._health_monitor_task.cancel()
            try:
                await self._health_monitor_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped session health monitoring")
    
    async def add_backup_account(self, account_id: int):
        """Adds an account to the backup accounts list."""
        if account_id not in self._backup_accounts:
            self._backup_accounts.append(account_id)
            self.logger.info(f"Added backup account {account_id}")
    
    async def remove_backup_account(self, account_id: int):
        """Removes an account from the backup accounts list."""
        if account_id in self._backup_accounts:
            self._backup_accounts.remove(account_id)
            self.logger.info(f"Removed backup account {account_id}")
    
    async def get_session_statistics(self) -> Dict[str, Any]:
        """Returns statistics about session health and recovery."""
        total_sessions = len(self.session_status)
        healthy_sessions = sum(1 for s in self.session_status.values() if s.health == SessionHealth.HEALTHY)
        
        health_counts = {}
        for health in SessionHealth:
            health_counts[health.value] = sum(
                1 for s in self.session_status.values() if s.health == health
            )
        
        return {
            "total_sessions": total_sessions,
            "healthy_sessions": healthy_sessions,
            "health_distribution": health_counts,
            "backup_accounts": len(self._backup_accounts),
            "active_rotations": len(self._primary_accounts),
            "monitoring_active": self._is_monitoring
        }
    
    async def _attempt_reconnection(self, account_id: int) -> Optional[TelegramClient]:
        """
        Attempts to reconnect a session with exponential backoff.
        Uses a lock to prevent concurrent reconnection attempts for the same account.
        
        Args:
            account_id: The account ID to reconnect
            
        Returns:
            TelegramClient if successful, None otherwise
        """
        # CRITICAL FIX: Use lock to prevent reconnection storm
        if account_id not in self._reconnection_locks:
            self._reconnection_locks[account_id] = asyncio.Lock()
        
        # Try to acquire lock - if already locked, another reconnection is in progress
        if self._reconnection_locks[account_id].locked():
            self.logger.info(f"Reconnection already in progress for account {account_id}, skipping")
            # Wait for the ongoing reconnection to complete
            async with self._reconnection_locks[account_id]:
                pass
            # Check if reconnection was successful
            from backend.app.services.telegram_service import telegram_manager
            client = telegram_manager.clients.get(account_id)
            if client and client.is_connected():
                return client
            return None
        
        async with self._reconnection_locks[account_id]:
            from backend.app.services.telegram_service import telegram_manager
            from backend.app.db.database import async_session_maker
            
            status = self.session_status.get(account_id, SessionStatus(
                account_id=account_id,
                health=SessionHealth.UNKNOWN,
                last_check=datetime.utcnow()
            ))
            
            if status.reconnect_attempts >= self.max_reconnect_attempts:
                self.logger.error(f"Max reconnection attempts reached for account {account_id}")
                return None
            
            try:
                # Calculate delay with exponential backoff
                delay = self.reconnect_delay_base * (2 ** status.reconnect_attempts)
                self.logger.info(f"Attempting reconnection for account {account_id} (attempt {status.reconnect_attempts + 1}) after {delay}s delay")
                
                await asyncio.sleep(delay)
                
                # Get account from database
                async with async_session_maker() as db:
                    result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
                    account = result.scalar_one_or_none()
                    
                    if not account:
                        self.logger.error(f"Account {account_id} not found in database")
                        return None
                    
                    # Create new client
                    client = await telegram_manager.create_client(account)
                    await client.connect()
                    
                    # Check authorization
                    if not await client.is_user_authorized():
                        self.logger.warning(f"Account {account_id} not authorized after reconnection")
                        await self._update_session_status(
                            account_id, 
                            SessionHealth.UNAUTHORIZED,
                            "Not authorized after reconnection"
                        )
                        return None
                    
                    # Update client in manager
                    telegram_manager.clients[account_id] = client
                    
                    # Reset reconnection attempts and update status
                    status.reconnect_attempts = 0
                    status.last_successful_operation = datetime.utcnow()
                    await self._update_session_status(account_id, SessionHealth.HEALTHY)
                    
                    self.logger.info(f"Successfully reconnected account {account_id}")
                    return client
                    
            except Exception as e:
                status.reconnect_attempts += 1
                self.logger.error(f"Reconnection attempt {status.reconnect_attempts} failed for account {account_id}: {e}")
                await self._update_session_status(
                    account_id, 
                    SessionHealth.ERROR,
                    f"Reconnection failed: {str(e)}"
                )
                return None
    
    async def _update_session_status(
        self, 
        account_id: int, 
        health: SessionHealth, 
        error_message: Optional[str] = None
    ):
        """Updates the session status for an account."""
        current_time = datetime.utcnow()
        
        if account_id in self.session_status:
            status = self.session_status[account_id]
            status.health = health
            status.last_check = current_time
            status.error_message = error_message
            if health == SessionHealth.HEALTHY:
                status.last_successful_operation = current_time
        else:
            status = SessionStatus(
                account_id=account_id,
                health=health,
                last_check=current_time,
                error_message=error_message,
                last_successful_operation=current_time if health == SessionHealth.HEALTHY else None
            )
            self.session_status[account_id] = status
    
    async def _health_monitor_loop(self):
        """Background loop for monitoring session health."""
        while self._is_monitoring:
            try:
                await self.monitor_session_health()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health monitor loop: {e}")
                await asyncio.sleep(self.health_check_interval)