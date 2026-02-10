import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func

from backend.app.models.telegram_user import TelegramUser
from backend.app.models.config import GlobalConfig
from backend.app.db.database import async_session_maker
from backend.app.services.client_load_balancer import load_balancer
from backend.app.services.live_stats import live_stats

logger = logging.getLogger("story_monitor")


class StoryMonitor:
    def __init__(self):
        self._running = False
        self._task = None
        self._check_interval = 300
        self._default_story_interval = 1
        self._default_batch_size = 50
        self._default_parallel_workers = 5
        self._last_check = None
        self._current_offset = 0
        self._force_check = False
        self._stats = {
            "users_checked": 0,
            "users_with_stories": 0,
            "stories_downloaded": 0,
            "errors": 0,
            "last_run": None,
            "is_scanning": False,
            "current_offset": 0,
            "total_users": 0
        }
    
    def get_status(self) -> dict:
        return {
            "running": self._running,
            "is_scanning": self._stats.get("is_scanning", False),
            "users_checked": self._stats.get("users_checked", 0),
            "users_with_stories": self._stats.get("users_with_stories", 0),
            "stories_downloaded": self._stats.get("stories_downloaded", 0),
            "errors": self._stats.get("errors", 0),
            "last_run": self._stats.get("last_run"),
            "current_offset": self._stats.get("current_offset", 0),
            "total_users": self._stats.get("total_users", 0)
        }
    
    async def start(self, telegram_manager):
        if self._running:
            return
        
        self._running = True
        self.manager = telegram_manager
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("[StoryMonitor] Started - Multi-account load balanced mode")
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[StoryMonitor] Stopped")
    
    async def _get_config(self, db: AsyncSession) -> tuple:
        result = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "story_check_interval_hours")
        )
        config = result.scalar_one_or_none()
        interval_hours = float(config.value) if config and config.value else self._default_story_interval
        
        result2 = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "story_batch_size")
        )
        config2 = result2.scalar_one_or_none()
        batch_size = int(config2.value) if config2 and config2.value else self._default_batch_size
        
        result3 = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "story_parallel_workers")
        )
        config3 = result3.scalar_one_or_none()
        parallel_workers = int(config3.value) if config3 and config3.value else self._default_parallel_workers
        
        return interval_hours, batch_size, parallel_workers
    
    def _refresh_load_balancer(self):
        load_balancer.register_clients(self.manager.clients)
    
    async def _monitor_loop(self):
        await asyncio.sleep(30)
        
        while self._running:
            try:
                if self._force_check:
                    self._force_check = False
                    async with async_session_maker() as db:
                        _, batch_size, parallel_workers = await self._get_config(db)
                    logger.info("[StoryMonitor] Force check - starting immediate scan")
                    self._refresh_load_balancer()
                    await self._scan_all_users_balanced(batch_size, parallel_workers)
                    self._last_check = datetime.utcnow()
                    self._stats["last_run"] = self._last_check.isoformat()
                    await asyncio.sleep(self._check_interval)
                    continue
                
                if load_balancer.all_clients_blocked():
                    min_wait = load_balancer.get_min_flood_wait_remaining()
                    if min_wait > 0:
                        logger.info(f"[StoryMonitor] All clients blocked, waiting {min_wait}s")
                        await asyncio.sleep(min(min_wait, 60))
                        continue
                
                async with async_session_maker() as db:
                    interval_hours, batch_size, parallel_workers = await self._get_config(db)
                    
                    if interval_hours <= 0:
                        await asyncio.sleep(self._check_interval)
                        continue
                
                now = datetime.utcnow()
                if self._last_check:
                    hours_since_last = (now - self._last_check).total_seconds() / 3600
                    if hours_since_last < interval_hours:
                        await asyncio.sleep(self._check_interval)
                        continue
                
                self._refresh_load_balancer()
                available = len(load_balancer.get_available_clients())
                logger.info(f"[StoryMonitor] Starting scan with {available} accounts (batch={batch_size}, workers={parallel_workers})")
                await self._scan_all_users_balanced(batch_size, parallel_workers)
                self._last_check = now
                self._stats["last_run"] = now.isoformat()
                    
            except Exception as e:
                logger.error(f"[StoryMonitor] Error in main loop: {e}")
                self._stats["errors"] += 1
            
            await asyncio.sleep(self._check_interval)
    
    async def _scan_all_users_balanced(self, batch_size: int = 50, parallel_workers: int = 5):
        from backend.app.services.story_service import StoryService
        from telethon.errors import FloodWaitError
        
        self._refresh_load_balancer()
        
        available_clients = load_balancer.get_available_clients()
        if not available_clients:
            logger.warning("[StoryMonitor] No available clients")
            return
        
        num_accounts = len(available_clients)
        logger.info(f"[StoryMonitor] Using {num_accounts} account(s) for parallel download")
        
        self._stats["is_scanning"] = True
        offset = 0
        total_checked = 0
        total_with_stories = 0
        total_downloaded = 0
        
        async with async_session_maker() as db:
            count_result = await db.execute(
                select(func.count()).select_from(TelegramUser).where(TelegramUser.access_hash != None)
            )
            self._stats["total_users"] = count_result.scalar() or 0
        
        effective_workers = min(parallel_workers, num_accounts * 2)
        
        while True:
            if load_balancer.all_clients_blocked():
                min_wait = load_balancer.get_min_flood_wait_remaining()
                logger.info(f"[StoryMonitor] All accounts blocked, waiting {min_wait}s")
                if min_wait > 0:
                    await asyncio.sleep(min(min_wait, 60))
                    self._refresh_load_balancer()
                    if load_balancer.all_clients_blocked():
                        break
            
            async with async_session_maker() as db:
                result = await db.execute(
                    select(TelegramUser)
                    .where(TelegramUser.access_hash != None)
                    .order_by(TelegramUser.id)
                    .offset(offset)
                    .limit(batch_size)
                )
                users = list(result.scalars().all())
                
                if not users:
                    break
                
                self._stats["current_offset"] = offset
                self._current_offset = offset
                logger.info(f"[StoryMonitor] Processing {len(users)} users (offset={offset})")
                
                semaphore = asyncio.Semaphore(effective_workers)
                
                async def process_user(user):
                    async with semaphore:
                        client_info = await load_balancer.get_next_client()
                        if not client_info:
                            return None, 0, None
                        
                        account_id, client = client_info
                        try:
                            has_stories, downloaded = await self._check_user_stories(client, user)
                            load_balancer.report_success(account_id, stories=downloaded)
                            return has_stories, downloaded, None
                        except FloodWaitError as e:
                            load_balancer.report_flood_wait(account_id, e.seconds)
                            return None, 0, e
                        except Exception as e:
                            load_balancer.report_error(account_id)
                            return None, 0, e
                
                tasks = [process_user(user) for user in users]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        self._stats["errors"] += 1
                        continue
                    
                    if not isinstance(result, tuple):
                        continue
                    
                    has_stories, downloaded, error = result
                    if error:
                        continue
                    
                    if has_stories is not None:
                        total_checked += 1
                        if has_stories:
                            total_with_stories += 1
                        total_downloaded += downloaded
                
            offset += batch_size
            
            if len(users) < batch_size:
                break
            
            await asyncio.sleep(0.3)
        
        self._stats["users_checked"] = total_checked
        self._stats["users_with_stories"] = total_with_stories
        self._stats["stories_downloaded"] += total_downloaded
        self._stats["is_scanning"] = False
        
        lb_stats = load_balancer.get_stats()
        logger.info(
            f"[StoryMonitor] Scan complete: checked={total_checked}, "
            f"with_stories={total_with_stories}, downloaded={total_downloaded}, "
            f"accounts_used={lb_stats['total_clients']}"
        )
    
    async def _check_user_stories(self, client, user: TelegramUser) -> tuple:
        from telethon.tl.functions.stories import GetPeerStoriesRequest
        from telethon.tl.types import InputPeerUser
        from backend.app.services.story_service import StoryService
        
        downloaded = 0
        
        input_peer = InputPeerUser(user.telegram_id, user.access_hash or 0)
        stories_result = await client(GetPeerStoriesRequest(peer=input_peer))
        
        has_stories = bool(
            stories_result and 
            stories_result.stories and 
            hasattr(stories_result.stories, 'stories') and
            stories_result.stories.stories and
            len(stories_result.stories.stories) > 0
        )
        
        if has_stories != user.has_stories:
            async with async_session_maker() as db:
                await db.execute(
                    update(TelegramUser)
                    .where(TelegramUser.id == user.id)
                    .values(has_stories=has_stories)
                )
                await db.commit()
        
        if has_stories:
            async with async_session_maker() as db:
                story_service = StoryService(client, db)
                stories = await story_service.download_user_stories(user)
                downloaded = len(stories)
                if downloaded > 0:
                    live_stats.record("stories_downloaded", downloaded)
                    logger.info(f"[StoryMonitor] Downloaded {downloaded} stories from @{user.username or user.telegram_id}")
        
        return has_stories, downloaded
    
    async def force_check_now(self):
        logger.info("[StoryMonitor] Force check requested")
        self._force_check = True
        return {"status": "started", "message": "Force scan initiated with load balancing"}
    
    async def get_stats(self):
        stats = self._stats.copy()
        stats["load_balancer"] = load_balancer.get_stats()
        return stats


story_monitor = StoryMonitor()
