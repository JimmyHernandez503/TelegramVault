import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Set
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func, exists, and_
from sqlalchemy.orm import selectinload

from backend.app.models.telegram_user import TelegramUser
from backend.app.models.history import UserProfilePhoto
from backend.app.models.config import GlobalConfig
from backend.app.db.database import async_session_maker
from backend.app.services.client_load_balancer import load_balancer

logger = logging.getLogger("profile_photo_scanner")


class ProfilePhotoScanner:
    def __init__(self):
        self._running = False
        self._task = None
        self._check_interval = 300
        self._default_scan_interval = 24
        self._default_batch_size = 100
        self._default_parallel_workers = 5
        self._last_check = None
        self._force_check = False
        self._skipped_users: Set[int] = set()
        self._problem_users: Set[int] = set()
        self._stats = {
            "users_scanned": 0,
            "photos_downloaded": 0,
            "errors": 0,
            "skipped": 0,
            "last_run": None,
            "is_scanning": False,
            "current_phase": "",
            "total_users": 0,
            "priority_pending": 0
        }
    
    def get_status(self) -> dict:
        return {
            "running": self._running,
            "is_scanning": self._stats.get("is_scanning", False),
            "users_scanned": self._stats.get("users_scanned", 0),
            "photos_downloaded": self._stats.get("photos_downloaded", 0),
            "errors": self._stats.get("errors", 0),
            "skipped": self._stats.get("skipped", 0),
            "last_run": self._stats.get("last_run"),
            "current_phase": self._stats.get("current_phase", ""),
            "total_users": self._stats.get("total_users", 0),
            "priority_pending": self._stats.get("priority_pending", 0),
            "problem_users": len(self._problem_users)
        }
    
    async def start(self, telegram_manager):
        if self._running:
            return
        
        self._running = True
        self.manager = telegram_manager
        self._task = asyncio.create_task(self._scanner_loop())
        logger.info("[ProfilePhotoScanner] Started - Priority queue mode")
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[ProfilePhotoScanner] Stopped")
    
    async def _get_config(self, db: AsyncSession) -> tuple:
        result = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "photo_scan_interval_hours")
        )
        config = result.scalar_one_or_none()
        interval_hours = int(config.value) if config and config.value else self._default_scan_interval
        
        result2 = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "photo_scan_batch_size")
        )
        config2 = result2.scalar_one_or_none()
        batch_size = int(config2.value) if config2 and config2.value else self._default_batch_size
        
        result3 = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "photo_scan_parallel_workers")
        )
        config3 = result3.scalar_one_or_none()
        parallel_workers = int(config3.value) if config3 and config3.value else self._default_parallel_workers
        
        result4 = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "photo_scan_enabled")
        )
        config4 = result4.scalar_one_or_none()
        enabled = True
        if config4 and config4.value:
            enabled = config4.value.lower() in ("true", "1", "yes")
        
        return interval_hours, batch_size, parallel_workers, enabled
    
    def _refresh_load_balancer(self):
        load_balancer.register_clients(self.manager.clients)
    
    def trigger_scan(self):
        self._force_check = True
        logger.info("[ProfilePhotoScanner] Manual scan triggered")
    
    async def _scanner_loop(self):
        await asyncio.sleep(30)
        
        while self._running:
            try:
                if self._force_check:
                    self._force_check = False
                    async with async_session_maker() as db:
                        _, batch_size, parallel_workers, enabled = await self._get_config(db)
                    if enabled:
                        logger.info("[ProfilePhotoScanner] Force check - starting priority scan")
                        self._refresh_load_balancer()
                        await self._priority_scan(batch_size, parallel_workers)
                        self._last_check = datetime.utcnow()
                        self._stats["last_run"] = self._last_check.isoformat()
                    await asyncio.sleep(self._check_interval)
                    continue
                
                async with async_session_maker() as db:
                    interval_hours, batch_size, parallel_workers, enabled = await self._get_config(db)
                    
                    if not enabled or interval_hours <= 0:
                        await asyncio.sleep(self._check_interval)
                        continue
                
                now = datetime.utcnow()
                
                if self._last_check is None:
                    self._last_check = now - timedelta(hours=interval_hours)
                
                time_since_last = (now - self._last_check).total_seconds() / 3600
                
                if time_since_last >= interval_hours:
                    logger.info(f"[ProfilePhotoScanner] Starting scheduled priority scan")
                    self._refresh_load_balancer()
                    await self._priority_scan(batch_size, parallel_workers)
                    self._last_check = datetime.utcnow()
                    self._stats["last_run"] = self._last_check.isoformat()
                
                await asyncio.sleep(self._check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[ProfilePhotoScanner] Error in scanner loop: {e}")
                self._stats["errors"] += 1
                await asyncio.sleep(60)
    
    async def _priority_scan(self, batch_size: int, parallel_workers: int):
        self._stats["is_scanning"] = True
        self._stats["users_scanned"] = 0
        self._stats["photos_downloaded"] = 0
        self._stats["skipped"] = 0
        self._skipped_users.clear()
        
        try:
            self._stats["current_phase"] = "Phase 1: Users without photos"
            await self._scan_users_without_photos(batch_size, parallel_workers)
            
            self._stats["current_phase"] = "Phase 2: Users with old scans"
            await self._scan_stale_users(batch_size, parallel_workers)
            
            if self._skipped_users:
                self._stats["current_phase"] = "Phase 3: Retry skipped users"
                await self._retry_skipped_users(batch_size, parallel_workers)
            
            self._stats["current_phase"] = "Complete"
            logger.info(
                f"[ProfilePhotoScanner] Scan complete: {self._stats['users_scanned']} scanned, "
                f"{self._stats['photos_downloaded']} photos, {self._stats['skipped']} skipped"
            )
            
        except Exception as e:
            logger.error(f"[ProfilePhotoScanner] Error during priority scan: {e}")
            self._stats["errors"] += 1
        finally:
            self._stats["is_scanning"] = False
    
    async def _scan_users_without_photos(self, batch_size: int, parallel_workers: int):
        async with async_session_maker() as db:
            has_photos = (
                select(UserProfilePhoto.user_id)
                .where(UserProfilePhoto.user_id == TelegramUser.id)
                .exists()
            )
            
            count_result = await db.execute(
                select(func.count(TelegramUser.id)).where(~has_photos)
            )
            total = count_result.scalar() or 0
            self._stats["priority_pending"] = total
            
            if total == 0:
                logger.info("[ProfilePhotoScanner] No users without photos")
                return
            
            logger.info(f"[ProfilePhotoScanner] Phase 1: {total} users without photos")
        
        offset = 0
        while offset < total and self._running:
            async with async_session_maker() as db:
                has_photos = (
                    select(UserProfilePhoto.user_id)
                    .where(UserProfilePhoto.user_id == TelegramUser.id)
                    .exists()
                )
                
                result = await db.execute(
                    select(TelegramUser)
                    .where(~has_photos)
                    .where(TelegramUser.id.notin_(self._problem_users))
                    .order_by(TelegramUser.id)
                    .limit(batch_size)
                )
                users = result.scalars().all()
            
            if not users:
                break
            
            await self._process_batch(users, parallel_workers)
            offset += len(users)
            self._stats["priority_pending"] = max(0, total - offset)
            
            await asyncio.sleep(0.5)
    
    async def _scan_stale_users(self, batch_size: int, parallel_workers: int):
        stale_threshold = datetime.utcnow() - timedelta(days=7)
        
        async with async_session_maker() as db:
            result = await db.execute(
                select(func.count(TelegramUser.id))
                .where(
                    (TelegramUser.last_photo_scan < stale_threshold) | 
                    (TelegramUser.last_photo_scan.is_(None))
                )
                .where(TelegramUser.id.notin_(self._problem_users))
            )
            total = result.scalar() or 0
            
            if total == 0:
                logger.info("[ProfilePhotoScanner] No stale users to scan")
                return
            
            logger.info(f"[ProfilePhotoScanner] Phase 2: {total} stale users")
        
        offset = 0
        while offset < total and self._running:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(TelegramUser)
                    .where(
                        (TelegramUser.last_photo_scan < stale_threshold) | 
                        (TelegramUser.last_photo_scan.is_(None))
                    )
                    .where(TelegramUser.id.notin_(self._problem_users))
                    .order_by(TelegramUser.last_photo_scan.asc().nullsfirst())
                    .limit(batch_size)
                )
                users = result.scalars().all()
            
            if not users:
                break
            
            await self._process_batch(users, parallel_workers)
            offset += len(users)
            
            await asyncio.sleep(0.5)
    
    async def _retry_skipped_users(self, batch_size: int, parallel_workers: int):
        skipped_list = list(self._skipped_users)
        logger.info(f"[ProfilePhotoScanner] Phase 3: Retrying {len(skipped_list)} skipped users")
        
        for i in range(0, len(skipped_list), batch_size):
            if not self._running:
                break
            
            batch_ids = skipped_list[i:i + batch_size]
            
            async with async_session_maker() as db:
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.id.in_(batch_ids))
                )
                users = result.scalars().all()
            
            if users:
                self._skipped_users -= set(batch_ids)
                await self._process_batch(users, parallel_workers, is_retry=True)
            
            await asyncio.sleep(1)
    
    async def _process_batch(self, users: List[TelegramUser], parallel_workers: int, is_retry: bool = False):
        self._refresh_load_balancer()
        semaphore = asyncio.Semaphore(parallel_workers)
        
        async def scan_user(user: TelegramUser):
            async with semaphore:
                if not self._running:
                    return 0
                
                try:
                    client_info = await load_balancer.get_next_client()
                    
                    if not client_info:
                        if not is_retry:
                            self._skipped_users.add(user.id)
                            self._stats["skipped"] += 1
                        return 0
                    
                    account_id, client = client_info
                    
                    if not user.access_hash:
                        self._problem_users.add(user.id)
                        return 0
                    
                    from telethon.tl.types import InputUser
                    try:
                        input_user = InputUser(
                            user_id=user.telegram_id,
                            access_hash=user.access_hash
                        )
                        tg_user = await client.get_entity(input_user)
                    except Exception as e:
                        err_str = str(e).lower()
                        if "peer" in err_str or "access" in err_str or "invalid" in err_str:
                            self._problem_users.add(user.id)
                        elif not is_retry:
                            self._skipped_users.add(user.id)
                            self._stats["skipped"] += 1
                        return 0
                    
                    from backend.app.services.user_enricher import UserEnricherService
                    enricher = UserEnricherService()
                    
                    async with async_session_maker() as db:
                        fresh_user = await db.get(TelegramUser, user.id)
                        if not fresh_user:
                            return 0
                        
                        photos_downloaded = await enricher.sync_all_profile_photos(
                            client, db, fresh_user, tg_user
                        )
                        
                        fresh_user.last_photo_scan = datetime.utcnow()
                        await db.commit()
                        
                        self._stats["users_scanned"] += 1
                        self._stats["photos_downloaded"] += photos_downloaded
                        return photos_downloaded
                        
                except Exception as e:
                    logger.debug(f"[ProfilePhotoScanner] Error scanning user {user.id}: {e}")
                    self._stats["errors"] += 1
                    if not is_retry:
                        self._skipped_users.add(user.id)
                    return 0
        
        tasks = [scan_user(user) for user in users]
        await asyncio.gather(*tasks, return_exceptions=True)


profile_photo_scanner = ProfilePhotoScanner()
