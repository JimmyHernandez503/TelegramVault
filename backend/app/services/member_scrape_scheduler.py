import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from backend.app.db.database import async_session_maker
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.config import GlobalConfig
from backend.app.services.websocket_manager import ws_manager
from backend.app.services.client_load_balancer import load_balancer

logger = logging.getLogger("member_scheduler")

DEFAULT_SCRAPE_INTERVAL_HOURS = 24


class MemberScrapeScheduler:
    def __init__(self, telegram_manager):
        self.manager = telegram_manager
        self._running = False
        self._task = None
        self._last_scrape = None
        self._groups_scraped = 0
    
    def get_status(self) -> dict:
        return {
            "running": self._running,
            "last_scrape": self._last_scrape.isoformat() if self._last_scrape else None,
            "groups_scraped": self._groups_scraped
        }
    
    def _refresh_load_balancer(self):
        load_balancer.register_clients(self.manager.clients)
    
    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("[MemberScrapeScheduler] Started with load balancing")
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[MemberScrapeScheduler] Stopped")
    
    async def _get_interval_hours(self, db: AsyncSession) -> int:
        result = await db.execute(
            select(GlobalConfig).where(GlobalConfig.key == "member_scrape_interval_hours")
        )
        config = result.scalar_one_or_none()
        if config and config.value:
            try:
                return int(config.value)
            except ValueError:
                pass
        return DEFAULT_SCRAPE_INTERVAL_HOURS
    
    async def _run_loop(self):
        await asyncio.sleep(60)
        
        while self._running:
            try:
                async with async_session_maker() as db:
                    interval_hours = await self._get_interval_hours(db)
                    
                    if interval_hours <= 0:
                        await asyncio.sleep(3600)
                        continue
                    
                    await self._check_and_scrape_groups(db, interval_hours)
                
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.exception(f"[MemberScrapeScheduler] Error in loop: {e}")
                await asyncio.sleep(60)
    
    async def _check_and_scrape_groups(self, db: AsyncSession, interval_hours: int):
        cutoff_time = datetime.utcnow() - timedelta(hours=interval_hours)
        
        result = await db.execute(
            select(TelegramGroup).where(
                and_(
                    TelegramGroup.is_monitoring == True,
                    TelegramGroup.assigned_account_id.isnot(None),
                    TelegramGroup.group_type.in_(["group", "supergroup", "megagroup"]),
                    or_(
                        TelegramGroup.last_member_scrape_at.is_(None),
                        TelegramGroup.last_member_scrape_at < cutoff_time
                    )
                )
            )
        )
        groups = result.scalars().all()
        
        if not groups:
            return
        
        logger.info(f"[MemberScrapeScheduler] Found {len(groups)} groups needing member scrape")
        
        for group in groups:
            if not self._running:
                break
            
            await self._scrape_group_members(group)
            await asyncio.sleep(5)
    
    async def _scrape_group_members(self, group: TelegramGroup):
        from backend.app.services.member_scraper import member_scraper
        from telethon.errors import FloodWaitError
        
        self._refresh_load_balancer()
        
        client = self.manager.clients.get(group.assigned_account_id)
        account_id = group.assigned_account_id
        
        if not client or not client.is_connected():
            client_info = await load_balancer.get_next_client()
            if not client_info:
                logger.warning(f"[MemberScrapeScheduler] No available clients for group {group.id}")
                return
            account_id, client = client_info
            logger.info(f"[MemberScrapeScheduler] Using load-balanced account {account_id} for group {group.id}")
        
        try:
            logger.info(f"[MemberScrapeScheduler] Starting scheduled scrape for group {group.id} ({group.title})")
            
            await ws_manager.broadcast("tasks", {
                "type": "scheduled_member_scrape_started",
                "group_id": group.id,
                "group_title": group.title
            })
            
            async with async_session_maker() as db:
                result = await db.execute(
                    select(TelegramGroup).where(TelegramGroup.id == group.id)
                )
                fresh_group = result.scalar_one_or_none()
                if not fresh_group:
                    return
                
                stats = await member_scraper.scrape_group_members(
                    client=client,
                    group=fresh_group,
                    db=db,
                    account_id=group.assigned_account_id
                )
                
                fresh_group.last_member_scrape_at = datetime.utcnow()
                await db.commit()
            
            load_balancer.report_success(account_id, members=stats.get("new_members", 0) if isinstance(stats, dict) else 0)
            
            self._last_scrape = datetime.utcnow()
            self._groups_scraped += 1
            
            await ws_manager.broadcast("tasks", {
                "type": "scheduled_member_scrape_completed",
                "group_id": group.id,
                "stats": stats
            })
            
            logger.info(f"[MemberScrapeScheduler] Completed scrape for group {group.id}: {stats}")
            
        except FloodWaitError as e:
            load_balancer.report_flood_wait(account_id, e.seconds)
            logger.warning(f"[MemberScrapeScheduler] FloodWait on group {group.id}: {e.seconds}s")
        except Exception as e:
            load_balancer.report_error(account_id)
            logger.error(f"[MemberScrapeScheduler] Failed to scrape group {group.id}: {e}")
    
    async def scrape_now(self, group_ids: list[int] | None = None) -> dict:
        from backend.app.services.member_scraper import member_scraper
        from telethon.errors import FloodWaitError
        
        self._refresh_load_balancer()
        
        async with async_session_maker() as db:
            query = select(TelegramGroup).where(
                TelegramGroup.group_type.in_(["group", "supergroup", "megagroup"])
            )
            
            if group_ids:
                query = query.where(TelegramGroup.id.in_(group_ids))
            else:
                query = query.where(TelegramGroup.is_monitoring == True)
            
            result = await db.execute(query)
            groups = result.scalars().all()
        
        if not groups:
            return {"message": "No groups to scrape", "count": 0}
        
        available_clients = len(load_balancer.get_available_clients())
        results = {"started": len(groups), "completed": 0, "failed": 0, "accounts_used": available_clients, "details": []}
        
        for group in groups:
            client_info = await load_balancer.get_next_client()
            if not client_info:
                results["failed"] += 1
                results["details"].append({"group_id": group.id, "error": "No available clients"})
                continue
            
            account_id, client = client_info
            
            try:
                async with async_session_maker() as db:
                    result = await db.execute(
                        select(TelegramGroup).where(TelegramGroup.id == group.id)
                    )
                    fresh_group = result.scalar_one_or_none()
                    if not fresh_group:
                        continue
                    
                    stats = await member_scraper.scrape_group_members(
                        client=client,
                        group=fresh_group,
                        db=db,
                        account_id=account_id
                    )
                    
                    fresh_group.last_member_scrape_at = datetime.utcnow()
                    await db.commit()
                
                load_balancer.report_success(account_id, members=stats.get("new_members", 0) if isinstance(stats, dict) else 0)
                results["completed"] += 1
                results["details"].append({"group_id": group.id, "account_id": account_id, "stats": stats})
                
                await asyncio.sleep(1)
                
            except FloodWaitError as e:
                load_balancer.report_flood_wait(account_id, e.seconds)
                results["failed"] += 1
                results["details"].append({"group_id": group.id, "error": f"FloodWait {e.seconds}s"})
            except Exception as e:
                load_balancer.report_error(account_id)
                results["failed"] += 1
                results["details"].append({"group_id": group.id, "error": str(e)})
        
        return results


member_scrape_scheduler = None


def get_member_scrape_scheduler():
    return member_scrape_scheduler


def init_member_scrape_scheduler(manager):
    global member_scrape_scheduler
    member_scrape_scheduler = MemberScrapeScheduler(manager)
    return member_scrape_scheduler
