import asyncio
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from telethon.tl.types import User, Channel, ChannelParticipantsRecent, ChannelParticipantsSearch
from telethon.errors import ChatAdminRequiredError, FloodWaitError, ChannelPrivateError

from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.membership import GroupMembership
from backend.app.services.user_enricher import user_enricher
from backend.app.services.live_stats import live_stats


class MemberScraper:
    def __init__(self):
        self.rate_limit_delay = 2.0
        self.batch_size = 200
        self.is_running = {}
    
    async def scrape_group_members(
        self,
        client,
        group: TelegramGroup,
        db: AsyncSession,
        account_id: int,
        progress_callback=None
    ) -> dict:
        group_key = f"{account_id}_{group.telegram_id}"
        if group_key in self.is_running:
            return {"status": "already_running", "scraped": 0}
        
        self.is_running[group_key] = True
        stats = {
            "total_scraped": 0,
            "new_users": 0,
            "updated_users": 0,
            "new_memberships": 0,
            "errors": []
        }
        
        try:
            entity = await client.get_entity(group.telegram_id)
            
            if not hasattr(entity, 'megagroup') and not hasattr(entity, 'broadcast'):
                stats["errors"].append("Not a group or channel")
                return stats
            
            participants = []
            offset = 0
            
            try:
                async for participant in client.iter_participants(
                    entity,
                    limit=self.batch_size * 10,
                    aggressive=True
                ):
                    if isinstance(participant, User):
                        participants.append(participant)
                        
                        if len(participants) % 50 == 0:
                            await self._process_batch(participants[-50:], group, db, stats, account_id)
                            await asyncio.sleep(self.rate_limit_delay)
                            
                            if progress_callback:
                                await progress_callback(len(participants))
                
                if participants:
                    remaining = len(participants) % 50
                    if remaining > 0:
                        await self._process_batch(participants[-remaining:], group, db, stats, account_id)
                        
            except ChatAdminRequiredError:
                stats["errors"].append("Admin rights required to view participants")
            except ChannelPrivateError:
                stats["errors"].append("Channel is private")
            except FloodWaitError as e:
                stats["errors"].append(f"Rate limited, wait {e.seconds} seconds")
                await asyncio.sleep(min(e.seconds, 60))
            
            stats["total_scraped"] = len(participants)
            
            group.member_count = len(participants)
            await db.commit()
            
        except Exception as e:
            stats["errors"].append(str(e))
        finally:
            self.is_running.pop(group_key, None)
        
        return stats
    
    async def _process_batch(
        self,
        users: list,
        group: TelegramGroup,
        db: AsyncSession,
        stats: dict,
        account_id: int
    ):
        # Import user_management_service for UPSERT operations
        from backend.app.services.user_management_service import user_management_service, TelegramUserData
        
        for tg_user in users:
            try:
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.telegram_id == tg_user.id)
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    existing.username = tg_user.username
                    existing.first_name = tg_user.first_name
                    existing.last_name = tg_user.last_name
                    existing.is_premium = getattr(tg_user, 'premium', False) or False
                    existing.is_verified = getattr(tg_user, 'verified', False) or False
                    existing.is_bot = tg_user.bot or False
                    existing.is_scam = getattr(tg_user, 'scam', False) or False
                    existing.is_fake = getattr(tg_user, 'fake', False) or False
                    if not existing.access_hash and tg_user.access_hash:
                        existing.access_hash = tg_user.access_hash
                    stats["updated_users"] += 1
                    user = existing
                else:
                    # Use UPSERT instead of db.add to prevent UniqueViolationError
                    user_data = TelegramUserData(
                        telegram_id=tg_user.id,
                        access_hash=tg_user.access_hash,
                        username=tg_user.username,
                        first_name=tg_user.first_name,
                        last_name=tg_user.last_name,
                        is_premium=getattr(tg_user, 'premium', False) or False,
                        is_verified=getattr(tg_user, 'verified', False) or False,
                        is_bot=tg_user.bot or False,
                        is_scam=getattr(tg_user, 'scam', False) or False,
                        is_fake=getattr(tg_user, 'fake', False) or False
                    )
                    user = await user_management_service.upsert_user(user_data)
                    if not user:
                        continue
                    # Refresh user from database to get the ID
                    result = await db.execute(
                        select(TelegramUser).where(TelegramUser.telegram_id == tg_user.id)
                    )
                    user = result.scalar_one_or_none()
                    if not user:
                        continue
                    stats["new_users"] += 1
                
                membership_result = await db.execute(
                    select(GroupMembership).where(
                        GroupMembership.user_id == user.id,
                        GroupMembership.group_id == group.id
                    )
                )
                existing_membership = membership_result.scalar_one_or_none()
                
                if not existing_membership:
                    membership = GroupMembership(
                        user_id=user.id,
                        group_id=group.id,
                        joined_at=datetime.utcnow()
                    )
                    db.add(membership)
                    stats["new_memberships"] += 1
                    user.groups_count = (user.groups_count or 0) + 1
                
                live_stats.record("members_scraped")
                
            except Exception as e:
                continue
        
        await db.commit()
    
    async def scrape_all_groups(
        self,
        client,
        db: AsyncSession,
        account_id: int
    ) -> dict:
        result = await db.execute(
            select(TelegramGroup).where(
                TelegramGroup.assigned_account_id == account_id,
                TelegramGroup.status == "active"
            )
        )
        groups = result.scalars().all()
        
        total_stats = {
            "groups_processed": 0,
            "total_members": 0,
            "new_users": 0,
            "errors": []
        }
        
        for group in groups:
            stats = await self.scrape_group_members(client, group, db, account_id)
            total_stats["groups_processed"] += 1
            total_stats["total_members"] += stats.get("total_scraped", 0)
            total_stats["new_users"] += stats.get("new_users", 0)
            total_stats["errors"].extend(stats.get("errors", []))
            
            await asyncio.sleep(5)
        
        return total_stats


member_scraper = MemberScraper()
