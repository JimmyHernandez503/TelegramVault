import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from telethon.errors import FloodWaitError, InviteHashExpiredError, UserAlreadyParticipantError, InviteHashInvalidError, ChannelPrivateError, ChatWriteForbiddenError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, or_

from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.invite import InviteLink
from backend.app.models.config import GlobalConfig
from backend.app.db.database import async_session_maker
from backend.app.services.client_load_balancer import load_balancer
from backend.app.services.websocket_manager import ws_manager

logger = logging.getLogger("autojoin")

INVITE_LINK_PATTERN = re.compile(r'(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)([a-zA-Z0-9_-]+)')


class AutoJoinService:
    REQUEST_PENDING_TIMEOUT_DAYS = 7
    MAX_PREVIEW_RETRIES = 3
    
    def __init__(self, telegram_manager=None):
        self.manager = telegram_manager
        self._running = False
        self._task = None
        self._preview_task = None
        self._cleanup_task = None
        self._processed_links: set = set()
        self._daily_joins: dict = {}  # {date_str: count}
        self._stats = {
            "total_joined": 0,
            "total_failed": 0,
            "pending_count": 0,
            "last_join": None,
            "last_error": None,
            "joins_today": 0
        }
        self._default_config = {
            "enabled": False,
            "mode": "rotation",
            "delay_minutes": 5,
            "enabled_accounts": [],
            "auto_backfill": True,
            "auto_scrape_members": True,
            "auto_monitor": True,
            "auto_stories": True,
            "max_joins_per_day": 20
        }
    
    def get_status(self) -> dict:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        joins_today = self._daily_joins.get(today, 0)
        return {
            "running": self._running,
            "pending_count": self._stats.get("pending_count", 0),
            "total_joined": self._stats.get("total_joined", 0),
            "total_failed": self._stats.get("total_failed", 0),
            "joins_today": joins_today,
            "last_join": self._stats.get("last_join"),
            "last_error": self._stats.get("last_error"),
            "processed_links_count": len(self._processed_links)
        }
    
    async def start(self, telegram_manager=None):
        if telegram_manager:
            self.manager = telegram_manager
        
        if self._running or not self.manager:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._process_loop())
        self._preview_task = asyncio.create_task(self._preview_loop())
        self._approval_check_task = asyncio.create_task(self._check_approved_requests_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_requests_loop())
        
        await self._load_daily_joins_from_db()
        await self._register_join_handlers()
        
        logger.info("[AutoJoin] Started with multi-account load balancing + auto preview + approval checker + cleanup + join event handlers")
    
    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[AutoJoin] Stopped")
    
    async def _register_join_handlers(self):
        from telethon import events
        from telethon.tl.types import UpdateChannel, UpdateNewChannelMessage
        
        for account_id, client in self.manager.clients.items():
            if client and client.is_connected():
                async def on_chat_action(event, acc_id=account_id):
                    await self._handle_chat_action(event, acc_id)
                
                async def on_raw_update(update, acc_id=account_id):
                    await self._handle_raw_update(update, acc_id)
                
                client.add_event_handler(on_chat_action, events.ChatAction())
                client.on(events.Raw)(on_raw_update)
                
                logger.info(f"[AutoJoin] Registered join handlers for account {account_id}")
    
    async def _handle_chat_action(self, event, account_id: int):
        try:
            if event.user_added or event.user_joined:
                me = await event.client.get_me()
                if event.user_id == me.id:
                    chat = await event.get_chat()
                    chat_id = event.chat_id
                    
                    logger.info(f"[AutoJoin] Detected join event! Chat: {getattr(chat, 'title', chat_id)}")
                    
                    await self._process_join_event(chat, account_id)
        except Exception as e:
            logger.debug(f"[AutoJoin] Error handling chat action: {e}")
    
    async def _handle_raw_update(self, update, account_id: int):
        from telethon.tl.types import UpdateChannel
        
        try:
            if isinstance(update, UpdateChannel):
                channel_id = update.channel_id
                
                client = self.manager.clients.get(account_id)
                if not client:
                    return
                
                try:
                    chat = await client.get_entity(channel_id)
                    
                    async with async_session_maker() as db:
                        telegram_id = -1000000000000 - channel_id
                        result = await db.execute(
                            select(TelegramGroup).where(TelegramGroup.telegram_id == telegram_id)
                        )
                        existing = result.scalar_one_or_none()
                        
                        if not existing:
                            result = await db.execute(
                                select(InviteLink).where(
                                    and_(
                                        InviteLink.status == "request_pending",
                                        or_(
                                            InviteLink.preview_title == getattr(chat, 'title', None),
                                            InviteLink.assigned_account_id == account_id
                                        )
                                    )
                                ).limit(1)
                            )
                            pending_invite = result.scalar_one_or_none()
                            
                            if pending_invite:
                                logger.info(f"[AutoJoin] Detected approval via UpdateChannel: {getattr(chat, 'title', channel_id)}")
                                await self._process_join_event(chat, account_id, pending_invite)
                            else:
                                logger.info(f"[AutoJoin] New channel detected: {getattr(chat, 'title', channel_id)}")
                                await self._process_join_event(chat, account_id)
                except Exception as e:
                    logger.debug(f"[AutoJoin] Could not get entity for channel {channel_id}: {e}")
        except Exception as e:
            logger.debug(f"[AutoJoin] Error handling raw update: {e}")
    
    async def _process_join_event(self, chat, account_id: int, invite: Optional[InviteLink] = None):
        try:
            async with async_session_maker() as db:
                config = await self._get_config(db)
                
                group = await self._save_joined_group(chat, account_id, db)
                
                if invite:
                    invite_result = await db.execute(
                        select(InviteLink).where(InviteLink.id == invite.id)
                    )
                    inv = invite_result.scalar_one_or_none()
                    if inv:
                        inv.status = "joined"
                        inv.joined_group_id = group.id
                        inv.last_error = "Aprobacion detectada automaticamente"
                
                await db.commit()
                
                self._stats["total_joined"] += 1
                self._stats["last_join"] = datetime.utcnow().isoformat()
                
                await ws_manager.broadcast("tasks", {
                    "type": "autojoin_detected",
                    "group_id": group.id,
                    "group_title": group.title,
                    "account_id": account_id,
                    "method": "event_handler"
                })
                
                logger.info(f"[AutoJoin] Successfully processed join: {group.title}")
                
                await self._post_join_actions(group, account_id, config)
                
        except Exception as e:
            logger.error(f"[AutoJoin] Error processing join event: {e}")
    
    async def _get_config(self, db: AsyncSession) -> Dict[str, Any]:
        config = self._default_config.copy()
        
        keys_map = {
            "autojoin_enabled": ("enabled", "bool"),
            "autojoin_mode": ("mode", "str"),
            "autojoin_delay_minutes": ("delay_minutes", "int"),
            "autojoin_enabled_accounts": ("enabled_accounts", "list"),
            "autojoin_auto_backfill": ("auto_backfill", "bool"),
            "autojoin_auto_scrape_members": ("auto_scrape_members", "bool"),
            "autojoin_auto_monitor": ("auto_monitor", "bool"),
            "autojoin_auto_stories": ("auto_stories", "bool"),
            "autojoin_max_joins_per_day": ("max_joins_per_day", "int")
        }
        
        for db_key, (config_key, value_type) in keys_map.items():
            result = await db.execute(
                select(GlobalConfig).where(GlobalConfig.key == db_key)
            )
            cfg = result.scalar_one_or_none()
            if cfg and cfg.value:
                if value_type == "bool":
                    config[config_key] = cfg.value.lower() == "true"
                elif value_type == "int":
                    try:
                        config[config_key] = int(cfg.value)
                    except ValueError:
                        pass
                elif value_type == "list":
                    try:
                        config[config_key] = [int(x) for x in cfg.value.split(",") if x.strip()]
                    except ValueError:
                        pass
                else:
                    config[config_key] = cfg.value
        
        return config
    
    async def _load_daily_joins_from_db(self):
        """Cargar contador de joins del dia actual desde la BD"""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        async with async_session_maker() as db:
            from sqlalchemy import func
            result = await db.execute(
                select(func.count(InviteLink.id)).where(
                    and_(
                        InviteLink.status == "joined",
                        InviteLink.updated_at >= today_start
                    )
                )
            )
            count = result.scalar() or 0
            self._daily_joins[today] = count
            logger.info(f"[AutoJoin] Loaded daily joins count: {count} for {today}")
    
    def _can_join_today(self, max_joins: int) -> bool:
        """Verificar si podemos hacer mas joins hoy"""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        current_joins = self._daily_joins.get(today, 0)
        return current_joins < max_joins
    
    def _increment_daily_joins(self):
        """Incrementar contador de joins del dia"""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self._daily_joins[today] = self._daily_joins.get(today, 0) + 1
        # Limpiar dias anteriores para no acumular memoria
        old_dates = [d for d in self._daily_joins.keys() if d != today]
        for d in old_dates:
            del self._daily_joins[d]
    
    async def _cleanup_expired_requests_loop(self):
        """Loop para marcar como expirados los request_pending antiguos"""
        await asyncio.sleep(300)  # Esperar 5 min al inicio
        
        while self._running:
            try:
                async with async_session_maker() as db:
                    timeout_date = datetime.utcnow() - timedelta(days=self.REQUEST_PENDING_TIMEOUT_DAYS)
                    
                    result = await db.execute(
                        select(InviteLink).where(
                            and_(
                                InviteLink.status == "request_pending",
                                InviteLink.updated_at < timeout_date
                            )
                        )
                    )
                    expired_invites = result.scalars().all()
                    
                    for invite in expired_invites:
                        invite.status = "expired"
                        invite.last_error = f"Timeout: sin aprobacion despues de {self.REQUEST_PENDING_TIMEOUT_DAYS} dias"
                        logger.info(f"[AutoJoin] Marked request_pending as expired: {invite.link}")
                    
                    if expired_invites:
                        await db.commit()
                        logger.info(f"[AutoJoin] Cleaned up {len(expired_invites)} expired request_pending invites")
                
                await asyncio.sleep(3600)  # Check cada hora
                
            except Exception as e:
                logger.error(f"[AutoJoin] Error in cleanup loop: {e}")
                await asyncio.sleep(600)
    
    async def _check_approved_requests_loop(self):
        await asyncio.sleep(120)
        
        while self._running:
            try:
                async with async_session_maker() as db:
                    config = await self._get_config(db)
                    
                    result = await db.execute(
                        select(InviteLink).where(
                            InviteLink.status == "request_pending"
                        ).order_by(InviteLink.updated_at.asc()).limit(10)
                    )
                    pending_requests = result.scalars().all()
                    
                    for invite in pending_requests:
                        try:
                            await self._check_if_approved(invite, config, db)
                        except Exception as e:
                            logger.debug(f"[AutoJoin] Error checking approval for {invite.id}: {e}")
                    
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.exception(f"[AutoJoin] Error in approval check loop: {e}")
                await asyncio.sleep(120)
    
    async def _check_if_approved(self, invite: InviteLink, config: Dict[str, Any], db: AsyncSession):
        if invite.assigned_account_id and invite.assigned_account_id in self.manager.clients:
            account_id = invite.assigned_account_id
            client = self.manager.clients[account_id]
            if not client.is_connected():
                logger.debug(f"[AutoJoin] Client {account_id} not connected for approval check")
                return
        else:
            client_info = await self._get_client_for_join(config)
            if not client_info:
                logger.debug(f"[AutoJoin] No available clients for approval check of invite {invite.id}")
                return
            account_id, client = client_info
        
        from telethon.tl.functions.messages import CheckChatInviteRequest
        from telethon.tl.types import ChatInviteAlready
        
        invite_hash = invite.invite_hash or self._extract_hash(invite.link)
        if not invite_hash or invite_hash.startswith("username:"):
            return
        
        try:
            preview = await client(CheckChatInviteRequest(hash=invite_hash))
            
            if isinstance(preview, ChatInviteAlready):
                chat = preview.chat
                logger.info(f"[AutoJoin] Request APPROVED! {getattr(chat, 'title', 'Unknown')}")
                
                group = await self._save_joined_group(chat, account_id, db)
                
                invite.status = "joined"
                invite.joined_group_id = group.id
                invite.last_error = "Solicitud aprobada por admin"
                await db.commit()
                
                self._stats["total_joined"] += 1
                self._stats["last_join"] = datetime.utcnow().isoformat()
                
                await ws_manager.broadcast("tasks", {
                    "type": "autojoin_approved",
                    "invite_id": invite.id,
                    "group_id": group.id,
                    "group_title": group.title,
                    "account_id": account_id
                })
                
                await self._post_join_actions(group, account_id, config)
                
                logger.info(f"[AutoJoin] Successfully added approved group: {group.title}")
                
        except Exception as e:
            if "expired" in str(e).lower() or "invalid" in str(e).lower():
                invite.status = "expired"
                invite.last_error = "Link expirado mientras esperaba aprobacion"
                await db.commit()
    
    async def _process_loop(self):
        await asyncio.sleep(30)
        
        while self._running:
            try:
                async with async_session_maker() as db:
                    config = await self._get_config(db)
                    
                    if not config["enabled"]:
                        await asyncio.sleep(60)
                        continue
                    
                    max_joins = config.get("max_joins_per_day", 20)
                    if not self._can_join_today(max_joins):
                        today = datetime.utcnow().strftime("%Y-%m-%d")
                        logger.info(f"[AutoJoin] Daily limit reached ({self._daily_joins.get(today, 0)}/{max_joins}), waiting...")
                        await asyncio.sleep(300)
                        continue
                    
                    pending = await db.execute(
                        select(InviteLink).where(
                            and_(
                                InviteLink.status == "pending",
                                or_(
                                    InviteLink.next_retry_at.is_(None),
                                    InviteLink.next_retry_at <= datetime.utcnow()
                                )
                            )
                        ).order_by(InviteLink.created_at.asc()).limit(1)
                    )
                    invite = pending.scalar_one_or_none()
                    
                    if invite:
                        self._stats["pending_count"] = await self._count_pending(db)
                        await self._process_invite(invite, config, db)
                
                delay_minutes = config.get("delay_minutes", 5)
                await asyncio.sleep(delay_minutes * 60)
                
            except Exception as e:
                logger.exception(f"[AutoJoin] Error in loop: {e}")
                await asyncio.sleep(60)
    
    async def _count_pending(self, db: AsyncSession) -> int:
        result = await db.execute(
            select(InviteLink).where(InviteLink.status == "pending")
        )
        return len(result.scalars().all())
    
    def _refresh_load_balancer(self):
        if self.manager:
            load_balancer.register_clients(self.manager.clients)
    
    async def _get_client_for_join(self, config: Dict[str, Any]):
        self._refresh_load_balancer()
        
        mode = config.get("mode", "rotation")
        enabled_accounts = config.get("enabled_accounts", [])
        
        if mode == "rotation" or not enabled_accounts:
            return await load_balancer.get_next_client()
        else:
            for acc_id in enabled_accounts:
                if acc_id in self.manager.clients:
                    client = self.manager.clients[acc_id]
                    if client and client.is_connected():
                        return (acc_id, client)
            return await load_balancer.get_next_client()
    
    async def _process_invite(self, invite: InviteLink, config: Dict[str, Any], db: AsyncSession):
        logger.info(f"[AutoJoin] Processing invite {invite.id}: {invite.link}")
        
        client_info = await self._get_client_for_join(config)
        
        if not client_info:
            logger.warning("[AutoJoin] No available clients")
            return
        
        account_id, client = client_info
        
        invite.status = "processing"
        invite.assigned_account_id = account_id
        await db.commit()
        
        await ws_manager.broadcast("tasks", {
            "type": "autojoin_started",
            "invite_id": invite.id,
            "link": invite.link,
            "account_id": account_id
        })
        
        try:
            invite_hash = self._extract_hash(invite.link)
            if not invite_hash:
                if 't.me/' in invite.link and '+' not in invite.link and 'joinchat' not in invite.link:
                    username = invite.link.split('t.me/')[-1].split('/')[0].split('?')[0]
                    if username:
                        invite_hash = f"username:{username}"
            
            if not invite_hash:
                raise ValueError("Invalid invite link format")
            
            invite.invite_hash = invite_hash
            
            from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
            from telethon.tl.types import ChatInvite, ChatInviteAlready, ChatInvitePeek
            
            chat = None
            
            if invite_hash.startswith("username:"):
                username = invite_hash.replace("username:", "")
                chat = await client.get_entity(username)
                invite.status = "joined"
            else:
                try:
                    preview = await client(CheckChatInviteRequest(hash=invite_hash))
                    
                    if isinstance(preview, ChatInviteAlready):
                        chat = preview.chat
                        invite.status = "already_joined"
                        invite.last_error = "Already a member"
                        logger.info(f"[AutoJoin] Already member: {getattr(chat, 'title', 'Unknown')}")
                    elif isinstance(preview, ChatInvitePeek):
                        invite.status = "request_pending"
                        invite.last_error = "Requiere aprobacion del admin"
                        if hasattr(preview, 'chat'):
                            invite.preview_title = getattr(preview.chat, 'title', None)
                        invite.preview_fetched_at = datetime.utcnow()
                        logger.info(f"[AutoJoin] Request pending approval: {invite.preview_title or invite.link}")
                    else:
                        if hasattr(preview, 'title'):
                            invite.preview_title = preview.title
                        if hasattr(preview, 'about'):
                            invite.preview_about = preview.about
                        if hasattr(preview, 'participants_count'):
                            invite.preview_member_count = preview.participants_count
                        if hasattr(preview, 'channel') or hasattr(preview, 'broadcast'):
                            invite.preview_is_channel = getattr(preview, 'channel', False) or getattr(preview, 'broadcast', False)
                        invite.preview_fetched_at = datetime.utcnow()
                        
                        if hasattr(preview, 'request_needed') and preview.request_needed:
                            from telethon.tl.functions.messages import ImportChatInviteRequest as JoinReq
                            try:
                                updates = await client(JoinReq(hash=invite_hash))
                                if hasattr(updates, 'updates') and not updates.chats:
                                    invite.status = "request_pending"
                                    invite.last_error = "Solicitud enviada - esperando aprobacion"
                                    logger.info(f"[AutoJoin] Join request sent: {preview.title}")
                                else:
                                    chat = updates.chats[0] if updates.chats else None
                                    invite.status = "joined"
                            except Exception as req_err:
                                if "request" in str(req_err).lower() or "pending" in str(req_err).lower():
                                    invite.status = "request_pending"
                                    invite.last_error = "Solicitud enviada - esperando aprobacion"
                                else:
                                    raise
                        else:
                            updates = await client(ImportChatInviteRequest(hash=invite_hash))
                            chat = updates.chats[0] if updates.chats else None
                            invite.status = "joined"
                        
                except InviteHashExpiredError:
                    invite.status = "expired"
                    invite.last_error = "Link expirado"
                    raise
                except InviteHashInvalidError:
                    invite.status = "invalid"
                    invite.last_error = "Link invalido"
                    raise
            
            if chat:
                group = await self._save_joined_group(chat, account_id, db)
                invite.joined_group_id = group.id
                self._stats["total_joined"] += 1
                self._stats["last_join"] = datetime.utcnow().isoformat()
                self._increment_daily_joins()
                
                load_balancer.report_success(account_id)
                
                logger.info(f"[AutoJoin] Successfully joined: {group.title} (ID: {group.id})")
                
                await ws_manager.broadcast("tasks", {
                    "type": "autojoin_success",
                    "invite_id": invite.id,
                    "group_id": group.id,
                    "group_title": group.title,
                    "account_id": account_id
                })
                
                await db.commit()
                
                await self._post_join_actions(group, account_id, config)
                
        except UserAlreadyParticipantError:
            invite.status = "already_joined"
            invite.last_error = "Ya eres miembro"
            logger.info(f"[AutoJoin] Already member of {invite.link}")
        
        except ChannelPrivateError:
            invite.status = "private"
            invite.last_error = "Canal/grupo privado - sin acceso"
            self._stats["total_failed"] += 1
            logger.warning(f"[AutoJoin] Private channel: {invite.link}")
            
        except (InviteHashExpiredError, InviteHashInvalidError):
            self._stats["total_failed"] += 1
            
        except FloodWaitError as e:
            load_balancer.report_flood_wait(account_id, e.seconds)
            invite.status = "pending"
            invite.retry_count += 1
            invite.next_retry_at = datetime.utcnow() + timedelta(seconds=e.seconds + 60)
            invite.last_error = f"FloodWait {e.seconds}s"
            logger.warning(f"[AutoJoin] FloodWait {e.seconds}s for {invite.link}")
            
        except Exception as e:
            error_str = str(e).lower()
            invite.retry_count += 1
            invite.last_error = str(e)[:500]
            self._stats["total_failed"] += 1
            self._stats["last_error"] = str(e)[:200]
            logger.error(f"[AutoJoin] Failed {invite.link}: {e}")
            
            if "too many" in error_str or "many attempts" in error_str or "try again later" in error_str:
                invite.status = "pending"
                invite.next_retry_at = datetime.utcnow() + timedelta(minutes=10)
                load_balancer.report_flood_wait(account_id, 600)
                logger.warning(f"[AutoJoin] Too many attempts - waiting 10 minutes for {invite.link}")
            elif invite.retry_count < 5:
                invite.status = "pending"
                invite.next_retry_at = datetime.utcnow() + timedelta(minutes=30)
            else:
                invite.status = "failed"
        
        await db.commit()
    
    def _extract_hash(self, link: str) -> Optional[str]:
        match = INVITE_LINK_PATTERN.search(link)
        if match:
            return match.group(1)
        return None
    
    async def _save_joined_group(self, chat, account_id: int, db: AsyncSession) -> TelegramGroup:
        from telethon.tl.types import Channel, Chat
        
        chat_id = getattr(chat, 'id', 0)
        
        if hasattr(chat, 'broadcast') and chat.broadcast:
            telegram_id = -1000000000000 - chat_id
            group_type = "channel"
        elif hasattr(chat, 'megagroup') and chat.megagroup:
            telegram_id = -1000000000000 - chat_id
            group_type = "supergroup"
        elif isinstance(chat, Channel):
            telegram_id = -1000000000000 - chat_id
            group_type = "supergroup"
        else:
            telegram_id = -chat_id
            group_type = "group"
        
        result = await db.execute(
            select(TelegramGroup).where(TelegramGroup.telegram_id == telegram_id)
        )
        group = result.scalar_one_or_none()
        
        if not group:
            group = TelegramGroup(
                telegram_id=telegram_id,
                title=getattr(chat, 'title', 'Unknown'),
                username=getattr(chat, 'username', None),
                group_type=group_type,
                member_count=getattr(chat, 'participants_count', 0) or 0,
                assigned_account_id=account_id,
                is_monitoring=False,
                backfill_in_progress=False,
                backfill_done=False
            )
            db.add(group)
            await db.flush()
        else:
            group.title = getattr(chat, 'title', group.title)
            group.username = getattr(chat, 'username', group.username)
            new_count = getattr(chat, 'participants_count', None)
            if new_count is not None:
                group.member_count = new_count
            if not group.assigned_account_id:
                group.assigned_account_id = account_id
        
        return group
    
    async def _post_join_actions(self, group: TelegramGroup, account_id: int, config: Dict[str, Any]):
        logger.info(f"[AutoJoin] Starting post-join actions for {group.title}")
        
        actions_done = []
        telegram_id = group.telegram_id
        
        try:
            client = self.manager.clients.get(account_id)
            if client:
                await self._download_group_photo(client, group, account_id)
                actions_done.append("group_photo")
        except Exception as e:
            logger.error(f"[AutoJoin] Failed to download group photo: {e}")
        
        if config.get("auto_monitor", True):
            try:
                async with async_session_maker() as db:
                    result = await db.execute(
                        select(TelegramGroup).where(TelegramGroup.id == group.id)
                    )
                    g = result.scalar_one_or_none()
                    if g:
                        g.is_monitoring = True
                        await db.commit()
                
                await self.manager.live_monitor.start_monitor(account_id, group.id, telegram_id)
                actions_done.append("monitoring")
                logger.info(f"[AutoJoin] Started monitoring for {group.title}")
            except Exception as e:
                logger.error(f"[AutoJoin] Failed to start monitoring: {e}")
        
        if config.get("auto_backfill", True):
            try:
                async with async_session_maker() as db:
                    result = await db.execute(
                        select(TelegramGroup).where(TelegramGroup.id == group.id)
                    )
                    g = result.scalar_one_or_none()
                    if g:
                        g.backfill_in_progress = False
                        g.backfill_done = False
                        await db.commit()
                
                asyncio.create_task(
                    self.manager.backfill_service.start_backfill(
                        account_id=account_id,
                        channel_id=group.id,
                        telegram_id=telegram_id
                    )
                )
                actions_done.append("backfill")
                logger.info(f"[AutoJoin] Started backfill for {group.title}")
            except Exception as e:
                logger.error(f"[AutoJoin] Failed to start backfill: {e}")
        
        if config.get("auto_scrape_members", True) and group.group_type in ["group", "supergroup", "megagroup"]:
            try:
                from backend.app.services.member_scraper import member_scraper
                client = self.manager.clients.get(account_id)
                if client:
                    async with async_session_maker() as db:
                        result = await db.execute(
                            select(TelegramGroup).where(TelegramGroup.id == group.id)
                        )
                        fresh_group = result.scalar_one_or_none()
                        if fresh_group:
                            asyncio.create_task(
                                member_scraper.scrape_group_members(client, fresh_group, db, account_id)
                            )
                            actions_done.append("scrape_members")
                            logger.info(f"[AutoJoin] Started member scraping for {group.title}")
            except Exception as e:
                logger.error(f"[AutoJoin] Failed to start member scraping: {e}")
        
        await ws_manager.broadcast("tasks", {
            "type": "autojoin_post_actions",
            "group_id": group.id,
            "group_title": group.title,
            "actions_completed": actions_done
        })
    
    async def _download_group_photo(self, client, group: TelegramGroup, account_id: int):
        import os
        from backend.app.core.config import settings
        
        try:
            entity = await client.get_entity(group.telegram_id)
            if hasattr(entity, 'photo') and entity.photo:
                photo_dir = os.path.join(settings.MEDIA_PATH, "group_photos", str(group.id))
                os.makedirs(photo_dir, exist_ok=True)
                
                photo_path = os.path.join(photo_dir, "photo.jpg")
                await client.download_profile_photo(entity, file=photo_path)
                
                async with async_session_maker() as db:
                    result = await db.execute(
                        select(TelegramGroup).where(TelegramGroup.id == group.id)
                    )
                    g = result.scalar_one_or_none()
                    if g:
                        g.photo_path = f"media/group_photos/{group.id}/photo.jpg"
                        await db.commit()
                
                logger.info(f"[AutoJoin] Downloaded group photo for {group.title}")
        except Exception as e:
            logger.debug(f"[AutoJoin] Could not download group photo: {e}")
    
    async def add_from_detection(self, link: str, source_group_id: Optional[int] = None, source_user_id: Optional[int] = None, source_message_id: Optional[int] = None):
        invite_hash = self._extract_hash(link)
        if not invite_hash:
            if 't.me/' in link and '+' not in link and 'joinchat' not in link:
                username = link.split('t.me/')[-1].split('/')[0].split('?')[0]
                if username:
                    invite_hash = f"username:{username}"
        
        if not invite_hash:
            return None
        
        if invite_hash in self._processed_links:
            return None
        
        async with async_session_maker() as db:
            existing = await db.execute(
                select(InviteLink).where(
                    or_(
                        InviteLink.link == link,
                        InviteLink.invite_hash == invite_hash
                    )
                )
            )
            if existing.scalar_one_or_none():
                return None
            
            invite = InviteLink(
                link=link,
                invite_hash=invite_hash,
                status="pending",
                source_group_id=source_group_id,
                source_user_id=source_user_id,
                source_message_id=source_message_id
            )
            db.add(invite)
            await db.commit()
            await db.refresh(invite)
            
            self._processed_links.add(invite_hash)
            
            logger.info(f"[AutoJoin] Added invite from detection: {link}")
            
            await ws_manager.broadcast("detections", {
                "type": "new_invite_link",
                "invite_id": invite.id,
                "link": link
            })
            
            return invite
    
    async def join_now(self, invite_id: int) -> Dict[str, Any]:
        async with async_session_maker() as db:
            result = await db.execute(
                select(InviteLink).where(InviteLink.id == invite_id)
            )
            invite = result.scalar_one_or_none()
            
            if not invite:
                return {"error": "Invite not found"}
            
            config = await self._get_config(db)
            config["enabled"] = True
            
            await self._process_invite(invite, config, db)
            
            await db.refresh(invite)
            return {
                "status": invite.status,
                "joined_group_id": invite.joined_group_id,
                "error": invite.last_error
            }
    
    async def get_stats(self) -> Dict[str, Any]:
        async with async_session_maker() as db:
            self._stats["pending_count"] = await self._count_pending(db)
            config = await self._get_config(db)
        
        return {
            **self._stats,
            "config": config,
            "load_balancer": load_balancer.get_stats()
        }


    async def _fetch_preview(self, invite: InviteLink, client, db: AsyncSession) -> bool:
        try:
            from telethon.tl.functions.messages import CheckChatInviteRequest
            from telethon.tl.types import ChatInvite, ChatInviteAlready, ChatInvitePeek
            import os
            
            invite_hash = self._extract_hash(invite.link)
            if not invite_hash:
                return False
            
            try:
                preview = await client(CheckChatInviteRequest(hash=invite_hash))
            except Exception as e:
                invite.last_error = str(e)[:200]
                await db.commit()
                return False
            
            if isinstance(preview, ChatInviteAlready):
                chat = preview.chat
                invite.preview_title = getattr(chat, 'title', None)
                invite.preview_member_count = getattr(chat, 'participants_count', None)
                invite.preview_is_channel = getattr(chat, 'broadcast', False)
                invite.status = "already_joined"
                
                if hasattr(chat, 'photo') and chat.photo:
                    try:
                        os.makedirs("media/invite_previews", exist_ok=True)
                        photo_path = f"media/invite_previews/{invite_hash}.jpg"
                        await client.download_profile_photo(chat, file=photo_path)
                        if os.path.exists(photo_path):
                            invite.preview_photo_path = photo_path
                            logger.info(f"[AutoJoin] Downloaded preview photo: {photo_path}")
                    except Exception as photo_err:
                        logger.error(f"[AutoJoin] Photo download error: {photo_err}")
                        
            elif isinstance(preview, ChatInvitePeek):
                if hasattr(preview, 'chat'):
                    chat = preview.chat
                    invite.preview_title = getattr(chat, 'title', None)
                    invite.preview_member_count = getattr(chat, 'participants_count', None)
                    invite.preview_is_channel = getattr(chat, 'broadcast', False)
                    
            elif hasattr(preview, 'title'):
                invite.preview_title = preview.title
                invite.preview_about = getattr(preview, 'about', None)
                invite.preview_member_count = getattr(preview, 'participants_count', None)
                invite.preview_is_channel = getattr(preview, 'broadcast', False) or getattr(preview, 'channel', False)
                
                if hasattr(preview, 'photo') and preview.photo:
                    try:
                        os.makedirs("media/invite_previews", exist_ok=True)
                        photo_path = f"media/invite_previews/{invite_hash}.jpg"
                        await client.download_profile_photo(preview, file=photo_path)
                        if os.path.exists(photo_path):
                            invite.preview_photo_path = photo_path
                            logger.info(f"[AutoJoin] Downloaded preview photo: {photo_path}")
                    except Exception as photo_err:
                        logger.error(f"[AutoJoin] Photo download error: {photo_err}")
            
            invite.preview_fetched_at = datetime.utcnow()
            invite.invite_hash = invite_hash
            await db.commit()
            
            logger.info(f"[AutoJoin] Fetched preview: {invite.preview_title or invite.link}")
            return True
            
        except Exception as e:
            logger.error(f"[AutoJoin] Preview fetch error: {e}")
            return False
    
    async def fetch_all_previews(self) -> Dict[str, Any]:
        if not self.manager:
            return {"error": "No manager available"}
        
        stats = {"fetched": 0, "failed": 0, "skipped": 0}
        
        async with async_session_maker() as db:
            result = await db.execute(
                select(InviteLink).where(
                    and_(
                        InviteLink.preview_title.is_(None),
                        InviteLink.status.in_(["pending", "processing"])
                    )
                ).limit(50)
            )
            invites = result.scalars().all()
            
            if not invites:
                return {"message": "No invites need preview", **stats}
            
            client_info = await load_balancer.get_next_client()
            if not client_info:
                return {"error": "No connected accounts"}
            
            account_id, client = client_info
            
            for invite in invites:
                try:
                    success = await self._fetch_preview(invite, client, db)
                    if success:
                        stats["fetched"] += 1
                    else:
                        stats["failed"] += 1
                    
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    stats["failed"] += 1
                    logger.error(f"[AutoJoin] Preview error: {e}")
        
        return stats
    
    async def _preview_loop(self):
        await asyncio.sleep(60)
        
        while self._running:
            try:
                async with async_session_maker() as db:
                    result = await db.execute(
                        select(InviteLink).where(
                            and_(
                                InviteLink.preview_title.is_(None),
                                InviteLink.status.in_(["pending"]),
                                or_(
                                    InviteLink.preview_fetched_at.is_(None),
                                    InviteLink.preview_retry_count < self.MAX_PREVIEW_RETRIES
                                )
                            )
                        ).order_by(InviteLink.created_at.asc()).limit(10)
                    )
                    invites = result.scalars().all()
                    
                    if invites and self.manager:
                        client_info = await load_balancer.get_next_client()
                        if client_info:
                            _, client = client_info
                            for invite in invites:
                                if invite.preview_retry_count >= self.MAX_PREVIEW_RETRIES:
                                    logger.debug(f"[AutoJoin] Skipping preview for {invite.link} - max retries reached")
                                    continue
                                invite.preview_retry_count += 1
                                await self._fetch_preview(invite, client, db)
                                await asyncio.sleep(3)
                            await db.commit()
                
                await asyncio.sleep(120)
                
            except Exception as e:
                logger.error(f"[AutoJoin] Preview loop error: {e}")
                await asyncio.sleep(60)


autojoin_service = AutoJoinService()


def get_autojoin_service(telegram_manager=None) -> AutoJoinService:
    global autojoin_service
    if telegram_manager and not autojoin_service.manager:
        autojoin_service.manager = telegram_manager
    return autojoin_service
