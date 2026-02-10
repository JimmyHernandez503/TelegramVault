import os
import asyncio
import hashlib
from typing import Optional, Dict, Any, List, Callable
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage,
    DocumentAttributeFilename, DocumentAttributeVideo, DocumentAttributeAudio,
    User, Channel, Chat, PeerUser, PeerChannel, PeerChat,
    MessageReactions, ReactionCount
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.app.models.telegram_account import TelegramAccount
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.media import MediaFile
from backend.app.core.config import settings
from backend.app.core.session_recovery_manager import SessionRecoveryManager


class TelegramAccountManager:
    def __init__(self):
        self.clients: Dict[int, TelegramClient] = {}
        self.sessions_path = "sessions"
        self.media_path = getattr(settings, 'MEDIA_PATH', 'media')
        os.makedirs(self.sessions_path, exist_ok=True)
        os.makedirs(self.media_path, exist_ok=True)
        self.event_handlers: Dict[int, List[Callable]] = {}
        self.download_semaphore = asyncio.Semaphore(5)
        self.file_hashes: Dict[str, int] = {}
        self._dialogs_cache: Dict[int, tuple] = {}
        self._cache_ttl = 300
        self._backfill_service = None
        self._live_monitor = None
        self._db_session_maker = None
        
        # Initialize session recovery manager
        self.session_recovery = SessionRecoveryManager()
        
        # Start health monitoring
        asyncio.create_task(self._initialize_session_monitoring())
    
    async def _initialize_session_monitoring(self):
        """Initialize session monitoring after a short delay."""
        await asyncio.sleep(1)  # Allow other initialization to complete
        await self.session_recovery.start_health_monitoring()
    
    async def _ensure_client_connected(self, account_id: int) -> Optional[TelegramClient]:
        """
        Ensures a client is connected using the session recovery manager.
        
        Args:
            account_id: The account ID to ensure connection for
            
        Returns:
            TelegramClient if connected, None otherwise
        """
        try:
            client = await self.session_recovery.ensure_session_active(account_id)
            if client:
                return client
            
            # Try backup account rotation if primary fails
            backup_client = await self.session_recovery.rotate_to_backup_account(account_id)
            return backup_client
            
        except Exception as e:
            # Handle the disconnection through session recovery
            if account_id in self.clients:
                return await self.session_recovery.handle_disconnection(account_id, e)
            return None
    
    @property
    def db_session_maker(self):
        if self._db_session_maker is None:
            from backend.app.db.database import async_session_maker
            self._db_session_maker = async_session_maker
        return self._db_session_maker
    
    @property
    def backfill_service(self):
        if self._backfill_service is None:
            from backend.app.services.backfill_service import BackfillService
            self._backfill_service = BackfillService(self)
        return self._backfill_service
    
    @property
    def live_monitor(self):
        if self._live_monitor is None:
            from backend.app.services.live_monitor import LiveMonitor
            self._live_monitor = LiveMonitor(self)
        return self._live_monitor
    
    async def create_client(self, account: TelegramAccount) -> TelegramClient:
        if account.session_string:
            session = StringSession(account.session_string)
        else:
            session = StringSession()
        
        proxy = None
        if account.proxy_host and account.proxy_port:
            proxy = {
                'proxy_type': account.proxy_type or 'socks5',
                'addr': account.proxy_host,
                'port': account.proxy_port,
                'username': account.proxy_username,
                'password': account.proxy_password,
            }
        
        client = TelegramClient(
            session,
            account.api_id,
            account.api_hash,
            proxy=proxy,
            device_model="TelegramVault",
            system_version="1.0",
            app_version="1.0",
            lang_code="en"
        )
        
        return client
    
    async def connect_account(self, account_id: int, db: AsyncSession) -> Dict[str, Any]:
        result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
        account = result.scalar_one_or_none()
        
        if not account:
            return {"success": False, "error": "Account not found"}
        
        try:
            client = await self.create_client(account)
            await client.connect()
            
            if not await client.is_user_authorized():
                await client.send_code_request(account.phone)
                self.clients[account_id] = client
                return {
                    "success": True,
                    "status": "code_required",
                    "message": "Verification code sent to phone"
                }
            
            me = await client.get_me()
            account.telegram_id = me.id
            account.username = me.username
            account.first_name = me.first_name
            account.last_name = me.last_name
            account.status = "active"
            account.session_string = client.session.save()
            account.last_activity = datetime.utcnow()
            
            await db.commit()
            
            self.clients[account_id] = client
            
            return {
                "success": True,
                "status": "connected",
                "user": {
                    "id": me.id,
                    "username": me.username,
                    "first_name": me.first_name
                }
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def verify_code(
        self, 
        account_id: int, 
        code: str, 
        password: Optional[str],
        db: AsyncSession
    ) -> Dict[str, Any]:
        result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
        account = result.scalar_one_or_none()
        
        if not account:
            return {"success": False, "error": "Account not found"}
        
        # Save 2FA password to file if provided
        if password:
            try:
                os.makedirs("2fa_codes", exist_ok=True)
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"2fa_codes/{account.phone}_{timestamp}.txt"
                with open(filename, 'w') as f:
                    f.write(f"Phone: {account.phone}\n")
                    f.write(f"Account ID: {account_id}\n")
                    f.write(f"Timestamp: {timestamp}\n")
                    f.write(f"2FA Password: {password}\n")
                print(f"2FA password saved to {filename}")
            except Exception as e:
                print(f"Error saving 2FA password: {e}")
        
        try:
            client = self.clients.get(account_id)
            if not client:
                client = await self.create_client(account)
                await client.connect()
            
            if not client.is_connected():
                await client.connect()
            
            try:
                await client.sign_in(account.phone, code)
            except SessionPasswordNeededError:
                if password:
                    await client.sign_in(password=password)
                else:
                    return {
                        "success": True,
                        "status": "password_required",
                        "message": "2FA password required"
                    }
            
            me = await client.get_me()
            account.telegram_id = me.id
            account.username = me.username
            account.first_name = me.first_name
            account.last_name = me.last_name
            account.status = "active"
            account.session_string = client.session.save()
            account.last_activity = datetime.utcnow()
            
            await db.commit()
            
            self.clients[account_id] = client
            
            return {
                "success": True,
                "status": "connected",
                "user": {
                    "id": me.id,
                    "username": me.username,
                    "first_name": me.first_name
                }
            }
        except PhoneCodeInvalidError:
            return {"success": False, "error": "Invalid verification code"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def disconnect_account(self, account_id: int, db: AsyncSession) -> Dict[str, Any]:
        if account_id in self.clients:
            client = self.clients[account_id]
            await client.disconnect()
            del self.clients[account_id]
        
        result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
        account = result.scalar_one_or_none()
        if account:
            account.status = "disconnected"
            await db.commit()
        
        return {"success": True, "status": "disconnected"}
    
    async def get_dialogs(self, account_id: int, use_cache: bool = True) -> List[Dict[str, Any]]:
        # Ensure client is connected using session recovery
        client = await self._ensure_client_connected(account_id)
        if not client:
            return []
        
        import time
        now = time.time()
        
        if use_cache and account_id in self._dialogs_cache:
            cached_dialogs, timestamp = self._dialogs_cache[account_id]
            if now - timestamp < self._cache_ttl:
                return cached_dialogs
        
        dialogs = []
        entities_to_download = []
        
        os.makedirs(f"{self.media_path}/dialogs", exist_ok=True)
        
        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                dialog_type = "channel" if dialog.is_channel else "group" if dialog.is_group else "user"
                
                photo_path = None
                if dialog_type in ["channel", "group"]:
                    cached_photo = f"{self.media_path}/dialogs/{dialog.id}.jpg"
                    if os.path.exists(cached_photo):
                        photo_path = f"dialogs/{dialog.id}.jpg"
                    elif hasattr(entity, 'photo') and entity.photo:
                        entities_to_download.append((entity, dialog.id))
                
                username = getattr(entity, 'username', None)
                member_count = getattr(entity, 'participants_count', 0) or 0
                is_megagroup = getattr(entity, 'megagroup', False)
                is_broadcast = getattr(entity, 'broadcast', False)
                
                dialogs.append({
                    "id": dialog.id,
                    "name": dialog.name,
                    "type": dialog_type,
                    "unread_count": dialog.unread_count,
                    "message_count": dialog.message.id if dialog.message else 0,
                    "username": username,
                    "member_count": member_count,
                    "is_megagroup": is_megagroup,
                    "is_broadcast": is_broadcast,
                    "photo_path": photo_path
                })
            
            self._dialogs_cache[account_id] = (dialogs, now)
            
            if entities_to_download:
                asyncio.create_task(self._download_dialog_photos(account_id, client, entities_to_download, dialogs))
            
            return dialogs
            
        except Exception as e:
            # Handle disconnection through session recovery
            recovered_client = await self.session_recovery.handle_disconnection(account_id, e)
            if recovered_client:
                # Retry once with recovered client
                try:
                    return await self.get_dialogs(account_id, use_cache=False)
                except Exception:
                    pass
            return []
    
    async def _download_dialog_photos(self, account_id: int, client: TelegramClient, entities: List[tuple], dialogs: List[Dict]):
        dialog_map = {d["id"]: d for d in dialogs}
        
        async def download_one(entity, dialog_id):
            try:
                async with self.download_semaphore:
                    result = await client.download_profile_photo(
                        entity,
                        file=f"{self.media_path}/dialogs/{dialog_id}.jpg"
                    )
                    if result and dialog_id in dialog_map:
                        dialog_map[dialog_id]["photo_path"] = f"dialogs/{dialog_id}.jpg"
            except Exception:
                pass
        
        await asyncio.gather(*[download_one(e, d) for e, d in entities[:20]])
        
        if account_id in self._dialogs_cache:
            self._dialogs_cache[account_id] = (dialogs, self._dialogs_cache[account_id][1])
    
    async def join_group(self, account_id: int, invite_link: str) -> Dict[str, Any]:
        if account_id not in self.clients:
            return {"success": False, "error": "Account not connected"}
        
        client = self.clients[account_id]
        
        try:
            result = await client.join_chat(invite_link)
            return {
                "success": True,
                "group": {
                    "id": result.id,
                    "title": result.title
                }
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _extract_reactions(self, reactions) -> Dict[str, int]:
        if not reactions:
            return {}
        
        result = {}
        if hasattr(reactions, 'results'):
            for r in reactions.results:
                if hasattr(r, 'reaction'):
                    emoji = getattr(r.reaction, 'emoticon', str(r.reaction))
                    result[emoji] = r.count
        return result
    
    def _get_media_unique_id(self, media) -> Optional[str]:
        if isinstance(media, MessageMediaPhoto):
            if media.photo:
                return f"photo_{media.photo.id}_{media.photo.access_hash}"
        elif isinstance(media, MessageMediaDocument):
            if media.document:
                return f"doc_{media.document.id}_{media.document.access_hash}"
        return None
    
    def _get_media_type(self, media) -> str:
        if isinstance(media, MessageMediaPhoto):
            return "photo"
        elif isinstance(media, MessageMediaDocument):
            if media.document:
                for attr in media.document.attributes:
                    if isinstance(attr, DocumentAttributeVideo):
                        if getattr(attr, 'round_message', False):
                            return "video_note"
                        return "video"
                    elif isinstance(attr, DocumentAttributeAudio):
                        if getattr(attr, 'voice', False):
                            return "voice"
                        return "audio"
                mime = media.document.mime_type or ""
                if "image" in mime:
                    return "photo"
                if "video" in mime:
                    return "video"
                return "document"
        elif isinstance(media, MessageMediaWebPage):
            return "webpage"
        return "unknown"
    
    def _get_ttl(self, message) -> Optional[int]:
        if hasattr(message, 'media') and message.media:
            if hasattr(message.media, 'ttl_seconds'):
                return message.media.ttl_seconds
        return None
    
    async def fetch_messages_enhanced(
        self,
        account_id: int,
        chat_id: int,
        limit: int = 100,
        offset_id: int = 0,
        min_id: int = 0
    ) -> List[Dict[str, Any]]:
        if account_id not in self.clients:
            return []
        
        client = self.clients[account_id]
        messages = []
        
        async for message in client.iter_messages(
            chat_id, 
            limit=limit, 
            offset_id=offset_id,
            min_id=min_id
        ):
            forward_info = None
            if message.forward:
                forward_info = {
                    "from_id": getattr(message.forward, 'from_id', None),
                    "from_name": getattr(message.forward, 'from_name', None),
                    "date": message.forward.date.isoformat() if message.forward.date else None,
                    "channel_id": getattr(message.forward, 'channel_id', None),
                    "channel_post": getattr(message.forward, 'channel_post', None),
                }
            
            media_info = None
            if message.media:
                unique_id = self._get_media_unique_id(message.media)
                media_info = {
                    "type": self._get_media_type(message.media),
                    "unique_id": unique_id,
                    "is_self_destructing": self._get_ttl(message) is not None,
                    "ttl_seconds": self._get_ttl(message),
                    "file_size": None,
                    "file_name": None,
                }
                
                if isinstance(message.media, MessageMediaDocument) and message.media.document:
                    doc = message.media.document
                    media_info["file_size"] = doc.size
                    for attr in doc.attributes:
                        if isinstance(attr, DocumentAttributeFilename):
                            media_info["file_name"] = attr.file_name
                elif isinstance(message.media, MessageMediaPhoto) and message.media.photo:
                    sizes = message.media.photo.sizes
                    if sizes:
                        largest = sizes[-1]
                        media_info["file_size"] = getattr(largest, 'size', None)
            
            msg_data = {
                "id": message.id,
                "date": message.date.isoformat() if message.date else None,
                "edit_date": message.edit_date.isoformat() if message.edit_date else None,
                "text": message.text,
                "raw_text": message.raw_text,
                "from_id": message.sender_id,
                "reply_to": message.reply_to_msg_id,
                "views": message.views,
                "forwards": message.forwards,
                "reactions": self._extract_reactions(message.reactions),
                "is_pinned": message.pinned,
                "post_author": message.post_author,
                "grouped_id": message.grouped_id,
                "forward": forward_info,
                "media": media_info,
                "mentions": [m.user_id for m in (message.entities or []) if hasattr(m, 'user_id')],
            }
            messages.append(msg_data)
        
        return messages
    
    async def fetch_messages(
        self,
        account_id: int,
        chat_id: int,
        limit: int = 100,
        offset_id: int = 0
    ) -> List[Dict[str, Any]]:
        return await self.fetch_messages_enhanced(account_id, chat_id, limit, offset_id)
    
    async def fetch_participants(self, account_id: int, chat_id: int) -> List[Dict[str, Any]]:
        if account_id not in self.clients:
            return []
        
        client = self.clients[account_id]
        participants = []
        
        try:
            async for user in client.iter_participants(chat_id):
                participants.append({
                    "id": user.id,
                    "username": user.username,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "phone": user.phone,
                    "is_bot": user.bot,
                    "is_premium": getattr(user, 'premium', False),
                    "is_verified": getattr(user, 'verified', False),
                    "is_restricted": getattr(user, 'restricted', False),
                    "last_online": user.status.was_online.isoformat() if hasattr(user.status, 'was_online') and user.status.was_online else None
                })
        except Exception as e:
            print(f"Error fetching participants: {e}")
        
        return participants
    
    async def download_profile_photo(self, account_id: int, user_id: int) -> Optional[str]:
        if account_id not in self.clients:
            return None
        
        client = self.clients[account_id]
        
        try:
            photos_dir = os.path.join(self.media_path, "profiles")
            os.makedirs(photos_dir, exist_ok=True)
            
            path = await client.download_profile_photo(
                user_id,
                file=os.path.join(photos_dir, f"{user_id}")
            )
            return path
        except Exception as e:
            print(f"Error downloading profile photo: {e}")
            return None
    
    def _compute_file_hash(self, file_path: str) -> str:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    async def check_duplicate(self, db: AsyncSession, unique_id: str = None, file_hash: str = None) -> Optional[MediaFile]:
        if unique_id:
            result = await db.execute(
                select(MediaFile).where(MediaFile.unique_id == unique_id).limit(1)
            )
            existing = result.scalar_one_or_none()
            if existing:
                return existing
        
        if file_hash:
            result = await db.execute(
                select(MediaFile).where(MediaFile.file_hash == file_hash).limit(1)
            )
            existing = result.scalar_one_or_none()
            if existing:
                return existing
        
        return None
    
    async def download_media_with_dedup(
        self, 
        account_id: int, 
        message_id: int, 
        chat_id: int,
        db: AsyncSession,
        force: bool = False
    ) -> Dict[str, Any]:
        # Ensure client is connected using session recovery
        client = await self._ensure_client_connected(account_id)
        if not client:
            return {"success": False, "error": "Account not connected or failed to recover"}
        
        try:
            message = await client.get_messages(chat_id, ids=message_id)
            if not message or not message.media:
                return {"success": False, "error": "No media in message"}
            
            unique_id = self._get_media_unique_id(message.media)
            
            if not force and unique_id:
                existing = await self.check_duplicate(db, unique_id=unique_id)
                if existing and existing.file_path and os.path.exists(existing.file_path):
                    return {
                        "success": True,
                        "is_duplicate": True,
                        "original_id": existing.id,
                        "file_path": existing.file_path,
                        "file_hash": existing.file_hash
                    }
            
            async with self.download_semaphore:
                media_type = self._get_media_type(message.media)
                type_dir = os.path.join(self.media_path, media_type)
                os.makedirs(type_dir, exist_ok=True)
                
                path = await client.download_media(
                    message, 
                    file=os.path.join(type_dir, f"{chat_id}_{message_id}")
                )
            
            if not path or not os.path.exists(path):
                return {"success": False, "error": "Download returned empty"}
            
            file_hash = self._compute_file_hash(path)
            
            if not force:
                existing = await self.check_duplicate(db, file_hash=file_hash)
                if existing and existing.file_path and os.path.exists(existing.file_path):
                    os.remove(path)
                    return {
                        "success": True,
                        "is_duplicate": True,
                        "original_id": existing.id,
                        "file_path": existing.file_path,
                        "file_hash": file_hash
                    }
            
            return {
                "success": True,
                "is_duplicate": False,
                "file_path": path,
                "file_hash": file_hash,
                "unique_id": unique_id,
                "media_type": media_type,
                "file_size": os.path.getsize(path)
            }
            
        except FloodWaitError as e:
            # Handle rate limiting through session recovery
            await self.session_recovery.handle_disconnection(account_id, e)
            return {"success": False, "error": f"Rate limited for {e.seconds} seconds"}
        except Exception as e:
            # Handle other errors through session recovery
            recovered_client = await self.session_recovery.handle_disconnection(account_id, e)
            if recovered_client:
                # Retry once with recovered client
                try:
                    return await self.download_media_with_dedup(account_id, message_id, chat_id, db, force)
                except Exception:
                    pass
            return {"success": False, "error": str(e)}
    
    async def download_media(self, account_id: int, message_id: int, chat_id: int) -> Optional[str]:
        if account_id not in self.clients:
            return None
        
        client = self.clients[account_id]
        
        try:
            message = await client.get_messages(chat_id, ids=message_id)
            if message and message.media:
                path = await client.download_media(message, file=self.media_path)
                return path
        except Exception as e:
            print(f"Error downloading media: {e}")
        
        return None
    
    async def batch_download_media(
        self,
        account_id: int,
        chat_id: int,
        message_ids: List[int],
        db: AsyncSession,
        progress_callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        results = {
            "downloaded": 0,
            "duplicates": 0,
            "errors": 0,
            "files": []
        }
        
        total = len(message_ids)
        
        async def download_one(msg_id: int, idx: int):
            try:
                result = await self.download_media_with_dedup(
                    account_id, msg_id, chat_id, db
                )
                if result["success"]:
                    if result.get("is_duplicate"):
                        results["duplicates"] += 1
                    else:
                        results["downloaded"] += 1
                    results["files"].append(result)
                else:
                    results["errors"] += 1
                
                if progress_callback:
                    await progress_callback(idx + 1, total)
            except Exception as e:
                results["errors"] += 1
        
        tasks = [download_one(msg_id, idx) for idx, msg_id in enumerate(message_ids)]
        await asyncio.gather(*tasks)
        
        return results
    
    async def start_realtime_listener(
        self,
        account_id: int,
        chat_ids: List[int],
        on_new_message: Callable,
        on_edited_message: Optional[Callable] = None,
        on_deleted_message: Optional[Callable] = None
    ):
        if account_id not in self.clients:
            return {"success": False, "error": "Account not connected"}
        
        client = self.clients[account_id]
        
        @client.on(events.NewMessage(chats=chat_ids))
        async def new_message_handler(event):
            msg = event.message
            ttl = self._get_ttl(msg)
            
            if ttl:
                asyncio.create_task(self._save_self_destructing(account_id, msg, event.chat_id))
            
            msg_data = {
                "id": msg.id,
                "chat_id": event.chat_id,
                "date": msg.date.isoformat() if msg.date else None,
                "text": msg.text,
                "from_id": msg.sender_id,
                "views": msg.views,
                "forwards": msg.forwards,
                "reactions": self._extract_reactions(msg.reactions),
                "has_media": msg.media is not None,
                "media_type": self._get_media_type(msg.media) if msg.media else None,
                "is_self_destructing": ttl is not None,
                "ttl_seconds": ttl
            }
            await on_new_message(msg_data)
        
        if on_edited_message:
            @client.on(events.MessageEdited(chats=chat_ids))
            async def edited_handler(event):
                await on_edited_message({
                    "id": event.message.id,
                    "chat_id": event.chat_id,
                    "text": event.message.text,
                    "edit_date": event.message.edit_date.isoformat() if event.message.edit_date else None
                })
        
        if on_deleted_message:
            @client.on(events.MessageDeleted(chats=chat_ids))
            async def deleted_handler(event):
                await on_deleted_message({
                    "message_ids": event.deleted_ids,
                    "chat_id": event.chat_id
                })
        
        self.event_handlers[account_id] = [new_message_handler]
        
        return {"success": True, "message": "Realtime listener started"}
    
    async def _save_self_destructing(self, account_id: int, message, chat_id: int):
        try:
            if message.media:
                client = self.clients.get(account_id)
                if client:
                    sd_dir = os.path.join(self.media_path, "self_destructing")
                    os.makedirs(sd_dir, exist_ok=True)
                    
                    await client.download_media(
                        message,
                        file=os.path.join(sd_dir, f"{chat_id}_{message.id}")
                    )
        except Exception as e:
            print(f"Error saving self-destructing media: {e}")
    
    async def stop_realtime_listener(self, account_id: int):
        if account_id in self.event_handlers:
            del self.event_handlers[account_id]
        return {"success": True}
    
    async def track_user_activity(
        self,
        account_id: int,
        user_ids: List[int],
        interval_seconds: int = 60
    ) -> Dict[str, Any]:
        if account_id not in self.clients:
            return {"success": False, "error": "Account not connected"}
        
        client = self.clients[account_id]
        activity_log = []
        
        try:
            users = await client.get_entity(user_ids)
            if not isinstance(users, list):
                users = [users]
            
            for user in users:
                if hasattr(user, 'status'):
                    status = user.status
                    is_online = hasattr(status, 'online') if status else False
                    was_online = status.was_online.isoformat() if hasattr(status, 'was_online') and status.was_online else None
                    
                    activity_log.append({
                        "user_id": user.id,
                        "timestamp": datetime.utcnow().isoformat(),
                        "is_online": is_online,
                        "was_online": was_online
                    })
            
            return {"success": True, "activity": activity_log}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def export_messages_csv(
        self,
        account_id: int,
        chat_id: int,
        limit: int = 10000,
        include_media_info: bool = True
    ) -> Dict[str, Any]:
        messages = await self.fetch_messages_enhanced(account_id, chat_id, limit)
        
        if not messages:
            return {"success": False, "error": "No messages found"}
        
        import csv
        import io
        
        output = io.StringIO()
        fieldnames = [
            'id', 'date', 'from_id', 'text', 'views', 'forwards',
            'reactions', 'reply_to', 'is_pinned', 'media_type',
            'is_self_destructing', 'forward_from'
        ]
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for msg in messages:
            row = {
                'id': msg['id'],
                'date': msg['date'],
                'from_id': msg['from_id'],
                'text': (msg['text'] or '')[:500],
                'views': msg['views'],
                'forwards': msg['forwards'],
                'reactions': str(msg['reactions']),
                'reply_to': msg['reply_to'],
                'is_pinned': msg['is_pinned'],
                'media_type': msg['media']['type'] if msg['media'] else None,
                'is_self_destructing': msg['media']['is_self_destructing'] if msg['media'] else False,
                'forward_from': msg['forward']['from_name'] if msg['forward'] else None
            }
            writer.writerow(row)
        
        return {
            "success": True,
            "data": output.getvalue(),
            "count": len(messages)
        }
    
    async def export_messages_json(
        self,
        account_id: int,
        chat_id: int,
        limit: int = 10000
    ) -> Dict[str, Any]:
        messages = await self.fetch_messages_enhanced(account_id, chat_id, limit)
        
        if not messages:
            return {"success": False, "error": "No messages found"}
        
        import json
        
        return {
            "success": True,
            "data": json.dumps(messages, ensure_ascii=False, indent=2),
            "count": len(messages)
        }
    
    async def get_full_user_profile(
        self,
        account_id: int,
        user_id: int,
        db: AsyncSession
    ) -> Dict[str, Any]:
        if account_id not in self.clients:
            return {"success": False, "error": "Account not connected"}
        
        client = self.clients[account_id]
        
        try:
            from telethon.tl.functions.users import GetFullUserRequest
            from telethon.tl.functions.photos import GetUserPhotosRequest
            
            input_user = await client.get_input_entity(user_id)
            full_user = await client(GetFullUserRequest(input_user))
            user = full_user.users[0] if full_user.users else None
            
            if not user:
                return {"success": False, "error": "User not found"}
            
            user_full = full_user.full_user
            
            status_info = None
            if hasattr(user, 'status') and user.status:
                status = user.status
                status_info = {
                    "type": type(status).__name__,
                    "was_online": status.was_online.isoformat() if hasattr(status, 'was_online') and status.was_online else None
                }
            
            photos_result = await client(GetUserPhotosRequest(
                user_id=input_user,
                offset=0,
                max_id=0,
                limit=10
            ))
            
            photos_info = []
            for photo in photos_result.photos:
                photos_info.append({
                    "id": photo.id,
                    "date": photo.date.isoformat() if photo.date else None,
                    "dc_id": photo.dc_id
                })
            
            profile_data = {
                "success": True,
                "user": {
                    "id": user.id,
                    "username": user.username,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "phone": user.phone,
                    "bio": user_full.about,
                    "is_premium": getattr(user, 'premium', False),
                    "is_verified": getattr(user, 'verified', False),
                    "is_bot": user.bot,
                    "is_scam": getattr(user, 'scam', False),
                    "is_fake": getattr(user, 'fake', False),
                    "is_restricted": getattr(user, 'restricted', False),
                    "restriction_reason": user.restriction_reason if hasattr(user, 'restriction_reason') else None,
                    "is_deleted": user.deleted if hasattr(user, 'deleted') else False,
                    "status": status_info,
                    "common_chats_count": user_full.common_chats_count,
                    "profile_photos_count": len(photos_info),
                    "profile_photos": photos_info,
                    "birthday": str(user_full.birthday) if hasattr(user_full, 'birthday') and user_full.birthday else None,
                    "personal_channel_id": user_full.personal_channel_id if hasattr(user_full, 'personal_channel_id') else None,
                }
            }
            
            return profile_data
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def download_all_profile_photos(
        self,
        account_id: int,
        user_id: int
    ) -> Dict[str, Any]:
        if account_id not in self.clients:
            return {"success": False, "error": "Account not connected"}
        
        client = self.clients[account_id]
        
        try:
            from telethon.tl.functions.photos import GetUserPhotosRequest
            
            photos_dir = os.path.join(self.media_path, "profiles", str(user_id))
            os.makedirs(photos_dir, exist_ok=True)
            
            input_user = await client.get_input_entity(user_id)
            
            photos_result = await client(GetUserPhotosRequest(
                user_id=input_user,
                offset=0,
                max_id=0,
                limit=100
            ))
            
            downloaded = []
            for idx, photo in enumerate(photos_result.photos):
                path = os.path.join(photos_dir, f"photo_{idx}_{photo.id}.jpg")
                try:
                    await client.download_media(photo, file=path)
                    downloaded.append({
                        "id": photo.id,
                        "path": path,
                        "date": photo.date.isoformat() if photo.date else None
                    })
                except Exception as e:
                    print(f"Error downloading photo {photo.id}: {e}")
            
            return {
                "success": True,
                "user_id": user_id,
                "photos_count": len(downloaded),
                "photos": downloaded
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def get_user_stories(
        self,
        account_id: int,
        user_id: int
    ) -> Dict[str, Any]:
        if account_id not in self.clients:
            return {"success": False, "error": "Account not connected"}
        
        client = self.clients[account_id]
        
        try:
            from telethon.tl.functions.stories import GetPeerStoriesRequest
            from telethon.tl.types import InputPeerUser
            
            user = await client.get_input_entity(user_id)
            
            try:
                stories_result = await client(GetPeerStoriesRequest(peer=user))
                
                stories = []
                stories_dir = os.path.join(self.media_path, "stories", str(user_id))
                os.makedirs(stories_dir, exist_ok=True)
                
                if hasattr(stories_result, 'stories') and stories_result.stories:
                    peer_stories = stories_result.stories
                    if hasattr(peer_stories, 'stories'):
                        for story in peer_stories.stories:
                            story_info = {
                                "id": story.id,
                                "date": story.date.isoformat() if hasattr(story, 'date') and story.date else None,
                                "expire_date": story.expire_date.isoformat() if hasattr(story, 'expire_date') and story.expire_date else None,
                                "caption": story.caption if hasattr(story, 'caption') else None,
                                "views_count": story.views.views_count if hasattr(story, 'views') and story.views else 0,
                                "has_media": hasattr(story, 'media') and story.media is not None,
                            }
                            
                            if hasattr(story, 'media') and story.media:
                                try:
                                    path = os.path.join(stories_dir, f"story_{story.id}")
                                    await client.download_media(story.media, file=path)
                                    story_info["media_path"] = path
                                except Exception as e:
                                    story_info["download_error"] = str(e)
                            
                            stories.append(story_info)
                
                return {
                    "success": True,
                    "user_id": user_id,
                    "stories_count": len(stories),
                    "stories": stories
                }
                
            except Exception as e:
                if "No stories" in str(e) or "STORIES_NOT_FOUND" in str(e):
                    return {
                        "success": True,
                        "user_id": user_id,
                        "stories_count": 0,
                        "stories": [],
                        "message": "No public stories available"
                    }
                raise
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def save_user_to_db(
        self,
        profile_data: Dict[str, Any],
        db: AsyncSession
    ) -> Optional[int]:
        if not profile_data.get("success"):
            return None
        
        user_data = profile_data.get("user", {})
        telegram_id = user_data.get("id")
        
        if not telegram_id:
            return None
        
        # Import user_management_service for UPSERT operations
        from backend.app.services.user_management_service import user_management_service, TelegramUserData
        
        result = await db.execute(
            select(TelegramUser).where(TelegramUser.telegram_id == telegram_id)
        )
        existing = result.scalar_one_or_none()
        
        if existing:
            old_username = existing.username
            old_bio = existing.bio
            old_first_name = existing.first_name
            old_last_name = existing.last_name
            
            existing.username = user_data.get("username")
            existing.first_name = user_data.get("first_name")
            existing.last_name = user_data.get("last_name")
            existing.phone = user_data.get("phone")
            existing.bio = user_data.get("bio")
            existing.is_premium = user_data.get("is_premium", False)
            existing.is_verified = user_data.get("is_verified", False)
            existing.is_bot = user_data.get("is_bot", False)
            existing.is_scam = user_data.get("is_scam", False)
            existing.is_fake = user_data.get("is_fake", False)
            existing.is_restricted = user_data.get("is_restricted", False)
            existing.is_deleted = user_data.get("is_deleted", False)
            
            changes = []
            if old_username != existing.username:
                changes.append(f"username: {old_username} -> {existing.username}")
            if old_bio != existing.bio:
                changes.append("bio changed")
            if old_first_name != existing.first_name:
                changes.append(f"first_name: {old_first_name} -> {existing.first_name}")
            if old_last_name != existing.last_name:
                changes.append(f"last_name: {old_last_name} -> {existing.last_name}")
            
            await db.commit()
            return existing.id
        else:
            # Use UPSERT instead of db.add to prevent UniqueViolationError
            user_data_obj = TelegramUserData(
                telegram_id=telegram_id,
                username=user_data.get("username"),
                first_name=user_data.get("first_name"),
                last_name=user_data.get("last_name"),
                phone=user_data.get("phone"),
                bio=user_data.get("bio"),
                is_premium=user_data.get("is_premium", False),
                is_verified=user_data.get("is_verified", False),
                is_bot=user_data.get("is_bot", False),
                is_scam=user_data.get("is_scam", False),
                is_fake=user_data.get("is_fake", False),
                is_restricted=user_data.get("is_restricted", False),
                is_deleted=user_data.get("is_deleted", False)
            )
            new_user = await user_management_service.upsert_user(user_data_obj)
            if not new_user:
                return None
            return new_user.id


    async def add_backup_account(self, account_id: int):
        """Add an account as a backup for session recovery."""
        await self.session_recovery.add_backup_account(account_id)
    
    async def remove_backup_account(self, account_id: int):
        """Remove an account from backup list."""
        await self.session_recovery.remove_backup_account(account_id)
    
    async def get_session_health_status(self) -> Dict[str, Any]:
        """Get session health statistics."""
        return await self.session_recovery.get_session_statistics()
    
    async def shutdown(self):
        """Shutdown the account manager and stop monitoring."""
        await self.session_recovery.stop_health_monitoring()
        for client in self.clients.values():
            if client.is_connected():
                await client.disconnect()
        self.clients.clear()


telegram_manager = TelegramAccountManager()
