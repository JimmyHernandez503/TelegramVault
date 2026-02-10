import asyncio
from datetime import datetime
from typing import Any, Callable, Optional
from telethon import events
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_user import TelegramUser
from backend.app.db.database import async_session_maker
from backend.app.services.websocket_manager import ws_manager
from backend.app.services.user_enricher import user_enricher
from backend.app.services.media_ingestion import media_ingestion
from backend.app.services.detection_service import detection_service
from backend.app.services.live_stats import live_stats
from backend.app.services.user_management_service import user_management_service
from backend.app.core.session_manager import session_manager
from backend.app.core.logging_config import get_logger

logger = get_logger("live_monitor")


class LiveMonitor:
    def __init__(self, telegram_manager):
        self.manager = telegram_manager
        self._channel_handlers: dict[int, tuple[int, Any]] = {}
        self._startup_done = False
        self.logger = logger
    
    def get_status(self) -> dict:
        return {
            "active_monitors": len(self._channel_handlers),
            "monitored_channels": list(self._channel_handlers.keys()),
            "startup_done": self._startup_done
        }
    
    async def start_monitor(self, account_id: int, channel_id: int, telegram_id: int):
        await self.stop_monitor(channel_id)
        
        await user_enricher.start_worker()
        await media_ingestion.start_workers()
        
        client = await self._ensure_client(account_id)
        if not client:
            raise RuntimeError("Account not connected and could not reconnect")
        
        if not await client.is_user_authorized():
            raise RuntimeError("Account not authorized")
        
        async def handler(event):
            msg = event.message
            await self._handle_new_message(account_id, channel_id, msg, client)
        
        try:
            entity = await client.get_input_entity(telegram_id)
        except Exception:
            entity = telegram_id
        
        ev = events.NewMessage(chats=entity)
        client.add_event_handler(handler, ev)
        self._channel_handlers[channel_id] = (account_id, handler)
        
        async with async_session_maker() as db:
            await db.execute(
                update(TelegramGroup).where(TelegramGroup.id == channel_id).values(
                    is_monitoring=True
                )
            )
            await db.commit()
        
        print(f"[LiveMonitor] Started monitoring channel {channel_id} (telegram_id={telegram_id})")
        self.logger.info(f"Started monitoring channel {channel_id} (telegram_id={telegram_id})")
    
    async def _ensure_client(self, account_id: int):
        client = self.manager.clients.get(account_id)
        if client and await client.is_user_authorized():
            return client
        
        print(f"[LiveMonitor] Client not connected for account {account_id}, attempting to connect...")
        from backend.app.models.telegram_account import TelegramAccount
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramAccount).where(TelegramAccount.id == account_id)
            )
            account = result.scalar_one_or_none()
            if account:
                await self.manager.connect_account(account_id, db)
                return self.manager.clients.get(account_id)
        
        return None
    
    async def stop_monitor(self, channel_id: int):
        if channel_id not in self._channel_handlers:
            return
        
        account_id, handler = self._channel_handlers.pop(channel_id)
        client = self.manager.clients.get(account_id)
        
        if client:
            try:
                client.remove_event_handler(handler)
            except Exception:
                pass
        
        async with async_session_maker() as db:
            await db.execute(
                update(TelegramGroup).where(TelegramGroup.id == channel_id).values(
                    is_monitoring=False
                )
            )
            await db.commit()
        
        print(f"[LiveMonitor] Stopped monitoring channel {channel_id}")
        self.logger.info(f"Stopped monitoring channel {channel_id}")
    
    async def _handle_new_message(self, account_id: int, channel_id: int, msg, client):
        async with async_session_maker() as db:
            existing = await db.execute(
                select(TelegramMessage).where(
                    TelegramMessage.telegram_id == msg.id,
                    TelegramMessage.group_id == channel_id
                )
            )
            if existing.scalar_one_or_none():
                return
            
            sender_id = None
            sender_name = "Unknown"
            sender_username = None
            sender_photo = None
            has_media = msg.media is not None
            media_type = "text"
            enrichment_pending = False
            
            # Only process sender if it's a user (positive ID), not a channel/group (negative ID)
            if msg.sender_id and msg.sender_id > 0:
                try:
                    sender = await msg.get_sender()
                    if sender:
                        # Use the enhanced User Management Service for robust user creation/update
                        user = await user_management_service.get_or_create_user(
                            telegram_id=msg.sender_id,
                            user_entity=sender
                        )
                        
                        if user:
                            sender_id = user.id
                            sender_name = f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username or "Unknown"
                            sender_username = user.username
                            sender_photo = user.current_photo_path
                            
                            # Update message count using the enhanced user management service
                            await user_management_service.update_user_message_count(user.id, 1)
                            
                            # If user data is incomplete, trigger enrichment
                            if sender_name == "Unknown" or not sender_photo:
                                from backend.app.services.enrichment_utils import trigger_user_enrichment
                                enrichment_pending = await trigger_user_enrichment(
                                    client=client,
                                    telegram_id=msg.sender_id,
                                    group_id=channel_id,
                                    source="live_monitor"
                                )
                            
                            await user_enricher.queue_enrichment(client, msg.sender_id, channel_id, source="live_monitor")
                        else:
                            self.logger.error(f"Failed to create/get user for sender_id {msg.sender_id}")
                except Exception as e:
                    self.logger.error(f"Error processing sender {msg.sender_id}: {e}")
                    # Continue processing the message even if user creation fails
            
            if msg.media:
                media_class = type(msg.media).__name__
                if "Photo" in media_class:
                    media_type = "photo"
                elif "Document" in media_class:
                    media_type = "document"
                elif "Video" in media_class:
                    media_type = "video"
            
            msg_date = msg.date.replace(tzinfo=None) if msg.date and msg.date.tzinfo else msg.date
            
            message = TelegramMessage(
                telegram_id=msg.id,
                group_id=channel_id,
                sender_id=sender_id,
                text=msg.message or "",
                message_type=media_type,
                date=msg_date or datetime.utcnow(),
                views=getattr(msg, 'views', None),
                forwards=getattr(msg, 'forwards', None),
                reply_to_msg_id=msg.reply_to.reply_to_msg_id if getattr(msg, 'reply_to', None) and hasattr(msg.reply_to, 'reply_to_msg_id') else None,
                grouped_id=getattr(msg, 'grouped_id', None),
            )
            db.add(message)
            await db.flush()
            
            live_stats.record("messages_saved")
            live_stats.record("messages_processed")
            
            if msg.media:
                await media_ingestion.queue_download(client, message.id, msg, channel_id)
                live_stats.record("media_queued")
            
            group_result = await db.execute(
                select(TelegramGroup).where(TelegramGroup.id == channel_id)
            )
            group = group_result.scalar_one_or_none()
            
            await db.execute(
                update(TelegramGroup).where(TelegramGroup.id == channel_id).values(
                    messages_count=TelegramGroup.messages_count + 1,
                    last_message_id=msg.id
                )
            )
            
            await db.commit()
            
            from backend.app.services.websocket_manager import WSMessage
            
            await ws_manager.broadcast("messages", WSMessage(
                event="new_message",
                data={
                    "id": message.id,
                    "telegram_id": msg.id,
                    "text": msg.message or "",
                    "sender_name": sender_name,
                    "sender_username": sender_username,
                    "sender_id": sender_id,
                    "sender_telegram_id": msg.sender_id,
                    "sender_photo": sender_photo,
                    "enrichment_pending": enrichment_pending,
                    "group_id": channel_id,
                    "group_name": group.title if group else "Unknown",
                    "timestamp": msg_date.isoformat() if msg_date else None,
                    "has_media": has_media,
                    "media_type": media_type,
                    "detections": []
                }
            ))
            
            if msg.message:
                try:
                    account_id = group.assigned_account_id if group else None
                    await detection_service.process_message(
                        message_id=message.id,
                        text=msg.message,
                        group_id=channel_id,
                        sender_id=sender_id,
                        account_id=account_id
                    )
                except Exception as e:
                    self.logger.error(f"Detection error for message {message.id}: {e}")
    
    def get_active_monitors(self) -> list[int]:
        return list(self._channel_handlers.keys())
    
    def is_monitoring(self, channel_id: int) -> bool:
        return channel_id in self._channel_handlers
    
    async def start_all_enabled(self):
        if self._startup_done:
            return
        
        self._startup_done = True
        
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramGroup).where(
                    TelegramGroup.status == "active",
                    TelegramGroup.assigned_account_id.isnot(None)
                )
            )
            groups = result.scalars().all()
        
        started = 0
        for group in groups:
            try:
                if group.assigned_account_id:
                    await self.start_monitor(
                        account_id=group.assigned_account_id,
                        channel_id=group.id,
                        telegram_id=group.telegram_id
                    )
                    started += 1
            except Exception as e:
                print(f"[LiveMonitor] Failed to start monitor for group {group.id}: {e}")
        
        print(f"[LiveMonitor] Auto-started {started} monitors on startup")
    
    async def auto_start_for_group(self, group_id: int):
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramGroup).where(TelegramGroup.id == group_id)
            )
            group = result.scalar_one_or_none()
            
            if group and group.assigned_account_id and group.status == "active":
                try:
                    await self.start_monitor(
                        account_id=group.assigned_account_id,
                        channel_id=group.id,
                        telegram_id=group.telegram_id
                    )
                    print(f"[LiveMonitor] Auto-started monitor for newly assigned group {group_id}")
                except Exception as e:
                    print(f"[LiveMonitor] Failed to auto-start for group {group_id}: {e}")
    
    async def stop_all(self):
        for channel_id in list(self._channel_handlers.keys()):
            try:
                await self.stop_monitor(channel_id)
            except Exception:
                pass
