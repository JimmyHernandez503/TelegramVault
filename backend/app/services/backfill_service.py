import asyncio
import logging
from datetime import datetime
from typing import Optional, Callable, Any
from telethon.errors import FloodWaitError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.membership import GroupMembership
from backend.app.db.database import async_session_maker
from backend.app.services.user_enricher import user_enricher
from backend.app.services.media_ingestion import media_ingestion
from backend.app.services.websocket_manager import ws_manager
from backend.app.services.live_stats import live_stats
from backend.app.services.detection_service import detection_service
from backend.app.services.message_ingestion_service import message_ingestion_service, TelegramMessageData
from backend.app.services.user_management_service import user_management_service, TelegramUserData
from backend.app.core.logging_config import get_logger

logger = get_logger("backfill")

BATCH_SIZE = 200
MAX_CONCURRENT_PER_ACCOUNT = 2


class BackfillService:
    def __init__(self, telegram_manager):
        self.manager = telegram_manager
        self._backfill_tasks: dict[int, tuple[asyncio.Task, asyncio.Event]] = {}
        self._account_semaphores: dict[int, asyncio.Semaphore] = {}
    
    def _get_account_semaphore(self, account_id: int) -> asyncio.Semaphore:
        if account_id not in self._account_semaphores:
            self._account_semaphores[account_id] = asyncio.Semaphore(MAX_CONCURRENT_PER_ACCOUNT)
        return self._account_semaphores[account_id]
    
    async def start_all_pending_backfills(self):
        logger.info("Starting all pending backfills...")
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramGroup).where(
                    TelegramGroup.backfill_done == False,
                    TelegramGroup.assigned_account_id != None
                )
            )
            groups = result.scalars().all()
        
        started = 0
        for group in groups:
            if group.id not in self._backfill_tasks:
                await self.start_backfill(
                    account_id=group.assigned_account_id,
                    channel_id=group.id,
                    telegram_id=group.telegram_id
                )
                started += 1
        
        logger.info(f"Started {started} backfills for pending groups")
        return started
    
    async def start_backfill(
        self,
        account_id: int,
        channel_id: int,
        telegram_id: int,
        mode: str = "full",
        since_date: Optional[datetime] = None,
        on_progress: Optional[Callable[[int, int], Any]] = None
    ):
        if channel_id in self._backfill_tasks:
            logger.info(f"Backfill already running for channel {channel_id}")
            return self._backfill_tasks[channel_id][0]
        
        await user_enricher.start_worker()
        await media_ingestion.start_workers()
        
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            self._run_backfill_with_semaphore(
                account_id=account_id,
                channel_id=channel_id,
                telegram_id=telegram_id,
                mode=mode,
                since_date=since_date,
                stop_event=stop_event,
                on_progress=on_progress
            ),
            name=f"backfill:{channel_id}"
        )
        
        self._backfill_tasks[channel_id] = (task, stop_event)
        return task
    
    async def _run_backfill_with_semaphore(
        self,
        account_id: int,
        channel_id: int,
        telegram_id: int,
        mode: str,
        since_date: Optional[datetime],
        stop_event: asyncio.Event,
        on_progress: Optional[Callable[[int, int], Any]]
    ):
        semaphore = self._get_account_semaphore(account_id)
        async with semaphore:
            return await self._run_backfill(
                account_id, channel_id, telegram_id, mode, 
                since_date, stop_event, on_progress
            )
    
    async def stop_backfill(self, channel_id: int):
        if channel_id not in self._backfill_tasks:
            return
        
        task, stop_event = self._backfill_tasks.pop(channel_id)
        stop_event.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    
    async def _run_backfill(
        self,
        account_id: int,
        channel_id: int,
        telegram_id: int,
        mode: str,
        since_date: Optional[datetime],
        stop_event: asyncio.Event,
        on_progress: Optional[Callable[[int, int], Any]]
    ):
        await self._set_in_progress(channel_id, True)
        
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramGroup).where(TelegramGroup.id == channel_id)
            )
            group = result.scalar_one_or_none()
            if not group:
                await self._set_in_progress(channel_id, False)
                return 0
        
        processed_total = 0
        last_processed_id: Optional[int] = None
        consecutive_empty_batches = 0
        start_time = datetime.utcnow()
        
        try:
            client = await self._ensure_client(account_id)
            if not client:
                logger.error(f"Failed to connect client for account {account_id}")
                await self._set_in_progress(channel_id, False)
                return 0
            
            if not await client.is_user_authorized():
                logger.error(f"Client for account {account_id} not authorized")
                await self._set_in_progress(channel_id, False)
                return 0
            
            logger.info(f"Starting FAST backfill for channel {channel_id} (telegram_id={telegram_id})")
            
            await ws_manager.broadcast("tasks", {
                "type": "backfill_started",
                "group_id": channel_id,
                "telegram_id": telegram_id
            })
            
            offset_id = 0
            
            while not stop_event.is_set():
                batch_messages = []
                batch_min_id: Optional[int] = None
                
                kwargs = {"limit": BATCH_SIZE, "offset_id": offset_id}
                
                try:
                    async for msg in client.iter_messages(telegram_id, **kwargs):
                        if stop_event.is_set():
                            break
                        if not msg:
                            continue
                        
                        if since_date and msg.date:
                            msg_date = msg.date.replace(tzinfo=None) if msg.date.tzinfo else msg.date
                            if msg_date < since_date:
                                stop_event.set()
                                break
                        
                        batch_messages.append(msg)
                        batch_min_id = int(msg.id)
                    
                    if batch_messages:
                        inserted = await self._batch_ingest_messages(
                            account_id, channel_id, batch_messages, client
                        )
                        processed_total += inserted
                        last_processed_id = batch_min_id
                        
                        await self._checkpoint(channel_id, last_processed_id, processed_total)
                        
                        elapsed = (datetime.utcnow() - start_time).total_seconds()
                        rate = processed_total / elapsed if elapsed > 0 else 0
                        
                        logger.info(f"Channel {channel_id}: {inserted} msgs in batch, total {processed_total} ({rate:.1f}/s)")
                        
                        await ws_manager.broadcast("tasks", {
                            "type": "backfill_progress",
                            "group_id": channel_id,
                            "processed": processed_total,
                            "rate": round(rate, 1)
                        })
                        
                        if on_progress:
                            try:
                                on_progress(processed_total, -1)
                            except Exception:
                                pass
                
                except FloodWaitError as e:
                    logger.warning(f"FloodWait during backfill: sleep {e.seconds}s")
                    await asyncio.sleep(e.seconds + 1)
                    continue
                except Exception as e:
                    logger.exception(f"Error during backfill: {e}")
                    break
                
                if stop_event.is_set():
                    break
                
                if len(batch_messages) == 0:
                    consecutive_empty_batches += 1
                    if consecutive_empty_batches >= 2 or batch_min_id is None:
                        consecutive_empty_batches = 2
                        logger.info(f"Backfill completed for channel {channel_id}! Total: {processed_total}")
                        break
                else:
                    consecutive_empty_batches = 0
                
                if batch_min_id is not None:
                    offset_id = batch_min_id
                else:
                    consecutive_empty_batches = 2
                    break
                
                await asyncio.sleep(0.1)
            
            done = not stop_event.is_set() and consecutive_empty_batches >= 2
            await self._finalize_backfill(channel_id, last_processed_id, done, processed_total)
            
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            rate = processed_total / elapsed if elapsed > 0 else 0
            
            await ws_manager.broadcast("tasks", {
                "type": "backfill_completed",
                "group_id": channel_id,
                "total_messages": processed_total,
                "done": done,
                "elapsed_seconds": round(elapsed, 1),
                "rate": round(rate, 1)
            })
            
            logger.info(f"Backfill done for {channel_id}: {processed_total} msgs in {elapsed:.1f}s ({rate:.1f}/s)")
            
            if done:
                await asyncio.sleep(2)
                await self._auto_scrape_members(client, channel_id, account_id)
        
        except Exception as e:
            logger.exception(f"Backfill failed for channel {channel_id}: {e}")
        
        finally:
            await self._set_in_progress(channel_id, False)
            if channel_id in self._backfill_tasks:
                del self._backfill_tasks[channel_id]
        
        return processed_total
    
    async def _ensure_client(self, account_id: int):
        client = self.manager.clients.get(account_id)
        if client and await client.is_user_authorized():
            return client
        
        logger.info(f"Client not connected for account {account_id}, attempting to connect...")
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
    
    async def _batch_ingest_messages(
        self, 
        account_id: int, 
        channel_id: int, 
        messages: list, 
        client
    ) -> int:
        """
        Enhanced batch message ingestion using the new Message Ingestion Service.
        
        This method now uses proper UPSERT operations to handle duplicate messages
        and constraint violations gracefully.
        """
        if not messages:
            return 0
        
        try:
            # Prepare message data using the enhanced service
            message_data_list = []
            user_data_list = []
            media_queue = []
            
            # First pass: collect unique users and prepare message data
            seen_users = set()
            
            for msg in messages:
                # Handle user data if sender exists
                # Skip if sender_id is negative (it's a channel/group, not a user)
                if msg.sender_id and msg.sender_id > 0 and msg.sender_id not in seen_users:
                    try:
                        sender = await msg.get_sender()
                        if sender:
                            user_data = TelegramUserData(
                                telegram_id=msg.sender_id,
                                username=getattr(sender, 'username', None),
                                first_name=getattr(sender, 'first_name', None),
                                last_name=getattr(sender, 'last_name', None),
                                phone=getattr(sender, 'phone', None),
                                access_hash=getattr(sender, 'access_hash', None),
                                is_bot=getattr(sender, 'bot', False),
                                is_premium=getattr(sender, 'premium', False),
                                is_verified=getattr(sender, 'verified', False),
                                is_scam=getattr(sender, 'scam', False),
                                is_fake=getattr(sender, 'fake', False),
                                is_restricted=getattr(sender, 'restricted', False),
                                is_deleted=getattr(sender, 'deleted', False)
                            )
                            user_data_list.append(user_data)
                            seen_users.add(msg.sender_id)
                    except Exception as e:
                        logger.warning(f"Could not get sender for message {msg.id}: {e}")
                
                # Create message data using the enhanced service
                message_data = message_ingestion_service.create_message_data_from_entity(
                    msg, channel_id, None  # sender_id will be resolved after user creation
                )
                message_data_list.append(message_data)
                
                # Queue media for later processing
                if msg.media:
                    media_queue.append(msg)
            
            # Batch upsert users first
            user_results = await user_management_service.batch_upsert_users(user_data_list)
            logger.info(f"User batch upsert: {user_results.success_count} successful, "
                       f"{user_results.failure_count} failed")
            
            # Create telegram_id to database_id mapping for users
            user_id_map = {}
            for user in user_results.successful_users:
                user_id_map[user.telegram_id] = user.id
            
            # Update message data with correct sender_ids
            updated_message_data = []
            for i, message_data in enumerate(message_data_list):
                original_msg = messages[i]
                sender_db_id = user_id_map.get(original_msg.sender_id) if original_msg.sender_id else None
                
                # Create new message data with correct sender_id
                updated_data = TelegramMessageData(
                    telegram_id=message_data.telegram_id,
                    group_id=message_data.group_id,
                    sender_id=sender_db_id,
                    text=message_data.text,
                    message_type=message_data.message_type,
                    date=message_data.date,
                    edit_date=message_data.edit_date,
                    reply_to_msg_id=message_data.reply_to_msg_id,
                    reply_preview=message_data.reply_preview,
                    forward_from_id=message_data.forward_from_id,
                    forward_from_name=message_data.forward_from_name,
                    forward_date=message_data.forward_date,
                    views=message_data.views,
                    forwards=message_data.forwards,
                    mentions=message_data.mentions,
                    reactions=message_data.reactions,
                    is_pinned=message_data.is_pinned,
                    is_deleted=message_data.is_deleted,
                    grouped_id=message_data.grouped_id,
                    content_hash=message_data.content_hash
                )
                updated_message_data.append(updated_data)
            
            # Batch insert messages using the enhanced service
            message_results = await message_ingestion_service.batch_insert_messages(updated_message_data)
            
            inserted_count = message_results.success_count
            logger.info(f"Message batch insert: {inserted_count} successful, "
                       f"{message_results.failure_count} failed, "
                       f"{message_results.duplicate_count} duplicates")
            
            # Update group message count
            if inserted_count > 0:
                async with async_session_maker() as db:
                    await db.execute(
                        update(TelegramGroup).where(TelegramGroup.id == channel_id).values(
                            messages_count=TelegramGroup.messages_count + inserted_count
                        )
                    )
                    await db.commit()
            
            # Record statistics
            live_stats.record("backfill_messages", inserted_count)
            live_stats.record("messages_saved", inserted_count)
            
            # Process media and run detections for successful messages
            if message_results.successful_messages:
                await self._process_message_media_and_detections(
                    message_results.successful_messages, 
                    media_queue, 
                    client, 
                    channel_id
                )
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error in enhanced batch message ingestion: {e}")
            # Fallback to basic insertion count
            return 0
    
    async def _process_message_media_and_detections(self, successful_messages, media_queue, client, channel_id):
        """Process media downloads and run detections for successfully inserted messages"""
        try:
            # Create mapping of telegram_id to database message
            telegram_id_to_msg = {msg.telegram_id: msg for msg in successful_messages}
            
            # Queue media downloads
            for telegram_msg in media_queue:
                db_msg = telegram_id_to_msg.get(telegram_msg.id)
                if db_msg:
                    await media_ingestion.queue_download(client, db_msg.id, telegram_msg, channel_id)
                    live_stats.record("media_queued")
                
                # Queue user enrichment
                if telegram_msg.sender_id:
                    await user_enricher.queue_enrichment(client, telegram_msg.sender_id, channel_id, source="backfill")
            
            # Run detections on messages with text
            async with async_session_maker() as db:
                for msg in successful_messages:
                    if msg.text and len(msg.text) > 3:
                        try:
                            await detection_service.process_message(
                                message_id=msg.id,
                                text=msg.text,
                                group_id=channel_id,
                                db=db
                            )
                        except Exception as e:
                            logger.warning(f"Detection failed for message {msg.id}: {e}")
                
                await db.commit()
                
        except Exception as e:
            logger.error(f"Error processing media and detections: {e}")
    
    async def _checkpoint(self, channel_id: int, msg_id: Optional[int], processed: int = 0):
        if not msg_id:
            return
        async with async_session_maker() as db:
            await db.execute(
                update(TelegramGroup).where(TelegramGroup.id == channel_id).values(
                    last_backfill_message_id=msg_id
                )
            )
            await db.commit()
    
    async def _finalize_backfill(self, channel_id: int, last_id: Optional[int], done: bool, total: int):
        async with async_session_maker() as db:
            count_result = await db.execute(
                select(func.count()).select_from(TelegramMessage).where(
                    TelegramMessage.group_id == channel_id
                )
            )
            actual_count = count_result.scalar() or 0
            
            values = {
                "backfill_done": done,
                "messages_count": actual_count
            }
            if last_id:
                values["last_backfill_message_id"] = last_id
            
            await db.execute(
                update(TelegramGroup).where(TelegramGroup.id == channel_id).values(**values)
            )
            await db.commit()
            logger.info(f"Finalized channel {channel_id}: done={done}, messages={actual_count}")
    
    async def _set_in_progress(self, channel_id: int, val: bool):
        async with async_session_maker() as db:
            await db.execute(
                update(TelegramGroup).where(TelegramGroup.id == channel_id).values(
                    backfill_in_progress=val
                )
            )
            await db.commit()
    
    def get_active_backfills(self) -> list[int]:
        return list(self._backfill_tasks.keys())
    
    def is_backfilling(self, channel_id: int) -> bool:
        return channel_id in self._backfill_tasks
    
    def get_status(self) -> dict:
        return {
            "active_backfills": len(self._backfill_tasks),
            "channels": list(self._backfill_tasks.keys())
        }
    
    async def _auto_scrape_members(self, client, channel_id: int, account_id: int):
        from backend.app.services.member_scraper import member_scraper
        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(TelegramGroup).where(TelegramGroup.id == channel_id)
                )
                group = result.scalar_one_or_none()
                
                if not group:
                    return
                
                if group.group_type == "channel":
                    logger.info(f"Skipping member scrape for channel {channel_id} (channels don't have members list)")
                    return
                
                logger.info(f"Auto-starting member scrape for group {channel_id}")
                
                await ws_manager.broadcast("tasks", {
                    "type": "member_scrape_started",
                    "group_id": channel_id,
                    "group_title": group.title
                })
                
                stats = await member_scraper.scrape_group_members(
                    client=client,
                    group=group,
                    db=db,
                    account_id=account_id
                )
                
                from datetime import datetime
                group.last_member_scrape_at = datetime.utcnow()
                await db.commit()
                
                await ws_manager.broadcast("tasks", {
                    "type": "member_scrape_completed",
                    "group_id": channel_id,
                    "stats": stats
                })
                
                logger.info(f"Member scrape completed for group {channel_id}: {stats}")
                
        except Exception as e:
            logger.error(f"Auto member scrape failed for group {channel_id}: {e}")
    
