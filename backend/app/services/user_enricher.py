import asyncio
import os
import hashlib
from datetime import datetime
from typing import Optional
from telethon import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.stories import GetPeerStoriesRequest
from telethon.tl.functions.photos import GetUserPhotosRequest
from telethon.tl.types import User, UserFull, InputPeerUser, Photo, InputUser
from telethon.errors import FloodWaitError, UserNotParticipantError
from telethon.errors import UserPrivacyRestrictedError, PeerIdInvalidError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from backend.app.models.telegram_user import TelegramUser
from backend.app.models.history import UserProfilePhoto, UserProfileHistory
from backend.app.models.membership import GroupMembership
from backend.app.db.database import async_session_maker
from backend.app.services.live_stats import live_stats
from backend.app.core.logging_config import get_logger

logger = get_logger("user_enricher")


class UserEnricherService:
    def __init__(self, media_dir: str = "media"):
        self.media_dir = media_dir
        self.profile_photos_dir = os.path.join(media_dir, "profile_photos")
        os.makedirs(self.profile_photos_dir, exist_ok=True)
        self._enrichment_queue: asyncio.Queue = asyncio.Queue()
        self._enrichment_task: Optional[asyncio.Task] = None
        self._processed_users: set[int] = set()
        self._in_progress_users: set[int] = set()  # Track users currently being enriched
        self._semaphore = asyncio.Semaphore(2)
        self._stats = {
            "users_queued": 0,
            "users_enriched": 0,
            "users_failed": 0,
            "photos_downloaded": 0,
            "last_idle_log": None
        }
        self.logger = logger
    
    def get_status(self) -> dict:
        is_running = self._enrichment_task is not None and not self._enrichment_task.done()
        return {
            "running": is_running,
            "queue_size": self._enrichment_queue.qsize(),
            "processed_users": len(self._processed_users),
            "in_progress_users": len(self._in_progress_users),
            "stats": self._stats.copy()
        }
    
    async def start_worker(self):
        if self._enrichment_task is None or self._enrichment_task.done():
            self._enrichment_task = asyncio.create_task(self._enrichment_worker())
            self.logger.info("[UserEnricher] Worker started successfully")
    
    async def stop_worker(self):
        if self._enrichment_task:
            self._enrichment_task.cancel()
            try:
                await self._enrichment_task
            except asyncio.CancelledError:
                pass
            self.logger.info("[UserEnricher] Worker stopped")
    
    async def queue_enrichment(self, client: TelegramClient, telegram_id: int, group_id: Optional[int] = None, source: str = "unknown"):
        # Skip if telegram_id is negative (it's a channel/group, not a user)
        if telegram_id < 0:
            return
            
        if telegram_id in self._processed_users:
            return
        
        queue_size = self._enrichment_queue.qsize()
        await self._enrichment_queue.put((client, telegram_id, group_id, source))
        self._stats["users_queued"] += 1
        
        self.logger.debug(f"[UserEnricher] Queued user {telegram_id} from {source}, queue size: {queue_size + 1}")
    
    async def _enrichment_worker(self):
        self.logger.info("[UserEnricher] Worker loop started")
        idle_log_interval = 60  # Log idle state every 60 seconds
        last_idle_log = datetime.utcnow()
        processed_count = 0
        
        while True:
            try:
                # Check if queue is empty and log idle state
                if self._enrichment_queue.empty():
                    now = datetime.utcnow()
                    if (now - last_idle_log).total_seconds() >= idle_log_interval:
                        self.logger.debug(f"[UserEnricher] Worker idle, queue empty. Processed: {len(self._processed_users)}")
                        last_idle_log = now
                
                client, telegram_id, group_id, source = await self._enrichment_queue.get()
                
                if telegram_id not in self._processed_users:
                    # Check for duplicate in-progress requests
                    if telegram_id in self._in_progress_users:
                        self.logger.warning(f"[UserEnricher] Skipping duplicate request for user {telegram_id}")
                        self._enrichment_queue.task_done()
                        continue
                    
                    async with self._semaphore:
                        self._in_progress_users.add(telegram_id)
                        start_time = datetime.utcnow()
                        
                        try:
                            self.logger.info(f"[UserEnricher] Starting enrichment for user {telegram_id} from {source}")
                            await self.enrich_user(client, telegram_id, group_id)
                            self._processed_users.add(telegram_id)
                            
                            duration = (datetime.utcnow() - start_time).total_seconds()
                            self._stats["users_enriched"] += 1
                            processed_count += 1
                            
                            self.logger.info(f"[UserEnricher] Completed enrichment for user {telegram_id} in {duration:.2f}s")
                            
                            # Log queue size every 10 processed users
                            if processed_count % 10 == 0:
                                self.logger.info(f"[UserEnricher] Progress: {processed_count} users enriched, queue size: {self._enrichment_queue.qsize()}")
                            
                        except FloodWaitError as e:
                            self.logger.warning(f"[UserEnricher] FloodWait: {e.seconds}s, requeuing user {telegram_id}")
                            await asyncio.sleep(e.seconds + 1)
                            await self._enrichment_queue.put((client, telegram_id, group_id, source))
                        except Exception as e:
                            self._stats["users_failed"] += 1
                            self.logger.error(f"[UserEnricher] Error enriching user {telegram_id}: {type(e).__name__}: {e}")
                        finally:
                            self._in_progress_users.discard(telegram_id)
                
                self._enrichment_queue.task_done()
            except asyncio.CancelledError:
                self.logger.info("[UserEnricher] Worker cancelled, shutting down")
                break
            except Exception as e:
                self.logger.error(f"[UserEnricher] Worker error: {e}")
                await asyncio.sleep(1)
    
    async def enrich_user(
        self, 
        client: TelegramClient, 
        telegram_id: int, 
        group_id: Optional[int] = None,
        retry_count: int = 0
    ) -> Optional[TelegramUser]:
        MAX_RETRY_ATTEMPTS = 3
        RETRY_BASE_DELAY = 1  # seconds
        async with async_session_maker() as db:
            # Use user_management_service for proper UPSERT
            from backend.app.services.user_management_service import user_management_service, TelegramUserData
            
            result = await db.execute(
                select(TelegramUser).where(TelegramUser.telegram_id == telegram_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                # Use UPSERT instead of db.add to prevent UniqueViolationError
                user_data = TelegramUserData(telegram_id=telegram_id)
                user = await user_management_service.upsert_user(user_data)
                if not user:
                    return None
                # Refresh user from database to get the ID
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.telegram_id == telegram_id)
                )
                user = result.scalar_one_or_none()
            
            # Check if user is deleted before enrichment
            if user.is_deleted:
                self.logger.warning(f"[UserEnricher] Skipping enrichment for deleted user {telegram_id}")
                return user
            
            # Check if user has access_hash (required for enrichment)
            if not user.access_hash:
                self.logger.warning(f"[UserEnricher] User {telegram_id} missing access_hash, attempting to get entity")
                try:
                    # Try to get the entity to obtain access_hash
                    entity = await client.get_entity(telegram_id)
                    if hasattr(entity, 'access_hash') and entity.access_hash:
                        user.access_hash = entity.access_hash
                        await db.commit()
                    else:
                        self.logger.warning(f"[UserEnricher] Cannot enrich user {telegram_id} without access_hash")
                        return user
                except Exception as e:
                    self.logger.warning(f"[UserEnricher] Failed to get entity for user {telegram_id}: {e}")
                    return user
            
            try:
                input_user = await client.get_input_entity(telegram_id)
                full_user_result = await client(GetFullUserRequest(input_user))
                
                if full_user_result:
                    full_user: UserFull = full_user_result.full_user
                    tg_user: User = full_user_result.users[0] if full_user_result.users else None
                    
                    if tg_user:
                        await self._update_user_from_entity(db, user, tg_user, full_user)
                    
                    if full_user.about and full_user.about != user.bio:
                        if user.bio is not None:
                            change = UserProfileHistory(
                                user_id=user.id,
                                field_changed="bio",
                                old_value=user.bio,
                                new_value=full_user.about
                            )
                            db.add(change)
                        user.bio = full_user.about
                    
                    if tg_user:
                        await self.sync_all_profile_photos(client, db, user, tg_user)
                    
                    has_stories = await self._check_stories(client, telegram_id)
                    user.has_stories = has_stories
                    
                    await db.commit()
                    live_stats.record("users_enriched")
                    print(f"[UserEnricher] Enriched user {telegram_id}: {user.username or user.first_name}, bio={bool(user.bio)}, has_stories={has_stories}")
                    
                    if has_stories and user.is_watchlist:
                        asyncio.create_task(self._download_stories_for_user(client, user))
            
            except UserNotParticipantError:
                self.logger.warning(f"[UserEnricher] User {telegram_id} not a participant")
                pass
            except PeerIdInvalidError:
                self.logger.warning(f"[UserEnricher] Invalid peer ID for user {telegram_id}")
                pass
            except UserPrivacyRestrictedError:
                self.logger.warning(f"[UserEnricher] Privacy restricted for user {telegram_id}")
                # Mark user as privacy restricted if field exists
                try:
                    user.is_restricted = True
                    await db.commit()
                except Exception:
                    pass
                pass
            except FloodWaitError as e:
                # FloodWaitError should be re-raised to be handled by worker
                self.logger.warning(f"[UserEnricher] FloodWait {e.seconds}s for user {telegram_id}")
                raise
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                # Network errors - retry with exponential backoff
                if retry_count < MAX_RETRY_ATTEMPTS:
                    delay = RETRY_BASE_DELAY * (2 ** retry_count)
                    self.logger.warning(f"[UserEnricher] Network error for user {telegram_id}, retry {retry_count + 1}/{MAX_RETRY_ATTEMPTS} after {delay}s: {e}")
                    await asyncio.sleep(delay)
                    return await self.enrich_user(client, telegram_id, group_id, retry_count + 1)
                else:
                    self.logger.error(f"[UserEnricher] Max retries reached for user {telegram_id}: {e}")
                    raise
            except Exception as e:
                print(f"[UserEnricher] Error getting full user {telegram_id}: {e}")
            
            if group_id:
                await self._ensure_membership(db, user.id, group_id)
            
            await db.commit()
            return user
    
    async def _update_user_from_entity(
        self, 
        db: AsyncSession, 
        user: TelegramUser, 
        tg_user: User,
        full_user: UserFull
    ):
        changes = []
        
        new_username = getattr(tg_user, 'username', None)
        if new_username != user.username:
            if user.username is not None:
                changes.append(("username", user.username, new_username))
            user.username = new_username
        
        new_first = getattr(tg_user, 'first_name', None)
        if new_first != user.first_name:
            if user.first_name is not None:
                changes.append(("first_name", user.first_name, new_first))
            user.first_name = new_first
        
        new_last = getattr(tg_user, 'last_name', None)
        if new_last != user.last_name:
            if user.last_name is not None:
                changes.append(("last_name", user.last_name, new_last))
            user.last_name = new_last
        
        new_phone = getattr(tg_user, 'phone', None)
        if new_phone != user.phone:
            if user.phone is not None:
                changes.append(("phone", user.phone, new_phone))
            user.phone = new_phone
        
        user.access_hash = getattr(tg_user, 'access_hash', None)
        user.is_premium = getattr(tg_user, 'premium', False) or False
        user.is_verified = getattr(tg_user, 'verified', False) or False
        user.is_bot = getattr(tg_user, 'bot', False) or False
        user.is_scam = getattr(tg_user, 'scam', False) or False
        user.is_fake = getattr(tg_user, 'fake', False) or False
        user.is_restricted = getattr(tg_user, 'restricted', False) or False
        user.is_deleted = getattr(tg_user, 'deleted', False) or False
        
        for field, old_val, new_val in changes:
            change = UserProfileHistory(
                user_id=user.id,
                field_changed=field,
                old_value=old_val,
                new_value=new_val
            )
            db.add(change)
    
    async def sync_all_profile_photos(
        self, 
        client: TelegramClient, 
        db: AsyncSession, 
        user: TelegramUser, 
        tg_user: User
    ) -> int:
        downloaded_count = 0
        try:
            input_user = InputUser(
                user_id=user.telegram_id,
                access_hash=user.access_hash or 0
            )
            
            all_photos = []
            offset = 0
            batch_limit = 100
            
            while True:
                photos_result = await client(GetUserPhotosRequest(
                    user_id=input_user,
                    offset=offset,
                    max_id=0,
                    limit=batch_limit
                ))
                
                if not photos_result or not photos_result.photos:
                    break
                    
                all_photos.extend(photos_result.photos)
                
                if len(photos_result.photos) < batch_limit:
                    break
                    
                offset += len(photos_result.photos)
            
            if not all_photos:
                if tg_user.photo:
                    await self._download_current_photo(client, db, user, tg_user)
                return 0
            
            user_dir = os.path.join(self.profile_photos_dir, str(user.telegram_id))
            os.makedirs(user_dir, exist_ok=True)
            
            existing_result = await db.execute(
                select(UserProfilePhoto.telegram_photo_id).where(
                    UserProfilePhoto.user_id == user.id,
                    UserProfilePhoto.telegram_photo_id.isnot(None)
                )
            )
            existing_photo_ids = {row[0] for row in existing_result.all()}
            
            current_photo_id = tg_user.photo.photo_id if tg_user.photo and hasattr(tg_user.photo, 'photo_id') else None
            
            await db.execute(
                update(UserProfilePhoto).where(
                    UserProfilePhoto.user_id == user.id,
                    UserProfilePhoto.is_current == True
                ).values(is_current=False)
            )
            
            for idx, photo in enumerate(all_photos):
                if not isinstance(photo, Photo):
                    continue
                
                telegram_photo_id = photo.id
                
                if telegram_photo_id in existing_photo_ids:
                    if telegram_photo_id == current_photo_id:
                        await db.execute(
                            update(UserProfilePhoto).where(
                                UserProfilePhoto.user_id == user.id,
                                UserProfilePhoto.telegram_photo_id == telegram_photo_id
                            ).values(is_current=True)
                        )
                    continue
                
                is_video = photo.video_sizes is not None and len(photo.video_sizes) > 0
                captured_at = None
                if hasattr(photo, 'date') and photo.date:
                    captured_at = photo.date.replace(tzinfo=None) if photo.date.tzinfo else photo.date
                
                if is_video and photo.video_sizes:
                    ext = "mp4"
                    filename = f"{telegram_photo_id}.{ext}"
                    file_path = os.path.join(user_dir, filename)
                    
                    try:
                        await client.download_media(photo, file=file_path)
                    except Exception as e:
                        print(f"[UserEnricher] Error downloading video profile: {e}")
                        ext = "jpg"
                        filename = f"{telegram_photo_id}.{ext}"
                        file_path = os.path.join(user_dir, filename)
                        await client.download_media(photo, file=file_path, thumb=-1)
                else:
                    ext = "jpg"
                    filename = f"{telegram_photo_id}.{ext}"
                    file_path = os.path.join(user_dir, filename)
                    await client.download_media(photo, file=file_path, thumb=-1)
                
                if os.path.exists(file_path):
                    file_hash = None
                    try:
                        with open(file_path, 'rb') as f:
                            file_hash = hashlib.sha256(f.read()).hexdigest()
                    except:
                        pass
                    
                    is_current = (telegram_photo_id == current_photo_id) or (idx == 0 and current_photo_id is None)
                    
                    profile_photo = UserProfilePhoto(
                        user_id=user.id,
                        photo_id=str(telegram_photo_id),
                        telegram_photo_id=telegram_photo_id,
                        file_path=file_path,
                        file_hash=file_hash,
                        is_current=is_current,
                        is_video=is_video,
                        captured_at=captured_at
                    )
                    db.add(profile_photo)
                    downloaded_count += 1
                    
                    if is_current:
                        user.current_photo_path = file_path
            
            if downloaded_count > 0:
                print(f"[UserEnricher] Downloaded {downloaded_count} profile photos for user {user.telegram_id}")
            
            result = await db.execute(
                select(UserProfilePhoto).where(UserProfilePhoto.user_id == user.id)
            )
            user.photos_count = len(result.all())
            
        except FloodWaitError:
            raise
        except Exception as e:
            print(f"[UserEnricher] Error syncing profile photos: {e}")
            if tg_user.photo:
                await self._download_current_photo(client, db, user, tg_user)
        
        return downloaded_count
    
    async def _download_current_photo(
        self, 
        client: TelegramClient, 
        db: AsyncSession, 
        user: TelegramUser, 
        tg_user: User
    ):
        try:
            photo = tg_user.photo
            if not photo:
                return
            
            photo_id = str(photo.photo_id) if hasattr(photo, 'photo_id') else str(id(photo))
            
            existing = await db.execute(
                select(UserProfilePhoto).where(
                    UserProfilePhoto.user_id == user.id,
                    UserProfilePhoto.photo_id == photo_id
                )
            )
            if existing.scalar_one_or_none():
                return
            
            user_dir = os.path.join(self.profile_photos_dir, str(user.telegram_id))
            os.makedirs(user_dir, exist_ok=True)
            
            filename = f"{photo_id}.jpg"
            file_path = os.path.join(user_dir, filename)
            
            await client.download_profile_photo(tg_user, file=file_path)
            
            if os.path.exists(file_path):
                await db.execute(
                    update(UserProfilePhoto).where(
                        UserProfilePhoto.user_id == user.id,
                        UserProfilePhoto.is_current == True
                    ).values(is_current=False)
                )
                
                profile_photo = UserProfilePhoto(
                    user_id=user.id,
                    photo_id=photo_id,
                    file_path=file_path,
                    is_current=True
                )
                db.add(profile_photo)
                
                user.current_photo_path = file_path
                
                print(f"[UserEnricher] Downloaded current profile photo for user {user.telegram_id}")
        
        except Exception as e:
            print(f"[UserEnricher] Error downloading profile photo: {e}")
    
    async def _check_stories(self, client: TelegramClient, telegram_id: int) -> bool:
        try:
            input_user = await client.get_input_entity(telegram_id)
            stories = await client(GetPeerStoriesRequest(peer=input_user))
            return bool(stories and stories.stories and stories.stories.stories)
        except Exception:
            return False
    
    async def _ensure_membership(self, db: AsyncSession, user_id: int, group_id: int):
        result = await db.execute(
            select(GroupMembership).where(
                GroupMembership.user_id == user_id,
                GroupMembership.group_id == group_id
            )
        )
        membership = result.scalar_one_or_none()
        
        if not membership:
            membership = GroupMembership(
                user_id=user_id,
                group_id=group_id,
                is_active=True
            )
            db.add(membership)
            
            await db.execute(
                update(TelegramUser).where(TelegramUser.id == user_id).values(
                    groups_count=TelegramUser.groups_count + 1
                )
            )
    
    async def _download_stories_for_user(self, client: TelegramClient, user: TelegramUser):
        try:
            from backend.app.services.story_service import StoryService
            async with async_session_maker() as db:
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.id == user.id)
                )
                fresh_user = result.scalar_one_or_none()
                if fresh_user and fresh_user.has_stories:
                    story_service = StoryService(client, db)
                    stories = await story_service.download_user_stories(fresh_user)
                    if stories:
                        print(f"[UserEnricher] Downloaded {len(stories)} stories for user {user.telegram_id}")
        except Exception as e:
            print(f"[UserEnricher] Failed to download stories for user {user.telegram_id}: {e}")


user_enricher = UserEnricherService()
