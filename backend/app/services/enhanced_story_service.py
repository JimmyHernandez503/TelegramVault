import asyncio
import os
import hashlib
import logging
import aiofiles
import aiofiles.os
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
from telethon import TelegramClient
from telethon.tl.functions.stories import GetPeerStoriesRequest, GetStoriesByIDRequest
from telethon.tl.types import (
    InputPeerUser, StoryItemSkipped, StoryItemDeleted, StoryItem,
    MessageMediaPhoto, MessageMediaDocument, InputStoryID
)
from telethon.errors import FloodWaitError, StoryNotAvailableError, PrivacyRestrictedError, RPCError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, or_
from sqlalchemy.dialects.postgresql import insert

from backend.app.models.telegram_user import TelegramUser
from backend.app.models.history import UserStory
from backend.app.models.media import MediaFile
from backend.app.models.download_task import DownloadTask
from backend.app.db.database import async_session_maker
from backend.app.services.live_stats import live_stats
from backend.app.core.session_recovery_manager import SessionRecoveryManager
from backend.app.core.media_validator import MediaValidator, ValidationStatus
from backend.app.core.duplicate_detector import DuplicateDetector, SimilarityLevel
from backend.app.core.download_queue_manager import DownloadQueueManager, TaskPriority
from backend.app.core.file_system_manager import FileSystemManager
from backend.app.core.api_rate_limiter import APIRateLimiter, OperationType


class StoryAccessLevel(Enum):
    """Story access levels"""
    PUBLIC = "public"
    CONTACTS = "contacts"
    CLOSE_FRIENDS = "close_friends"
    PRIVATE = "private"
    RESTRICTED = "restricted"


class StoryDownloadStatus(Enum):
    """Story download status"""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    ACCESS_DENIED = "access_denied"
    NOT_AVAILABLE = "not_available"


@dataclass
class StoryDownloadResult:
    """Result of story download operation"""
    success: bool
    story_id: int
    file_path: Optional[str] = None
    file_hash: Optional[str] = None
    file_size: int = 0
    story_type: Optional[str] = None
    error_message: Optional[str] = None
    access_level: Optional[StoryAccessLevel] = None
    expires_at: Optional[datetime] = None
    validation_status: Optional[ValidationStatus] = None
    is_duplicate: bool = False


@dataclass
class StoryBatchResult:
    """Result of batch story processing"""
    total_stories: int
    successful_downloads: int
    failed_downloads: int
    expired_stories: int
    access_denied: int
    duplicates_detected: int
    errors: List[str]


class EnhancedStoryService:
    """
    Enhanced story service with comprehensive story download management.
    
    Features:
    - Story download within expiration windows
    - Privacy setting and access restriction handling
    - Multi-component story processing (photos, videos, documents)
    - Access denial logging and continuation
    - Integration with enhanced media components
    """
    
    def __init__(self, media_dir: str = "media"):
        self.logger = logging.getLogger(__name__)
        self.media_dir = Path(media_dir)
        self.stories_dir = self.media_dir / "stories"
        
        # Enhanced components
        self.session_recovery = SessionRecoveryManager()
        self.media_validator = MediaValidator()
        self.duplicate_detector = DuplicateDetector()
        self.file_system_manager = FileSystemManager(media_dir)
        self.rate_limiter = APIRateLimiter()
        
        # Queue management (optional - can be injected)
        self.queue_manager: Optional[DownloadQueueManager] = None
        
        # Story processing settings
        self._settings = {
            "max_retries": 3,
            "retry_delay_base": 2,  # seconds
            "exponential_backoff": True,
            "jitter_enabled": True,
            "validate_downloads": True,
            "detect_duplicates": True,
            "story_timeout": 45,  # seconds
            "batch_size": 10,
            "concurrent_downloads": 2,
            "expiration_buffer_minutes": 30,  # Download stories X minutes before expiration
            "access_retry_attempts": 2,
            "handle_privacy_restrictions": True,
            "log_access_denials": True,
            "continue_on_access_denial": True,
            "max_stories_per_user": 50
        }
        
        # Statistics tracking
        self._stats = {
            "stories_processed": 0,
            "stories_downloaded": 0,
            "stories_failed": 0,
            "stories_expired": 0,
            "access_denied_count": 0,
            "duplicates_detected": 0,
            "validation_failures": 0,
            "retries_performed": 0,
            "privacy_restrictions": 0
        }
        
        # Access denial tracking
        self._access_denied_users: set[int] = set()
        self._privacy_restricted_users: set[int] = set()
        
        self._initialized = False
    
    async def initialize(self) -> bool:
        """
        Initialize the enhanced story service.
        
        Returns:
            bool: True if initialization successful
        """
        try:
            self.logger.info("Initializing EnhancedStoryService")
            
            # Initialize file system manager
            if not await self.file_system_manager.initialize():
                self.logger.error("Failed to initialize file system manager")
                return False
            
            # Initialize session recovery manager
            if not await self.session_recovery.initialize():
                self.logger.error("Failed to initialize session recovery manager")
                return False
            
            # Create stories directory structure
            await self._create_directory_structure()
            
            self._initialized = True
            self.logger.info("EnhancedStoryService initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize EnhancedStoryService: {e}")
            return False
    
    async def _create_directory_structure(self) -> None:
        """Create the stories directory structure."""
        try:
            # Ensure stories directory exists
            await aiofiles.os.makedirs(self.stories_dir, exist_ok=True)
            
            # Create numbered subdirectories for load balancing
            for i in range(1, 51):  # Create 1-50 subdirectories
                subdir = self.stories_dir / str(i)
                await aiofiles.os.makedirs(subdir, exist_ok=True)
            
            self.logger.info("Stories directory structure created")
            
        except Exception as e:
            self.logger.error(f"Failed to create directory structure: {e}")
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current service status and statistics.
        
        Returns:
            Dict: Service status information
        """
        return {
            "initialized": self._initialized,
            "settings": self._settings.copy(),
            "statistics": self._stats.copy(),
            "access_tracking": {
                "access_denied_users": len(self._access_denied_users),
                "privacy_restricted_users": len(self._privacy_restricted_users)
            },
            "components": {
                "session_recovery": self.session_recovery.get_status(),
                "file_system": await self._get_file_system_status(),
                "rate_limiter": self.rate_limiter.get_rate_limit_status()._asdict()
            }
        }
    
    async def _get_file_system_status(self) -> Dict[str, Any]:
        """Get file system status information."""
        try:
            storage_info = await self.file_system_manager.get_storage_info()
            stories_stats = await self.file_system_manager.get_directory_stats('stories')
            
            return {
                "storage_status": storage_info.status.value,
                "storage_usage_percent": storage_info.usage_percent,
                "stories_count": stories_stats.file_count if stories_stats else 0,
                "stories_size_mb": round(stories_stats.total_size / (1024 * 1024), 2) if stories_stats else 0
            }
        except Exception as e:
            self.logger.error(f"Failed to get file system status: {e}")
            return {"error": str(e)}
    
    async def download_user_stories(self, client: TelegramClient, user: TelegramUser) -> List[Dict[str, Any]]:
        """
        Download all available stories for a user with enhanced features.
        
        Args:
            client: Telegram client
            user: User object
            
        Returns:
            List: Downloaded story information
        """
        if not self._initialized:
            if not await self.initialize():
                return []
        
        if not user.has_stories:
            return []
        
        # Check if user is in access denied list
        if user.telegram_id in self._access_denied_users:
            self.logger.debug(f"Skipping user {user.telegram_id} - previously access denied")
            return []
        
        try:
            # Check session health
            if not await self.session_recovery.is_session_healthy(client):
                self.logger.warning(f"Session unhealthy for user {user.telegram_id}, attempting recovery")
                if not await self.session_recovery.recover_session(client):
                    self.logger.error(f"Failed to recover session for user {user.telegram_id}")
                    return []
            
            # Get user stories
            stories = await self._get_user_stories(client, user)
            
            if not stories:
                return []
            
            # Process stories with enhanced features
            results = await self._process_stories_batch(client, user, stories)
            
            # Update statistics
            self._stats["stories_processed"] += results.total_stories
            self._stats["stories_downloaded"] += results.successful_downloads
            self._stats["stories_failed"] += results.failed_downloads
            self._stats["stories_expired"] += results.expired_stories
            self._stats["access_denied_count"] += results.access_denied
            self._stats["duplicates_detected"] += results.duplicates_detected
            
            # Log results
            if results.successful_downloads > 0:
                self.logger.info(f"Downloaded {results.successful_downloads}/{results.total_stories} stories for user {user.telegram_id}")
            
            if results.access_denied > 0:
                self.logger.warning(f"Access denied for {results.access_denied} stories from user {user.telegram_id}")
                if results.access_denied == results.total_stories:
                    self._access_denied_users.add(user.telegram_id)
            
            # Return story data
            return await self._get_downloaded_stories_data(user)
            
        except PrivacyRestrictedError:
            self.logger.warning(f"Privacy restricted for user {user.telegram_id}")
            self._privacy_restricted_users.add(user.telegram_id)
            self._stats["privacy_restrictions"] += 1
            return []
            
        except FloodWaitError as e:
            self.logger.warning(f"FloodWait for user {user.telegram_id}: {e.seconds}s")
            await self.rate_limiter.handle_flood_wait(e, OperationType.STORY_DOWNLOAD)
            return []
            
        except Exception as e:
            self.logger.error(f"Error downloading stories for user {user.telegram_id}: {e}")
            return []
    
    async def _get_user_stories(self, client: TelegramClient, user: TelegramUser) -> List[StoryItem]:
        """Get all stories for a user."""
        try:
            input_peer = InputPeerUser(user.telegram_id, user.access_hash or 0)
            
            # Apply rate limiting
            await self.rate_limiter.wait_if_needed(OperationType.STORY_DOWNLOAD)
            
            result = await client(GetPeerStoriesRequest(peer=input_peer))
            
            if not hasattr(result, 'stories') or not result.stories:
                return []
            
            peer_stories = result.stories
            if not hasattr(peer_stories, 'stories'):
                return []
            
            # Filter out skipped and deleted stories
            valid_stories = []
            for story in peer_stories.stories:
                if not isinstance(story, (StoryItemSkipped, StoryItemDeleted)):
                    valid_stories.append(story)
            
            return valid_stories[:self._settings["max_stories_per_user"]]
            
        except Exception as e:
            self.logger.error(f"Error getting stories for user {user.telegram_id}: {e}")
            return []
    
    async def _process_stories_batch(self, client: TelegramClient, user: TelegramUser, stories: List[StoryItem]) -> StoryBatchResult:
        """Process a batch of stories with enhanced features."""
        result = StoryBatchResult(
            total_stories=len(stories),
            successful_downloads=0,
            failed_downloads=0,
            expired_stories=0,
            access_denied=0,
            duplicates_detected=0,
            errors=[]
        )
        
        try:
            async with async_session_maker() as db:
                # Get existing story IDs to avoid duplicates
                existing_result = await db.execute(
                    select(UserStory.story_id).where(
                        UserStory.user_id == user.id
                    )
                )
                existing_story_ids = {row[0] for row in existing_result.all()}
                
                # Process stories in batches
                batch_size = self._settings["batch_size"]
                
                for i in range(0, len(stories), batch_size):
                    batch = stories[i:i + batch_size]
                    
                    # Process batch concurrently
                    tasks = []
                    for story in batch:
                        if story.id not in existing_story_ids:
                            task = asyncio.create_task(
                                self._download_single_story(client, db, user, story)
                            )
                            tasks.append(task)
                    
                    # Wait for batch completion
                    if tasks:
                        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for batch_result in batch_results:
                            if isinstance(batch_result, Exception):
                                result.failed_downloads += 1
                                result.errors.append(str(batch_result))
                            elif isinstance(batch_result, StoryDownloadResult):
                                if batch_result.success:
                                    result.successful_downloads += 1
                                    if batch_result.is_duplicate:
                                        result.duplicates_detected += 1
                                else:
                                    result.failed_downloads += 1
                                    if "access denied" in (batch_result.error_message or "").lower():
                                        result.access_denied += 1
                                    elif "expired" in (batch_result.error_message or "").lower():
                                        result.expired_stories += 1
                                    
                                    if batch_result.error_message:
                                        result.errors.append(batch_result.error_message)
                    
                    # Rate limiting between batches
                    if i + batch_size < len(stories):
                        await asyncio.sleep(0.5)
                
                await db.commit()
                
        except Exception as e:
            self.logger.error(f"Error processing stories batch for user {user.telegram_id}: {e}")
            result.errors.append(f"Batch processing error: {str(e)}")
        
        return result
    
    async def _download_single_story(self, client: TelegramClient, db: AsyncSession, user: TelegramUser, story: StoryItem) -> StoryDownloadResult:
        """Download a single story with enhanced features."""
        result = StoryDownloadResult(success=False, story_id=story.id)
        
        try:
            # Check if story is expired
            if hasattr(story, 'expire_date') and story.expire_date:
                expire_time = story.expire_date
                if expire_time.tzinfo:
                    expire_time = expire_time.replace(tzinfo=None)
                
                buffer_time = datetime.now() + timedelta(minutes=self._settings["expiration_buffer_minutes"])
                if expire_time <= buffer_time:
                    result.error_message = "Story expired or expiring soon"
                    return result
                
                result.expires_at = expire_time
            
            # Determine story type and access level
            story_type, access_level = await self._analyze_story(story)
            result.story_type = story_type
            result.access_level = access_level
            
            # Check if we have media to download
            if not hasattr(story, 'media') or not story.media:
                result.error_message = "No media in story"
                return result
            
            # Get user directory
            user_dir = await self._get_user_story_directory(user.telegram_id)
            
            # Download story media with retry logic
            download_result = await self._download_story_media_with_retry(
                client, story, user_dir
            )
            
            if not download_result["success"]:
                result.error_message = download_result.get("error", "Download failed")
                return result
            
            result.file_path = download_result["file_path"]
            result.file_hash = download_result.get("file_hash")
            result.file_size = download_result.get("file_size", 0)
            
            # Check for duplicates
            if self._settings["detect_duplicates"] and result.file_hash:
                is_duplicate = await self._check_story_duplicate(db, user.id, result.file_hash)
                if is_duplicate:
                    result.is_duplicate = True
                    # Delete duplicate file
                    if result.file_path:
                        await self.file_system_manager.delete_file(Path(result.file_path))
                    result.success = True  # Still consider it successful
                    return result
            
            # Validate file if enabled
            validation_status = None
            if self._settings["validate_downloads"] and result.file_path:
                validation_status = await self.media_validator.validate_file(Path(result.file_path))
                result.validation_status = validation_status
                
                if validation_status == ValidationStatus.CORRUPTED:
                    await self.file_system_manager.delete_file(Path(result.file_path))
                    result.error_message = "Downloaded file is corrupted"
                    return result
            
            # Extract story metadata
            story_data = await self._extract_story_metadata(story)
            
            # Create story record
            user_story = UserStory(
                user_id=user.id,
                story_id=story.id,
                story_type=story_type,
                file_path=result.file_path,
                caption=story_data.get("caption"),
                width=story_data.get("width"),
                height=story_data.get("height"),
                duration=story_data.get("duration"),
                views_count=story_data.get("views_count", 0),
                posted_at=story_data.get("posted_at"),
                expires_at=result.expires_at,
                is_pinned=story_data.get("is_pinned", False),
                is_public=story_data.get("is_public", True),
                file_size=result.file_size,
                file_hash=result.file_hash,
                validation_status=validation_status.value if validation_status else None
            )
            
            db.add(user_story)
            result.success = True
            
        except StoryNotAvailableError:
            result.error_message = "Story not available"
        except PrivacyRestrictedError:
            result.error_message = "Access denied - privacy restricted"
        except FloodWaitError as e:
            result.error_message = f"FloodWait: {e.seconds}s"
            await self.rate_limiter.handle_flood_wait(e, OperationType.STORY_DOWNLOAD)
        except Exception as e:
            result.error_message = f"Download error: {str(e)}"
            self.logger.error(f"Error downloading story {story.id} for user {user.telegram_id}: {e}")
        
        return result
    
    async def _analyze_story(self, story: StoryItem) -> Tuple[str, StoryAccessLevel]:
        """Analyze story to determine type and access level."""
        story_type = "unknown"
        access_level = StoryAccessLevel.PUBLIC
        
        try:
            # Determine story type from media
            if hasattr(story, 'media') and story.media:
                media = story.media
                
                if hasattr(media, 'video') and media.video:
                    story_type = "video"
                elif hasattr(media, 'photo') and media.photo:
                    story_type = "photo"
                elif hasattr(media, 'document') and media.document:
                    story_type = "document"
            
            # Determine access level
            if hasattr(story, 'public') and not story.public:
                if hasattr(story, 'close_friends') and story.close_friends:
                    access_level = StoryAccessLevel.CLOSE_FRIENDS
                elif hasattr(story, 'contacts') and story.contacts:
                    access_level = StoryAccessLevel.CONTACTS
                else:
                    access_level = StoryAccessLevel.PRIVATE
            
        except Exception as e:
            self.logger.warning(f"Error analyzing story {story.id}: {e}")
        
        return story_type, access_level
    
    async def _get_user_story_directory(self, telegram_id: int) -> Path:
        """Get optimal directory for user stories with load balancing."""
        # Use file system manager to get optimal subdirectory
        optimal_subdir = await self.file_system_manager.get_optimal_subdirectory('stories')
        
        if optimal_subdir:
            user_dir = optimal_subdir / str(telegram_id)
        else:
            # Fallback to direct stories directory
            user_dir = self.stories_dir / str(telegram_id)
        
        # Ensure directory exists
        await self.file_system_manager.ensure_file_path(user_dir / "dummy")
        
        return user_dir
    
    async def _download_story_media_with_retry(self, client: TelegramClient, story: StoryItem, user_dir: Path) -> Dict[str, Any]:
        """Download story media with retry logic."""
        result = {"success": False}
        
        for attempt in range(self._settings["max_retries"]):
            try:
                # Apply rate limiting
                await self.rate_limiter.wait_if_needed(OperationType.STORY_DOWNLOAD)
                
                # Determine file extension
                story_type = "photo"
                if hasattr(story, 'media') and story.media:
                    if hasattr(story.media, 'video'):
                        story_type = "video"
                    elif hasattr(story.media, 'document'):
                        story_type = "document"
                
                ext = "mp4" if story_type == "video" else "jpg"
                if story_type == "document" and hasattr(story.media, 'document'):
                    # Try to get extension from document
                    doc = story.media.document
                    if hasattr(doc, 'attributes'):
                        for attr in doc.attributes:
                            if hasattr(attr, 'file_name') and attr.file_name:
                                ext = Path(attr.file_name).suffix.lstrip('.') or ext
                                break
                
                filename = f"{story.id}.{ext}"
                file_path = user_dir / filename
                
                # Download with timeout
                download_task = asyncio.create_task(
                    client.download_media(story.media, file=str(file_path))
                )
                
                try:
                    await asyncio.wait_for(download_task, timeout=self._settings["story_timeout"])
                except asyncio.TimeoutError:
                    download_task.cancel()
                    if attempt < self._settings["max_retries"] - 1:
                        continue
                    else:
                        result["error"] = "Download timeout"
                        break
                
                # Verify file exists and has content
                if not await aiofiles.os.path.exists(file_path):
                    if attempt < self._settings["max_retries"] - 1:
                        continue
                    else:
                        result["error"] = "File not created"
                        break
                
                file_stat = await aiofiles.os.stat(file_path)
                if file_stat.st_size == 0:
                    await self.file_system_manager.delete_file(file_path)
                    if attempt < self._settings["max_retries"] - 1:
                        continue
                    else:
                        result["error"] = "Empty file downloaded"
                        break
                
                # Calculate file hash
                file_hash = None
                if self._settings["detect_duplicates"]:
                    try:
                        async with aiofiles.open(file_path, 'rb') as f:
                            content = await f.read()
                            file_hash = hashlib.sha256(content).hexdigest()
                    except Exception as e:
                        self.logger.warning(f"Failed to calculate hash for {file_path}: {e}")
                
                # Success
                result.update({
                    "success": True,
                    "file_path": str(file_path),
                    "file_hash": file_hash,
                    "file_size": file_stat.st_size
                })
                break
                
            except Exception as e:
                self.logger.warning(f"Story download attempt {attempt + 1} failed: {e}")
                if attempt < self._settings["max_retries"] - 1:
                    # Exponential backoff with jitter
                    delay = self._settings["retry_delay_base"] * (2 ** attempt)
                    if self._settings["jitter_enabled"]:
                        delay += random.uniform(0, delay * 0.1)
                    await asyncio.sleep(delay)
                    self._stats["retries_performed"] += 1
                else:
                    result["error"] = str(e)
        
        return result
    
    async def _extract_story_metadata(self, story: StoryItem) -> Dict[str, Any]:
        """Extract metadata from a story."""
        metadata = {}
        
        try:
            # Extract basic information
            if hasattr(story, 'caption'):
                metadata["caption"] = story.caption
            
            if hasattr(story, 'date') and story.date:
                posted_at = story.date
                if posted_at.tzinfo:
                    posted_at = posted_at.replace(tzinfo=None)
                metadata["posted_at"] = posted_at
            
            # Extract media dimensions and duration
            if hasattr(story, 'media') and story.media:
                media = story.media
                
                if hasattr(media, 'video') and media.video:
                    if hasattr(media, 'w'):
                        metadata["width"] = media.w
                    if hasattr(media, 'h'):
                        metadata["height"] = media.h
                    if hasattr(media, 'duration'):
                        metadata["duration"] = int(media.duration)
                        
                elif hasattr(media, 'photo') and media.photo:
                    if hasattr(media.photo, 'sizes') and media.photo.sizes:
                        largest = max(media.photo.sizes, key=lambda s: getattr(s, 'w', 0) * getattr(s, 'h', 0))
                        metadata["width"] = getattr(largest, 'w', None)
                        metadata["height"] = getattr(largest, 'h', None)
            
            # Extract view count
            if hasattr(story, 'views') and story.views:
                metadata["views_count"] = getattr(story.views, 'views_count', 0)
            
            # Extract flags
            metadata["is_pinned"] = getattr(story, 'pinned', False)
            metadata["is_public"] = getattr(story, 'public', True)
            
        except Exception as e:
            self.logger.warning(f"Error extracting story metadata: {e}")
        
        return metadata
    
    async def _check_story_duplicate(self, db: AsyncSession, user_id: int, file_hash: str) -> bool:
        """Check if a story is a duplicate based on hash."""
        try:
            result = await db.execute(
                select(UserStory).where(
                    UserStory.user_id == user_id,
                    UserStory.file_hash == file_hash
                )
            )
            return result.scalar_one_or_none() is not None
        except Exception:
            return False
    
    async def _get_downloaded_stories_data(self, user: TelegramUser) -> List[Dict[str, Any]]:
        """Get data for downloaded stories."""
        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(UserStory).where(
                        UserStory.user_id == user.id
                    ).order_by(UserStory.posted_at.desc())
                )
                stories = result.scalars().all()
                
                stories_data = []
                for story in stories:
                    stories_data.append({
                        "story_id": story.story_id,
                        "story_type": story.story_type,
                        "file_path": story.file_path,
                        "caption": story.caption,
                        "views_count": story.views_count,
                        "posted_at": story.posted_at.isoformat() if story.posted_at else None,
                        "expires_at": story.expires_at.isoformat() if story.expires_at else None,
                        "is_pinned": story.is_pinned,
                        "is_public": story.is_public,
                        "file_size": story.file_size,
                        "validation_status": story.validation_status
                    })
                
                return stories_data
                
        except Exception as e:
            self.logger.error(f"Error getting downloaded stories data for user {user.telegram_id}: {e}")
            return []
    
    async def batch_download_stories(self, client: TelegramClient, users: List[TelegramUser]) -> Dict[str, Any]:
        """
        Batch download stories for multiple users.
        
        Args:
            client: Telegram client
            users: List of users to process
            
        Returns:
            Dict: Batch processing results
        """
        results = {
            "total_users": len(users),
            "successful_users": 0,
            "failed_users": 0,
            "total_stories": 0,
            "downloaded_stories": 0,
            "errors": []
        }
        
        try:
            self.logger.info(f"Starting batch story download for {len(users)} users")
            
            # Process users in batches
            batch_size = self._settings["batch_size"]
            
            for i in range(0, len(users), batch_size):
                batch = users[i:i + batch_size]
                
                # Process batch concurrently
                tasks = []
                for user in batch:
                    if user.has_stories and user.telegram_id not in self._access_denied_users:
                        task = asyncio.create_task(
                            self.download_user_stories(client, user)
                        )
                        tasks.append((user.telegram_id, task))
                
                # Wait for batch completion
                for user_id, task in tasks:
                    try:
                        stories = await task
                        if stories:
                            results["successful_users"] += 1
                            results["total_stories"] += len(stories)
                            results["downloaded_stories"] += len(stories)
                        else:
                            results["failed_users"] += 1
                    except Exception as e:
                        results["failed_users"] += 1
                        results["errors"].append(f"User {user_id}: {str(e)}")
                
                # Rate limiting between batches
                if i + batch_size < len(users):
                    await asyncio.sleep(2)
            
            self.logger.info(f"Batch story download completed: {results}")
            
        except Exception as e:
            self.logger.error(f"Batch story download failed: {e}")
            results["errors"].append(f"Batch processing error: {str(e)}")
        
        return results
    
    async def cleanup_expired_stories(self) -> Dict[str, Any]:
        """
        Clean up expired stories from database and filesystem.
        
        Returns:
            Dict: Cleanup results
        """
        results = {
            "expired_stories_found": 0,
            "files_deleted": 0,
            "database_records_removed": 0,
            "errors": []
        }
        
        try:
            self.logger.info("Starting expired stories cleanup")
            
            async with async_session_maker() as db:
                # Find expired stories
                now = datetime.now()
                result = await db.execute(
                    select(UserStory).where(
                        UserStory.expires_at < now
                    )
                )
                expired_stories = result.scalars().all()
                
                results["expired_stories_found"] = len(expired_stories)
                
                for story in expired_stories:
                    try:
                        # Delete file if exists
                        if story.file_path:
                            file_path = Path(story.file_path)
                            if await aiofiles.os.path.exists(file_path):
                                await self.file_system_manager.delete_file(file_path)
                                results["files_deleted"] += 1
                        
                        # Remove database record
                        await db.delete(story)
                        results["database_records_removed"] += 1
                        
                    except Exception as e:
                        results["errors"].append(f"Story {story.story_id}: {str(e)}")
                
                await db.commit()
            
            self.logger.info(f"Expired stories cleanup completed: {results}")
            
        except Exception as e:
            self.logger.error(f"Expired stories cleanup failed: {e}")
            results["errors"].append(f"Cleanup error: {str(e)}")
        
        return results
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get detailed service statistics."""
        return {
            "service_stats": self._stats.copy(),
            "access_tracking": {
                "access_denied_users": len(self._access_denied_users),
                "privacy_restricted_users": len(self._privacy_restricted_users)
            },
            "settings": self._settings.copy(),
            "component_stats": {
                "session_recovery": self.session_recovery.get_statistics(),
                "rate_limiter": self.rate_limiter.get_statistics(),
                "file_system": await self._get_file_system_status()
            }
        }
    
    async def shutdown(self) -> None:
        """Shutdown the service and cleanup resources."""
        try:
            self.logger.info("Shutting down EnhancedStoryService")
            
            # Shutdown components
            await self.session_recovery.shutdown()
            await self.file_system_manager.shutdown()
            
            self._initialized = False
            self.logger.info("EnhancedStoryService shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during EnhancedStoryService shutdown: {e}")


# Global instance
enhanced_story_service = EnhancedStoryService()