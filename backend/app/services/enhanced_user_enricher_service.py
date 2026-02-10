import asyncio
import os
import hashlib
import logging
import aiofiles
import aiofiles.os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
from telethon import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.stories import GetPeerStoriesRequest
from telethon.tl.functions.photos import GetUserPhotosRequest
from telethon.tl.types import User, UserFull, InputPeerUser, Photo, InputUser
from telethon.errors import FloodWaitError, UserNotParticipantError, RPCError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, or_
from sqlalchemy.dialects.postgresql import insert

from backend.app.models.telegram_user import TelegramUser
from backend.app.models.history import UserProfilePhoto, UserProfileHistory
from backend.app.models.membership import GroupMembership
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
from backend.app.core.config_manager import ConfigManager, get_config_manager
from backend.app.core.api_retry_wrapper import APIRetryWrapper
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem


class ProfilePhotoResolution(Enum):
    """Profile photo resolution options"""
    THUMBNAIL = "thumbnail"
    MEDIUM = "medium"
    FULL = "full"


@dataclass
class ProfilePhotoDownloadResult:
    """Result of profile photo download operation"""
    success: bool
    file_path: Optional[str] = None
    file_hash: Optional[str] = None
    file_size: int = 0
    resolution: Optional[ProfilePhotoResolution] = None
    error_message: Optional[str] = None
    is_duplicate: bool = False
    validation_status: Optional[ValidationStatus] = None


class EnhancedUserEnricherService:
    """
    Enhanced user enricher service with comprehensive profile photo management.
    
    Features:
    - Profile photo download with resolution fallback
    - Change detection for profile updates
    - Retry mechanisms with exponential backoff
    - Graceful handling of missing photos
    - Integration with enhanced media components
    - ConfigManager integration for all settings
    - APIRetryWrapper for automatic retries
    - Enhanced logging with structured context
    """
    
    def __init__(self, media_dir: Optional[str] = None, config_manager: Optional[ConfigManager] = None, logger: Optional[EnhancedLoggingSystem] = None):
        # Initialize configuration manager
        self.config = config_manager or get_config_manager()
        
        # Initialize enhanced logging system
        self.logger = logger or EnhancedLoggingSystem()
        
        # Get media directory from config
        self.media_dir = Path(media_dir or self.config.get("MEDIA_DIR", "media"))
        self.profile_photos_dir = self.media_dir / "profile_photos"
        
        # Enhanced components
        self.session_recovery = SessionRecoveryManager()
        self.media_validator = MediaValidator()
        self.duplicate_detector = DuplicateDetector()
        self.file_system_manager = FileSystemManager(str(self.media_dir))
        self.rate_limiter = APIRateLimiter()
        
        # Initialize API retry wrapper
        self.retry_wrapper = APIRetryWrapper(self.config, self.logger)
        
        # Queue management (optional - can be injected)
        self.queue_manager: Optional[DownloadQueueManager] = None
        
        # Legacy queue support for backward compatibility
        self._enrichment_queue: asyncio.Queue = asyncio.Queue()
        self._enrichment_task: Optional[asyncio.Task] = None
        self._processed_users: set[int] = set()
        
        # Load settings from ConfigManager
        self._settings = {
            "max_retries": self.config.get_int("USER_ENRICHMENT_MAX_RETRIES", 3),
            "retry_delay_base": self.config.get_int("TELEGRAM_API_RETRY_DELAY_BASE", 2),
            "exponential_backoff": True,
            "jitter_enabled": self.config.get_bool("TELEGRAM_API_RETRY_JITTER", True),
            "validate_downloads": self.config.get_bool("MEDIA_VALIDATION_ENABLED", True),
            "detect_duplicates": True,
            "resolution_fallback": True,
            "batch_size": self.config.get_int("USER_ENRICHMENT_BATCH_SIZE", 20),
            "concurrent_downloads": 3,
            "profile_photo_timeout": self.config.get_int("USER_ENRICHMENT_TIMEOUT", 30),
            "change_detection_enabled": True,
            "archive_old_photos": True,
            "max_photos_per_user": 100
        }
        
        # Semaphore for concurrent operations
        self._semaphore = asyncio.Semaphore(self._settings["concurrent_downloads"])
        
        # Statistics tracking
        self._stats = {
            "users_enriched": 0,
            "photos_downloaded": 0,
            "photos_failed": 0,
            "duplicates_detected": 0,
            "changes_detected": 0,
            "retries_performed": 0,
            "validation_failures": 0
        }
        
        self._initialized = False
    
    async def initialize(self) -> bool:
        """
        Initialize the enhanced user enricher service.
        
        Returns:
            bool: True if initialization successful
        """
        try:
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "initialize",
                "Initializing EnhancedUserEnricherService"
            )
            
            # Initialize file system manager
            if not await self.file_system_manager.initialize():
                await self.logger.log_error(
                    "EnhancedUserEnricherService",
                    "initialize",
                    "Failed to initialize file system manager"
                )
                return False
            
            # Note: SessionRecoveryManager doesn't have an initialize() method
            # It's ready to use immediately after construction
            
            # Create profile photos directory structure
            await self._create_directory_structure()
            
            self._initialized = True
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "initialize",
                "EnhancedUserEnricherService initialized successfully",
                details={
                    "media_dir": str(self.media_dir),
                    "settings": self._settings
                }
            )
            return True
            
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "initialize",
                "Failed to initialize EnhancedUserEnricherService",
                error=e
            )
            return False
    
    async def _create_directory_structure(self) -> None:
        """Create the profile photos directory structure."""
        try:
            # Ensure profile photos directory exists
            await aiofiles.os.makedirs(self.profile_photos_dir, exist_ok=True)
            
            # Create numbered subdirectories for load balancing
            for i in range(1, 101):
                subdir = self.profile_photos_dir / str(i)
                await aiofiles.os.makedirs(subdir, exist_ok=True)
            
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "_create_directory_structure",
                "Profile photos directory structure created"
            )
            
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "_create_directory_structure",
                "Failed to create directory structure",
                error=e
            )
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current service status and statistics.
        
        Returns:
            Dict: Service status information
        """
        is_running = self._enrichment_task is not None and not self._enrichment_task.done()
        
        return {
            "running": is_running,
            "initialized": self._initialized,
            "queue_size": self._enrichment_queue.qsize(),
            "processed_users": len(self._processed_users),
            "settings": self._settings.copy(),
            "statistics": self._stats.copy()
        }
    
    async def _get_file_system_status(self) -> Dict[str, Any]:
        """Get file system status information."""
        try:
            storage_info = await self.file_system_manager.get_storage_info()
            profile_stats = await self.file_system_manager.get_directory_stats('profile_photos')
            
            return {
                "storage_status": storage_info.status.value,
                "storage_usage_percent": storage_info.usage_percent,
                "profile_photos_count": profile_stats.file_count if profile_stats else 0,
                "profile_photos_size_mb": round(profile_stats.total_size / (1024 * 1024), 2) if profile_stats else 0
            }
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "_get_file_system_status",
                "Failed to get file system status",
                error=e
            )
            return {"error": str(e)}
    
    def validate_api_response(self, response: Any, expected_type: type = None) -> bool:
        """
        Validate Telegram API response.
        
        Args:
            response: API response to validate
            expected_type: Expected type of response (optional)
            
        Returns:
            bool: True if response is valid, False otherwise
            
        Example:
            if self.validate_api_response(full_user_result, UserFull):
                # Process valid response
        """
        try:
            # Check if response is None
            if response is None:
                return False
            
            # Check expected type if provided
            if expected_type is not None:
                if not isinstance(response, expected_type):
                    return False
            
            # For Telegram objects, check if they have required attributes
            if hasattr(response, '__dict__'):
                # Valid Telegram object
                return True
            
            return True
            
        except Exception as e:
            # Log validation error
            asyncio.create_task(self.logger.log_warning(
                "EnhancedUserEnricherService",
                "validate_api_response",
                "API response validation failed",
                error=e,
                details={"response_type": type(response).__name__}
            ))
            return False
    
    async def enrich_batch(
        self,
        client: TelegramClient,
        user_ids: List[int],
        group_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Enrich a batch of users with aggregated metrics.
        
        Args:
            client: Telegram client
            user_ids: List of user IDs to enrich
            group_id: Optional group ID for membership tracking
            
        Returns:
            Dict: Batch processing results with aggregated metrics
            
        Example:
            result = await enricher.enrich_batch(client, [123, 456, 789])
            print(f"Enriched {result['successful']} users")
        """
        start_time = datetime.now()
        
        results = {
            "total": len(user_ids),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "photos_downloaded": 0,
            "changes_detected": 0,
            "retries_performed": 0
        }
        
        try:
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "enrich_batch",
                f"Starting batch enrichment of {len(user_ids)} users",
                details={"batch_size": len(user_ids), "group_id": group_id}
            )
            
            # Process in batches
            batch_size = self._settings["batch_size"]
            
            for i in range(0, len(user_ids), batch_size):
                batch = user_ids[i:i + batch_size]
                
                # Queue all users in batch
                tasks = []
                for user_id in batch:
                    if user_id not in self._processed_users:
                        task = asyncio.create_task(
                            self.enrich_user(client, user_id, group_id)
                        )
                        tasks.append((user_id, task))
                    else:
                        results["skipped"] += 1
                
                # Wait for batch completion
                for user_id, task in tasks:
                    try:
                        result = await task
                        if result:
                            results["successful"] += 1
                            self._processed_users.add(user_id)
                        else:
                            results["failed"] += 1
                    except Exception as e:
                        results["failed"] += 1
                        error_msg = f"User {user_id}: {str(e)}"
                        results["errors"].append(error_msg)
                        await self.logger.log_error(
                            "EnhancedUserEnricherService",
                            "enrich_batch",
                            f"Failed to enrich user {user_id}",
                            error=e
                        )
                
                # Rate limiting between batches
                if i + batch_size < len(user_ids):
                    await asyncio.sleep(1)
            
            # Aggregate metrics from stats
            results["photos_downloaded"] = self._stats["photos_downloaded"]
            results["changes_detected"] = self._stats["changes_detected"]
            results["retries_performed"] = self._stats["retries_performed"]
            
            # Calculate duration
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            # Log batch completion with metrics
            await self.logger.log_metrics(
                "EnhancedUserEnricherService",
                {
                    "operation": "enrich_batch",
                    "total_users": results["total"],
                    "successful": results["successful"],
                    "failed": results["failed"],
                    "skipped": results["skipped"],
                    "photos_downloaded": results["photos_downloaded"],
                    "changes_detected": results["changes_detected"],
                    "retries_performed": results["retries_performed"],
                    "duration_ms": duration_ms,
                    "success_rate": results["successful"] / results["total"] if results["total"] > 0 else 0
                }
            )
            
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "enrich_batch",
                f"Batch enrichment completed: {results['successful']}/{results['total']} successful",
                details=results
            )
            
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "enrich_batch",
                "Batch enrichment failed",
                error=e
            )
            results["errors"].append(f"Batch processing error: {str(e)}")
        
        return results
    
    async def start_worker(self) -> bool:
        """
        Start the enrichment worker.
        
        Returns:
            bool: True if worker started successfully
        """
        try:
            if not self._initialized:
                if not await self.initialize():
                    return False
            
            if self._enrichment_task is None or self._enrichment_task.done():
                self._enrichment_task = asyncio.create_task(self._enrichment_worker())
                await self.logger.log_info(
                    "EnhancedUserEnricherService",
                    "start_worker",
                    "Enrichment worker started"
                )
                return True
            
            return True
            
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "start_worker",
                "Failed to start enrichment worker",
                error=e
            )
            return False
    
    async def stop_worker(self) -> None:
        """Stop the enrichment worker."""
        try:
            if self._enrichment_task:
                self._enrichment_task.cancel()
                try:
                    await self._enrichment_task
                except asyncio.CancelledError:
                    pass
                await self.logger.log_info(
                    "EnhancedUserEnricherService",
                    "stop_worker",
                    "Enrichment worker stopped"
                )
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "stop_worker",
                "Error stopping enrichment worker",
                error=e
            )
    
    async def queue_enrichment(self, client: TelegramClient, telegram_id: int, group_id: Optional[int] = None, priority: TaskPriority = TaskPriority.NORMAL) -> bool:
        """
        Queue a user for enrichment.
        
        Args:
            client: Telegram client
            telegram_id: User's Telegram ID
            group_id: Optional group ID for membership tracking
            priority: Task priority
            
        Returns:
            bool: True if queued successfully
        """
        try:
            if telegram_id in self._processed_users:
                return True
            
            # Use queue manager if available
            if self.queue_manager:
                task_data = {
                    'client': client,
                    'telegram_id': telegram_id,
                    'group_id': group_id,
                    'operation': 'user_enrichment'
                }
                
                return await self.queue_manager.add_task(
                    task_id=f"user_enrich_{telegram_id}",
                    task_data=task_data,
                    priority=priority
                )
            else:
                # Fallback to legacy queue
                await self._enrichment_queue.put((client, telegram_id, group_id, priority))
                return True
                
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "queue_enrichment",
                f"Failed to queue enrichment for user {telegram_id}",
                error=e
            )
            return False
    
    async def _enrichment_worker(self) -> None:
        """Main enrichment worker loop."""
        await self.logger.log_info(
            "EnhancedUserEnricherService",
            "_enrichment_worker",
            "Enrichment worker started"
        )
        
        while True:
            try:
                # Get task from queue
                if self.queue_manager:
                    task = await self.queue_manager.get_next_task()
                    if task:
                        client = task.task_data['client']
                        telegram_id = task.task_data['telegram_id']
                        group_id = task.task_data.get('group_id')
                        priority = task.priority
                    else:
                        await asyncio.sleep(1)
                        continue
                else:
                    # Fallback to legacy queue
                    client, telegram_id, group_id, priority = await self._enrichment_queue.get()
                
                if telegram_id not in self._processed_users:
                    async with self._semaphore:
                        try:
                            # Check session health before processing
                            if not await self.session_recovery.is_session_healthy(client):
                                await self.logger.log_warning(
                                    "EnhancedUserEnricherService",
                                    "_enrichment_worker",
                                    f"Session unhealthy for user {telegram_id}, attempting recovery"
                                )
                                if not await self.session_recovery.recover_session(client):
                                    await self.logger.log_error(
                                        "EnhancedUserEnricherService",
                                        "_enrichment_worker",
                                        f"Failed to recover session for user {telegram_id}"
                                    )
                                    continue
                            
                            # Perform enrichment
                            result = await self.enrich_user(client, telegram_id, group_id)
                            
                            if result:
                                self._processed_users.add(telegram_id)
                                self._stats["users_enriched"] += 1
                                
                                # Mark task as completed if using queue manager
                                if self.queue_manager and task:
                                    await self.queue_manager.complete_task(task.id)
                            
                        except FloodWaitError as e:
                            await self.logger.log_warning(
                                "EnhancedUserEnricherService",
                                "_enrichment_worker",
                                f"FloodWait: {e.seconds}s, requeuing user {telegram_id}"
                            )
                            await self.rate_limiter.handle_flood_wait(e, OperationType.PROFILE_PHOTO)
                            
                            # Requeue the task
                            await self.queue_enrichment(client, telegram_id, group_id, priority)
                            
                        except Exception as e:
                            await self.logger.log_error(
                                "EnhancedUserEnricherService",
                                "_enrichment_worker",
                                f"Error enriching user {telegram_id}",
                                error=e
                            )
                            
                            # Mark task as failed if using queue manager
                            if self.queue_manager and task:
                                await self.queue_manager.fail_task(task.id, str(e))
                
                # Mark legacy queue task as done
                if not self.queue_manager:
                    self._enrichment_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.logger.log_error(
                    "EnhancedUserEnricherService",
                    "_enrichment_worker",
                    "Enrichment worker error",
                    error=e
                )
                await asyncio.sleep(1)
        
        await self.logger.log_info(
            "EnhancedUserEnricherService",
            "_enrichment_worker",
            "Enrichment worker stopped"
        )
    
    async def enrich_user(
        self, 
        client: TelegramClient, 
        telegram_id: int, 
        group_id: Optional[int] = None
    ) -> Optional[TelegramUser]:
        """
        Enrich a user with comprehensive profile information and photos.
        
        Args:
            client: Telegram client
            telegram_id: User's Telegram ID
            group_id: Optional group ID for membership tracking
            
        Returns:
            TelegramUser: Enriched user object or None if failed
        """
        try:
            async with async_session_maker() as db:
                # Get or create user using UPSERT
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
                    if not user:
                        return None
                
                try:
                    # Get user entity with retry wrapper
                    input_user_result = await self.retry_wrapper.execute_with_retry(
                        client.get_input_entity,
                        telegram_id,
                        operation_name="get_input_entity"
                    )
                    
                    if not input_user_result.success:
                        await self.logger.log_error(
                            "EnhancedUserEnricherService",
                            "enrich_user",
                            f"Failed to get input entity for user {telegram_id}",
                            error=input_user_result.error
                        )
                        return None
                    
                    input_user = input_user_result.result
                    self._stats["retries_performed"] += input_user_result.attempts - 1
                    
                    # Get full user info with retry wrapper
                    full_user_result_retry = await self.retry_wrapper.execute_with_retry(
                        client,
                        GetFullUserRequest(input_user),
                        operation_name="get_full_user"
                    )
                    
                    if not full_user_result_retry.success:
                        await self.logger.log_error(
                            "EnhancedUserEnricherService",
                            "enrich_user",
                            f"Failed to get full user info for user {telegram_id}",
                            error=full_user_result_retry.error
                        )
                        return None
                    
                    full_user_result = full_user_result_retry.result
                    self._stats["retries_performed"] += full_user_result_retry.attempts - 1
                    
                    # Validate API response
                    if not self.validate_api_response(full_user_result):
                        await self.logger.log_warning(
                            "EnhancedUserEnricherService",
                            "enrich_user",
                            f"Invalid API response for user {telegram_id}"
                        )
                        return None
                    
                    if full_user_result:
                        full_user: UserFull = full_user_result.full_user
                        tg_user: User = full_user_result.users[0] if full_user_result.users else None
                        
                        if tg_user:
                            # Update user information with change detection
                            await self._update_user_from_entity(db, user, tg_user, full_user)
                        
                        # Update bio with change detection
                        if full_user.about and full_user.about != user.bio:
                            if user.bio is not None:
                                change = UserProfileHistory(
                                    user_id=user.id,
                                    field_changed="bio",
                                    old_value=user.bio,
                                    new_value=full_user.about
                                )
                                db.add(change)
                                self._stats["changes_detected"] += 1
                            user.bio = full_user.about
                        
                        # Enhanced profile photo synchronization
                        if tg_user:
                            photo_results = await self.sync_all_profile_photos(client, db, user, tg_user)
                            self._stats["photos_downloaded"] += photo_results.get("downloaded", 0)
                            self._stats["photos_failed"] += photo_results.get("failed", 0)
                            self._stats["duplicates_detected"] += photo_results.get("duplicates", 0)
                        
                        # Check for stories
                        has_stories = await self._check_stories(client, telegram_id)
                        if has_stories != user.has_stories:
                            user.has_stories = has_stories
                            if user.has_stories:
                                self._stats["changes_detected"] += 1
                        
                        await db.commit()
                        live_stats.record("users_enriched")
                        
                        await self.logger.log_with_context(
                            "INFO",
                            f"Enriched user {telegram_id}: {user.username or user.first_name}",
                            "EnhancedUserEnricherService",
                            context={
                                "telegram_id": telegram_id,
                                "username": user.username,
                                "has_bio": bool(user.bio),
                                "has_stories": has_stories,
                                "photos_count": user.photos_count,
                                "retries": self._stats["retries_performed"]
                            }
                        )
                        
                        # Download stories for watchlist users
                        if has_stories and user.is_watchlist:
                            asyncio.create_task(self._download_stories_for_user(client, user))
                
                except UserNotParticipantError:
                    await self.logger.log_debug(
                        "EnhancedUserEnricherService",
                        "enrich_user",
                        f"User {telegram_id} not accessible"
                    )
                except FloodWaitError:
                    raise
                except Exception as e:
                    await self.logger.log_error(
                        "EnhancedUserEnricherService",
                        "enrich_user",
                        f"Error getting full user {telegram_id}",
                        error=e
                    )
                
                # Ensure group membership if specified
                if group_id:
                    await self._ensure_membership(db, user.id, group_id)
                
                await db.commit()
                return user
                
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "enrich_user",
                f"Failed to enrich user {telegram_id}",
                error=e
            )
            return None
    
    async def _update_user_from_entity(
        self, 
        db: AsyncSession, 
        user: TelegramUser, 
        tg_user: User,
        full_user: UserFull
    ) -> None:
        """Update user information from Telegram entity with change detection."""
        changes = []
        
        # Track changes for all fields
        fields_to_check = [
            ('username', getattr(tg_user, 'username', None)),
            ('first_name', getattr(tg_user, 'first_name', None)),
            ('last_name', getattr(tg_user, 'last_name', None)),
            ('phone', getattr(tg_user, 'phone', None))
        ]
        
        for field_name, new_value in fields_to_check:
            old_value = getattr(user, field_name)
            if new_value != old_value:
                if old_value is not None:  # Only track changes, not initial values
                    changes.append((field_name, old_value, new_value))
                setattr(user, field_name, new_value)
        
        # Update boolean fields (no change tracking needed)
        user.access_hash = getattr(tg_user, 'access_hash', None)
        user.is_premium = getattr(tg_user, 'premium', False) or False
        user.is_verified = getattr(tg_user, 'verified', False) or False
        user.is_bot = getattr(tg_user, 'bot', False) or False
        user.is_scam = getattr(tg_user, 'scam', False) or False
        user.is_fake = getattr(tg_user, 'fake', False) or False
        user.is_restricted = getattr(tg_user, 'restricted', False) or False
        user.is_deleted = getattr(tg_user, 'deleted', False) or False
        
        # Record changes in history
        for field, old_val, new_val in changes:
            change = UserProfileHistory(
                user_id=user.id,
                field_changed=field,
                old_value=old_val,
                new_value=new_val
            )
            db.add(change)
            self._stats["changes_detected"] += 1
    
    async def sync_all_profile_photos(
        self, 
        client: TelegramClient, 
        db: AsyncSession, 
        user: TelegramUser, 
        tg_user: User
    ) -> Dict[str, int]:
        """
        Synchronize all profile photos for a user with enhanced features.
        
        Args:
            client: Telegram client
            db: Database session
            user: User object
            tg_user: Telegram user entity
            
        Returns:
            Dict: Results with counts of downloaded, failed, and duplicate photos
        """
        results = {"downloaded": 0, "failed": 0, "duplicates": 0, "skipped": 0}
        
        try:
            input_user = InputUser(
                user_id=user.telegram_id,
                access_hash=user.access_hash or 0
            )
            
            # Get all profile photos
            all_photos = await self._get_all_profile_photos(client, input_user)
            
            if not all_photos:
                # Try to download current photo if available
                if tg_user.photo:
                    result = await self._download_current_photo(client, db, user, tg_user)
                    if result.success:
                        results["downloaded"] += 1
                    else:
                        results["failed"] += 1
                return results
            
            # Get optimal directory for user photos
            user_dir = await self._get_user_photo_directory(user.telegram_id)
            
            # Get existing photo IDs to avoid duplicates
            existing_result = await db.execute(
                select(UserProfilePhoto.telegram_photo_id).where(
                    UserProfilePhoto.user_id == user.id,
                    UserProfilePhoto.telegram_photo_id.isnot(None)
                )
            )
            existing_photo_ids = {row[0] for row in existing_result.all()}
            
            # Determine current photo ID
            current_photo_id = None
            if tg_user.photo and hasattr(tg_user.photo, 'photo_id'):
                current_photo_id = tg_user.photo.photo_id
            
            # Reset current photo flags
            await db.execute(
                update(UserProfilePhoto).where(
                    UserProfilePhoto.user_id == user.id,
                    UserProfilePhoto.is_current == True
                ).values(is_current=False)
            )
            
            # Process photos with rate limiting
            for idx, photo in enumerate(all_photos[:self._settings["max_photos_per_user"]]):
                if not isinstance(photo, Photo):
                    continue
                
                telegram_photo_id = photo.id
                
                # Skip if already exists
                if telegram_photo_id in existing_photo_ids:
                    # Update current status if needed
                    if telegram_photo_id == current_photo_id:
                        await db.execute(
                            update(UserProfilePhoto).where(
                                UserProfilePhoto.user_id == user.id,
                                UserProfilePhoto.telegram_photo_id == telegram_photo_id
                            ).values(is_current=True)
                        )
                    results["skipped"] += 1
                    continue
                
                # Apply rate limiting
                await self.rate_limiter.wait_if_needed(OperationType.PROFILE_PHOTO)
                
                # Download photo with enhanced features
                download_result = await self._download_profile_photo_enhanced(
                    client, photo, user_dir, telegram_photo_id
                )
                
                if download_result.success:
                    # Check for duplicates
                    is_duplicate = False
                    if self._settings["detect_duplicates"] and download_result.file_hash:
                        is_duplicate = await self._check_photo_duplicate(
                            db, user.id, download_result.file_hash
                        )
                        if is_duplicate:
                            results["duplicates"] += 1
                            # Delete duplicate file
                            if download_result.file_path:
                                await self.file_system_manager.delete_file(Path(download_result.file_path))
                            continue
                    
                    # Determine photo metadata
                    is_video = photo.video_sizes is not None and len(photo.video_sizes) > 0
                    captured_at = None
                    if hasattr(photo, 'date') and photo.date:
                        captured_at = photo.date.replace(tzinfo=None) if photo.date.tzinfo else photo.date
                    
                    is_current = (telegram_photo_id == current_photo_id) or (idx == 0 and current_photo_id is None)
                    
                    # Create profile photo record
                    profile_photo = UserProfilePhoto(
                        user_id=user.id,
                        photo_id=str(telegram_photo_id),
                        telegram_photo_id=telegram_photo_id,
                        file_path=download_result.file_path,
                        file_hash=download_result.file_hash,
                        is_current=is_current,
                        is_video=is_video,
                        captured_at=captured_at,
                        file_size=download_result.file_size,
                        validation_status=download_result.validation_status.value if download_result.validation_status else None
                    )
                    db.add(profile_photo)
                    
                    # Update user's current photo path
                    if is_current:
                        user.current_photo_path = download_result.file_path
                    
                    results["downloaded"] += 1
                    
                else:
                    results["failed"] += 1
                    await self.logger.log_warning(
                        "EnhancedUserEnricherService",
                        "sync_all_profile_photos",
                        f"Failed to download profile photo {telegram_photo_id} for user {user.telegram_id}",
                        details={"error_message": download_result.error_message}
                    )
            
            # Update user's photo count
            result = await db.execute(
                select(UserProfilePhoto).where(UserProfilePhoto.user_id == user.id)
            )
            user.photos_count = len(result.all())
            
            if results["downloaded"] > 0:
                await self.logger.log_info(
                    "EnhancedUserEnricherService",
                    "sync_all_profile_photos",
                    f"Downloaded {results['downloaded']} profile photos for user {user.telegram_id}",
                    details=results
                )
            
        except FloodWaitError:
            raise
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "sync_all_profile_photos",
                f"Error syncing profile photos for user {user.telegram_id}",
                error=e
            )
            results["failed"] += 1
        
        return results
    
    async def _get_all_profile_photos(self, client: TelegramClient, input_user: InputUser) -> List[Photo]:
        """Get all profile photos for a user with pagination."""
        all_photos = []
        offset = 0
        batch_limit = 100
        
        try:
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
                
                # Apply rate limiting between requests
                await self.rate_limiter.wait_if_needed(OperationType.PROFILE_PHOTO)
        
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "_get_all_profile_photos",
                "Error getting profile photos",
                error=e
            )
        
        return all_photos
    
    async def _get_user_photo_directory(self, telegram_id: int) -> Path:
        """Get optimal directory for user photos with load balancing."""
        # Use file system manager to get optimal subdirectory
        optimal_subdir = await self.file_system_manager.get_optimal_subdirectory('profile_photos')
        
        if optimal_subdir:
            user_dir = optimal_subdir / str(telegram_id)
        else:
            # Fallback to direct profile photos directory
            user_dir = self.profile_photos_dir / str(telegram_id)
        
        # Ensure directory exists
        await self.file_system_manager.ensure_file_path(user_dir / "dummy")
        
        return user_dir
    
    async def _download_profile_photo_enhanced(
        self, 
        client: TelegramClient, 
        photo: Photo, 
        user_dir: Path, 
        telegram_photo_id: int
    ) -> ProfilePhotoDownloadResult:
        """
        Download a profile photo with enhanced features and retry logic.
        
        Args:
            client: Telegram client
            photo: Photo object
            user_dir: User's photo directory
            telegram_photo_id: Telegram photo ID
            
        Returns:
            ProfilePhotoDownloadResult: Download result
        """
        result = ProfilePhotoDownloadResult(success=False)
        
        # Determine if it's a video profile photo
        is_video = photo.video_sizes is not None and len(photo.video_sizes) > 0
        
        # Try different resolutions with fallback
        resolutions = [ProfilePhotoResolution.FULL, ProfilePhotoResolution.MEDIUM, ProfilePhotoResolution.THUMBNAIL]
        
        for resolution in resolutions:
            try:
                # Determine file extension and download parameters
                if is_video and resolution == ProfilePhotoResolution.FULL:
                    ext = "mp4"
                    download_params = {}
                else:
                    ext = "jpg"
                    if resolution == ProfilePhotoResolution.THUMBNAIL:
                        download_params = {"thumb": -1}
                    elif resolution == ProfilePhotoResolution.MEDIUM:
                        download_params = {"thumb": 0}
                    else:
                        download_params = {}
                
                filename = f"{telegram_photo_id}.{ext}"
                file_path = user_dir / filename
                
                # Download with timeout
                download_task = asyncio.create_task(
                    client.download_media(photo, file=str(file_path), **download_params)
                )
                
                try:
                    await asyncio.wait_for(download_task, timeout=self._settings["profile_photo_timeout"])
                except asyncio.TimeoutError:
                    download_task.cancel()
                    continue
                
                # Verify file exists and has content
                if not await aiofiles.os.path.exists(file_path):
                    continue
                
                file_stat = await aiofiles.os.stat(file_path)
                if file_stat.st_size == 0:
                    await self.file_system_manager.delete_file(file_path)
                    continue
                
                # Calculate file hash
                file_hash = None
                if self._settings["detect_duplicates"]:
                    try:
                        async with aiofiles.open(file_path, 'rb') as f:
                            content = await f.read()
                            file_hash = hashlib.sha256(content).hexdigest()
                    except Exception as e:
                        await self.logger.log_warning(
                            "EnhancedUserEnricherService",
                            "_download_profile_photo_enhanced",
                            f"Failed to calculate hash for {file_path}",
                            error=e
                        )
                
                # Validate file if enabled
                validation_status = None
                if self._settings["validate_downloads"]:
                    validation_status = await self.media_validator.validate_file(file_path)
                    if validation_status == ValidationStatus.CORRUPTED:
                        await self.file_system_manager.delete_file(file_path)
                        continue
                
                # Success
                result.success = True
                result.file_path = str(file_path)
                result.file_hash = file_hash
                result.file_size = file_stat.st_size
                result.resolution = resolution
                result.validation_status = validation_status
                
                break
                
            except Exception as e:
                await self.logger.log_warning(
                    "EnhancedUserEnricherService",
                    "_download_profile_photo_enhanced",
                    f"Failed to download photo {telegram_photo_id} at {resolution.value}",
                    error=e
                )
                result.error_message = str(e)
                continue
        
        if not result.success:
            result.error_message = result.error_message or "All resolution attempts failed"
        
        return result
    
    async def _download_current_photo(
        self, 
        client: TelegramClient, 
        db: AsyncSession, 
        user: TelegramUser, 
        tg_user: User
    ) -> ProfilePhotoDownloadResult:
        """Download the current profile photo for a user."""
        result = ProfilePhotoDownloadResult(success=False)
        
        try:
            photo = tg_user.photo
            if not photo:
                result.error_message = "No profile photo available"
                return result
            
            photo_id = str(photo.photo_id) if hasattr(photo, 'photo_id') else str(id(photo))
            
            # Check if already exists
            existing = await db.execute(
                select(UserProfilePhoto).where(
                    UserProfilePhoto.user_id == user.id,
                    UserProfilePhoto.photo_id == photo_id
                )
            )
            if existing.scalar_one_or_none():
                result.success = True
                result.error_message = "Photo already exists"
                return result
            
            # Get user directory
            user_dir = await self._get_user_photo_directory(user.telegram_id)
            
            filename = f"{photo_id}.jpg"
            file_path = user_dir / filename
            
            # Download with timeout
            download_task = asyncio.create_task(
                client.download_profile_photo(tg_user, file=str(file_path))
            )
            
            try:
                await asyncio.wait_for(download_task, timeout=self._settings["profile_photo_timeout"])
            except asyncio.TimeoutError:
                download_task.cancel()
                result.error_message = "Download timeout"
                return result
            
            # Verify download
            if not await aiofiles.os.path.exists(file_path):
                result.error_message = "File not created"
                return result
            
            file_stat = await aiofiles.os.stat(file_path)
            if file_stat.st_size == 0:
                await self.file_system_manager.delete_file(file_path)
                result.error_message = "Empty file downloaded"
                return result
            
            # Calculate hash and validate
            file_hash = None
            if self._settings["detect_duplicates"]:
                try:
                    async with aiofiles.open(file_path, 'rb') as f:
                        content = await f.read()
                        file_hash = hashlib.sha256(content).hexdigest()
                except Exception:
                    pass
            
            validation_status = None
            if self._settings["validate_downloads"]:
                validation_status = await self.media_validator.validate_file(file_path)
            
            # Reset current photo flags and create new record
            await db.execute(
                update(UserProfilePhoto).where(
                    UserProfilePhoto.user_id == user.id,
                    UserProfilePhoto.is_current == True
                ).values(is_current=False)
            )
            
            profile_photo = UserProfilePhoto(
                user_id=user.id,
                photo_id=photo_id,
                file_path=str(file_path),
                file_hash=file_hash,
                is_current=True,
                file_size=file_stat.st_size,
                validation_status=validation_status.value if validation_status else None
            )
            db.add(profile_photo)
            
            user.current_photo_path = str(file_path)
            
            result.success = True
            result.file_path = str(file_path)
            result.file_hash = file_hash
            result.file_size = file_stat.st_size
            result.validation_status = validation_status
            
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "_download_current_photo",
                f"Downloaded current profile photo for user {user.telegram_id}"
            )
            
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "_download_current_photo",
                f"Error downloading current profile photo for user {user.telegram_id}",
                error=e
            )
            result.error_message = str(e)
        
        return result
    
    async def _check_photo_duplicate(self, db: AsyncSession, user_id: int, file_hash: str) -> bool:
        """Check if a photo is a duplicate based on hash."""
        try:
            result = await db.execute(
                select(UserProfilePhoto).where(
                    UserProfilePhoto.user_id == user_id,
                    UserProfilePhoto.file_hash == file_hash
                )
            )
            return result.scalar_one_or_none() is not None
        except Exception:
            return False
    
    async def _check_stories(self, client: TelegramClient, telegram_id: int) -> bool:
        """Check if a user has active stories."""
        try:
            input_user = await client.get_input_entity(telegram_id)
            stories = await client(GetPeerStoriesRequest(peer=input_user))
            return bool(stories and stories.stories and stories.stories.stories)
        except Exception:
            return False
    
    async def _ensure_membership(self, db: AsyncSession, user_id: int, group_id: int) -> None:
        """Ensure group membership record exists."""
        try:
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
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "_ensure_membership",
                f"Error ensuring membership for user {user_id} in group {group_id}",
                error=e
            )
    
    async def _download_stories_for_user(self, client: TelegramClient, user: TelegramUser) -> None:
        """Download stories for a watchlist user."""
        try:
            from backend.app.services.enhanced_story_service import EnhancedStoryService
            
            async with async_session_maker() as db:
                result = await db.execute(
                    select(TelegramUser).where(TelegramUser.id == user.id)
                )
                fresh_user = result.scalar_one_or_none()
                
                if fresh_user and fresh_user.has_stories:
                    story_service = EnhancedStoryService()
                    if not story_service._initialized:
                        await story_service.initialize()
                    
                    stories = await story_service.download_user_stories(client, fresh_user)
                    if stories:
                        await self.logger.log_info(
                            "EnhancedUserEnricherService",
                            "_download_stories_for_user",
                            f"Downloaded {len(stories)} stories for user {user.telegram_id}",
                            details={"stories_count": len(stories)}
                        )
                        
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "_download_stories_for_user",
                f"Failed to download stories for user {user.telegram_id}",
                error=e
            )
    
    async def batch_enrich_users(self, client: TelegramClient, user_ids: List[int], group_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Batch enrich multiple users.
        
        Args:
            client: Telegram client
            user_ids: List of user IDs to enrich
            group_id: Optional group ID for membership tracking
            
        Returns:
            Dict: Batch processing results
        """
        results = {
            "total": len(user_ids),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": []
        }
        
        try:
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "batch_enrich_users",
                f"Starting batch enrichment of {len(user_ids)} users"
            )
            
            # Process in batches
            batch_size = self._settings["batch_size"]
            
            for i in range(0, len(user_ids), batch_size):
                batch = user_ids[i:i + batch_size]
                
                # Queue all users in batch
                tasks = []
                for user_id in batch:
                    if user_id not in self._processed_users:
                        task = asyncio.create_task(
                            self.enrich_user(client, user_id, group_id)
                        )
                        tasks.append((user_id, task))
                    else:
                        results["skipped"] += 1
                
                # Wait for batch completion
                for user_id, task in tasks:
                    try:
                        result = await task
                        if result:
                            results["successful"] += 1
                            self._processed_users.add(user_id)
                        else:
                            results["failed"] += 1
                    except Exception as e:
                        results["failed"] += 1
                        results["errors"].append(f"User {user_id}: {str(e)}")
                
                # Rate limiting between batches
                if i + batch_size < len(user_ids):
                    await asyncio.sleep(1)
            
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "batch_enrich_users",
                f"Batch enrichment completed: {results}"
            )
            
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "batch_enrich_users",
                "Batch enrichment failed",
                error=e
            )
            results["errors"].append(f"Batch processing error: {str(e)}")
        
        return results
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get detailed service statistics."""
        return {
            "service_stats": self._stats.copy(),
            "queue_status": {
                "queue_size": self._enrichment_queue.qsize(),
                "processed_users": len(self._processed_users),
                "worker_running": self._enrichment_task is not None and not self._enrichment_task.done()
            },
            "settings": self._settings.copy(),
            "component_stats": {
                "session_recovery": self.session_recovery.get_statistics(),
                "rate_limiter": self.rate_limiter.get_statistics(),
                "file_system": await self._get_file_system_status()
            }
        }
    
    async def enrich_user_with_notification(
        self,
        client: TelegramClient,
        telegram_id: int,
        group_id: Optional[int] = None,
        status_tracker: Optional['EnrichmentStatusTracker'] = None
    ) -> Optional[TelegramUser]:
        """
        Enrich user and broadcast WebSocket notification when complete.
        
        Args:
            client: Telegram client
            telegram_id: User's Telegram ID
            group_id: Optional group ID for context
            status_tracker: Optional status tracker for duplicate prevention
            
        Returns:
            Enriched TelegramUser or None if failed
        """
        from backend.app.services.websocket_manager import ws_manager, WSMessage
        
        # Check if enrichment is needed
        if status_tracker:
            if not await status_tracker.is_enrichment_needed(telegram_id):
                await self.logger.log_info(
                    "EnhancedUserEnricherService",
                    "enrich_user_with_notification",
                    f"Enrichment not needed for user {telegram_id} (already in progress or cached)"
                )
                return None
            
            # Mark as started
            if not await status_tracker.start_enrichment(telegram_id):
                return None
        
        try:
            # Perform enrichment
            user = await self.enrich_user(client, telegram_id, group_id)
            
            if user:
                # Mark as completed
                if status_tracker:
                    await status_tracker.complete_enrichment(telegram_id)
                
                # Broadcast enrichment completion via WebSocket
                await ws_manager.broadcast("user_enrichment", WSMessage(
                    event="enrichment_complete",
                    data={
                        "telegram_id": telegram_id,
                        "user_id": user.id,
                        "display_name": f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username or f"User {telegram_id}",
                        "username": user.username,
                        "photo_path": user.current_photo_path,
                        "is_premium": user.is_premium,
                        "is_bot": user.is_bot
                    }
                ))
                
                await self.logger.log_info(
                    "EnhancedUserEnricherService",
                    "enrich_user_with_notification",
                    f"Successfully enriched user {telegram_id} and broadcasted update"
                )
                
                return user
            else:
                # Mark as failed
                if status_tracker:
                    await status_tracker.fail_enrichment(telegram_id, "Enrichment returned None")
                
                # Broadcast failure
                await ws_manager.broadcast("user_enrichment", WSMessage(
                    event="enrichment_failed",
                    data={
                        "telegram_id": telegram_id,
                        "error": "Failed to fetch user information"
                    }
                ))
                
                return None
                
        except Exception as e:
            # Mark as failed
            if status_tracker:
                await status_tracker.fail_enrichment(telegram_id, str(e))
            
            # Broadcast failure
            await ws_manager.broadcast("user_enrichment", WSMessage(
                event="enrichment_failed",
                data={
                    "telegram_id": telegram_id,
                    "error": str(e)
                }
            ))
            
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "enrich_user_with_notification",
                f"Failed to enrich user {telegram_id}",
                error=e
            )
            
            return None
    
    async def shutdown(self) -> None:
        """Shutdown the service and cleanup resources."""
        try:
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "shutdown",
                "Shutting down EnhancedUserEnricherService"
            )
            
            # Stop worker
            await self.stop_worker()
            
            # Shutdown components
            await self.session_recovery.shutdown()
            await self.file_system_manager.shutdown()
            
            self._initialized = False
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "shutdown",
                "EnhancedUserEnricherService shutdown complete"
            )
            
        except Exception as e:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "shutdown",
                "Error during EnhancedUserEnricherService shutdown",
                error=e
            )


# Global instance
enhanced_user_enricher = EnhancedUserEnricherService()