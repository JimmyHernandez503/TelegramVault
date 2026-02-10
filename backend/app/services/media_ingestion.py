import asyncio
import os
import hashlib
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List
from telethon import TelegramClient
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage,
    Document, Photo
)
from telethon.errors import FloodWaitError, RPCError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from backend.app.models.media import MediaFile
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.download_task import DownloadTask
from backend.app.db.database import async_session_maker
from backend.app.services.live_stats import live_stats
from backend.app.core.session_recovery_manager import SessionRecoveryManager
from backend.app.core.media_validator import MediaValidator, ValidationStatus
from backend.app.core.duplicate_detector import DuplicateDetector, SimilarityLevel
from backend.app.core.download_queue_manager import DownloadQueueManager, TaskPriority
from backend.app.core.api_rate_limiter import APIRateLimiter, OperationType
from backend.app.core.config_manager import ConfigManager
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem, LogLevel


class MediaIngestionService:
    def __init__(self, media_dir: Optional[str] = None, config_manager: Optional[ConfigManager] = None, enhanced_logger: Optional[EnhancedLoggingSystem] = None):
        # Initialize configuration manager
        self.config = config_manager or ConfigManager()
        if not self.config._loaded:
            self.config.load()
        
        # Initialize enhanced logging
        self.enhanced_logger = enhanced_logger or EnhancedLoggingSystem()
        self.logger = logging.getLogger(__name__)
        
        # Get media directory from config
        self.media_dir = media_dir or self.config.get("MEDIA_DIR", "media")
        
        # Enhanced components
        self.session_recovery = SessionRecoveryManager()
        self.media_validator = MediaValidator()
        self.duplicate_detector = DuplicateDetector()
        self.rate_limiter = APIRateLimiter()
        
        # Queue management (optional - can be injected)
        self.queue_manager: Optional[DownloadQueueManager] = None
        
        # Legacy queue support for backward compatibility - get worker count from config
        self._download_queue: asyncio.Queue = asyncio.Queue()
        self._download_tasks: list[asyncio.Task] = []
        self._num_workers = self.config.get_int("MEDIA_INGESTION_WORKERS", 5)
        self._semaphore = asyncio.Semaphore(self._num_workers)
        
        # Get configuration values
        self._max_retries = self.config.get_int("MEDIA_RETRY_MAX_ATTEMPTS", 3)
        self._download_timeout = self.config.get_int("MEDIA_DOWNLOAD_TIMEOUT", 30)
        self._validation_enabled = self.config.get_bool("MEDIA_VALIDATION_ENABLED", True)
        
        # Enhanced tracking
        self._known_hashes: set[str] = set()
        self._known_unique_ids: set[str] = set()
        self._processing_stats = {
            "total_processed": 0,
            "successful_downloads": 0,
            "failed_downloads": 0,
            "duplicates_detected": 0,
            "validation_failures": 0,
            "session_recoveries": 0
        }
        
        # Error categorization
        self._error_categories = {
            "network_errors": 0,
            "authorization_errors": 0,
            "rate_limit_errors": 0,
            "validation_errors": 0,
            "file_system_errors": 0,
            "unknown_errors": 0
        }
        
        # Create media directories
        for subdir in ["photos", "videos", "documents", "audio", "voice", "stickers"]:
            os.makedirs(os.path.join(self.media_dir, subdir), exist_ok=True)
    
    async def validate_downloaded_file(self, file_path: str, expected_size: Optional[int] = None, media_type: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """
        Validate a downloaded file to ensure it's valid and complete.
        
        Args:
            file_path: Path to the downloaded file
            expected_size: Expected file size in bytes (optional)
            media_type: Type of media (photo, video, etc.) for type-specific validation
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                error_msg = f"File does not exist: {file_path}"
                await self.enhanced_logger.log_with_context(
                    level=LogLevel.ERROR,
                    message="File validation failed - file not found",
                    service="MediaIngestionService",
                    context={"operation": "validate_downloaded_file", "file_path": file_path, "error": error_msg}
                )
                return False, error_msg
            
            # Check if file has content
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                error_msg = f"File is empty: {file_path}"
                await self.enhanced_logger.log_with_context(
                    level=LogLevel.ERROR,
                    message="File validation failed - empty file",
                    service="MediaIngestionService",
                    context={"operation": "validate_downloaded_file", "file_path": file_path, "file_size": file_size}
                )
                return False, error_msg
            
            # Check expected size if provided
            if expected_size is not None and file_size != expected_size:
                # Allow some tolerance for size differences (e.g., 5%)
                tolerance = 0.05
                size_diff = abs(file_size - expected_size) / expected_size
                if size_diff > tolerance:
                    error_msg = f"File size mismatch: expected {expected_size}, got {file_size}"
                    await self.enhanced_logger.log_with_context(
                        level=LogLevel.WARNING,
                        message="File validation warning - size mismatch",
                        service="MediaIngestionService",
                        context={"operation": "validate_downloaded_file", "file_path": file_path,
                            "expected_size": expected_size,
                            "actual_size": file_size,
                            "difference_percent": size_diff * 100}
                    )
            
            # Use MediaValidator for comprehensive validation if enabled
            if self._validation_enabled and media_type:
                validation_result = await self.media_validator.validate_media_file(
                    file_path, media_type, expected_size
                )
                
                if validation_result.status == ValidationStatus.INVALID:
                    error_msg = f"Media validation failed: {validation_result.error_message}"
                    await self.enhanced_logger.log_with_context(
                        level=LogLevel.ERROR,
                        message="File validation failed - invalid media",
                        service="MediaIngestionService",
                        context={"operation": "validate_downloaded_file", "file_path": file_path,
                            "media_type": media_type,
                            "validation_status": validation_result.status.value,
                            "error": validation_result.error_message}
                    )
                    return False, error_msg
                
                elif validation_result.status == ValidationStatus.CORRUPTED:
                    error_msg = f"Media file appears corrupted: {validation_result.error_message}"
                    await self.enhanced_logger.log_with_context(
                        level=LogLevel.WARNING,
                        message="File validation warning - corrupted media",
                        service="MediaIngestionService",
                        context={"operation": "validate_downloaded_file", "file_path": file_path,
                            "media_type": media_type,
                            "validation_status": validation_result.status.value,
                            "error": validation_result.error_message}
                    )
            
            # File is valid
            await self.enhanced_logger.log_with_context(
                level=LogLevel.DEBUG,
                message="File validation successful",
                service="MediaIngestionService",
                context={"operation": "validate_downloaded_file", "file_path": file_path,
                    "file_size": file_size,
                    "media_type": media_type}
            )
            return True, None
            
        except Exception as e:
            error_msg = f"Error validating file: {str(e)}"
            await self.enhanced_logger.log_with_context(
                level=LogLevel.ERROR,
                message="File validation error",
                service="MediaIngestionService",
                context={"operation": "validate_downloaded_file", "file_path": file_path, "error": str(e)},
                error=e
            )
            return False, error_msg
    
    async def handle_invalid_file(self, file_path: str, error_message: str, message_id: int, db: AsyncSession) -> None:
        """
        Handle an invalid downloaded file by logging, deleting, and marking as failed.
        
        Args:
            file_path: Path to the invalid file
            error_message: Description of why the file is invalid
            message_id: Database message ID
            db: Database session
        """
        try:
            # Log the invalid file
            await self.enhanced_logger.log_with_context(
                level=LogLevel.WARNING,
                message="Handling invalid downloaded file",
                service="MediaIngestionService",
                context={"operation": "handle_invalid_file", "file_path": file_path,
                    "message_id": message_id,
                    "error": error_message}
            )
            
            # Delete the invalid file
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    await self.enhanced_logger.log_with_context(
                        level=LogLevel.INFO,
                        message="Deleted invalid file",
                        service="MediaIngestionService",
                        context={"operation": "handle_invalid_file", "file_path": file_path}
                    )
                except Exception as e:
                    await self.enhanced_logger.log_with_context(
                        level=LogLevel.ERROR,
                        message="Failed to delete invalid file",
                        service="MediaIngestionService",
                        context={"operation": "handle_invalid_file", "file_path": file_path, "error": str(e)},
                        error=e
                    )
            
            # Mark as failed in database
            await db.execute(
                update(MediaFile)
                .where(MediaFile.message_id == message_id)
                .values(
                    processing_status="failed",
                    validation_status="invalid",
                    error_message=error_message,
                    last_download_attempt=datetime.utcnow()
                )
            )
            await db.commit()
            
            await self.enhanced_logger.log_with_context(
                level=LogLevel.INFO,
                message="Marked media file as failed in database",
                service="MediaIngestionService",
                context={"operation": "handle_invalid_file", "message_id": message_id}
            )
            
        except Exception as e:
            await self.enhanced_logger.log_with_context(
                level=LogLevel.ERROR,
                message="Error handling invalid file",
                service="MediaIngestionService",
                context={"operation": "handle_invalid_file", "file_path": file_path,
                    "message_id": message_id,
                    "error": str(e)},
                error=e
            )
    
    def set_queue_manager(self, queue_manager: DownloadQueueManager):
        """Sets the download queue manager for enhanced processing."""
        self.queue_manager = queue_manager
    
    def get_status(self) -> dict:
        """Returns comprehensive status information."""
        active_workers = sum(1 for t in self._download_tasks if not t.done())
        
        status = {
            "running": active_workers > 0,
            "active_workers": active_workers,
            "total_workers": self._num_workers,
            "queue_size": self._download_queue.qsize(),
            "known_hashes": len(self._known_hashes),
            "known_unique_ids": len(self._known_unique_ids),
            "processing_stats": self._processing_stats.copy(),
            "error_categories": self._error_categories.copy(),
            "session_recovery_active": self.session_recovery._is_monitoring,
            "queue_manager_active": self.queue_manager is not None and self.queue_manager._is_running if self.queue_manager else False
        }
        
        # Add queue manager stats if available
        if self.queue_manager:
            try:
                queue_stats = asyncio.create_task(self.queue_manager.get_queue_statistics())
                # Note: This is synchronous, so we can't await here
                # In practice, this would be called from an async context
                status["queue_manager_stats"] = "available"
            except Exception:
                status["queue_manager_stats"] = "error"
        
        return status
    
    async def get_detailed_status(self) -> dict:
        """Returns detailed async status information."""
        status = self.get_status()
        
        # Add queue manager stats if available
        if self.queue_manager:
            try:
                queue_stats = await self.queue_manager.get_queue_statistics()
                status["queue_manager_stats"] = {
                    "total_tasks": queue_stats.total_tasks,
                    "queued_tasks": queue_stats.queued_tasks,
                    "processing_tasks": queue_stats.processing_tasks,
                    "completed_tasks": queue_stats.completed_tasks,
                    "failed_tasks": queue_stats.failed_tasks,
                    "active_workers": queue_stats.active_workers,
                    "queue_status": queue_stats.queue_status.value
                }
            except Exception as e:
                status["queue_manager_stats"] = {"error": str(e)}
        
        return status
    
    async def start_workers(self):
        """Starts worker pool with enhanced error handling."""
        if not self._download_tasks:
            self.logger.info(f"Starting {self._num_workers} media ingestion workers")
            
            # Start session recovery monitoring
            await self.session_recovery.start_health_monitoring()
            
            for i in range(self._num_workers):
                task = asyncio.create_task(self._download_worker(i))
                self._download_tasks.append(task)
            
            self.logger.info("Media ingestion workers started successfully")
    
    async def stop_workers(self):
        """Stops worker pool with graceful shutdown."""
        self.logger.info("Stopping media ingestion workers...")
        
        # Stop session recovery monitoring
        await self.session_recovery.stop_health_monitoring()
        
        for task in self._download_tasks:
            task.cancel()
        
        if self._download_tasks:
            try:
                await asyncio.gather(*self._download_tasks, return_exceptions=True)
            except Exception as e:
                self.logger.error(f"Error stopping workers: {e}")
        
        self._download_tasks.clear()
        self.logger.info("Media ingestion workers stopped")
    
    async def queue_download(
        self, 
        client: TelegramClient, 
        message_id: int,
        media: any,
        group_id: int,
        priority: int = TaskPriority.NORMAL.value
    ):
        """Queues a download with enhanced priority support."""
        if self.queue_manager:
            # Use enhanced queue manager
            try:
                # Create download task
                # CRITICAL FIX: Get account_id from telegram_manager instead of client.session
                from backend.app.services.telegram_service import telegram_manager
                account_id = None
                for acc_id, cli in telegram_manager.clients.items():
                    if cli == client:
                        account_id = acc_id
                        break
                
                download_task = DownloadTask(
                    media_file_id=None,  # Will be set when media file is created
                    task_type="media_download",
                    priority=priority,
                    metadata={
                        "message_id": message_id,
                        "group_id": group_id,
                        "client_id": account_id or 'unknown'
                    }
                )
                
                # Enqueue with priority
                task_id = await self.queue_manager.enqueue_download(download_task, priority)
                self.logger.debug(f"Queued download task {task_id} with priority {priority}")
                
            except Exception as e:
                self.logger.error(f"Error queuing download with queue manager: {e}")
                # Fallback to legacy queue
                await self._download_queue.put((client, message_id, media, group_id))
        else:
            # Use legacy queue
            await self._download_queue.put((client, message_id, media, group_id))
    
    async def _download_worker(self, worker_id: int):
        """Enhanced download worker with comprehensive error handling."""
        self.logger.debug(f"Media ingestion worker {worker_id} started")
        
        while True:
            try:
                client, message_id, media, group_id = await self._download_queue.get()
                async with self._semaphore:
                    try:
                        await self._process_media_with_recovery(client, message_id, media, group_id, worker_id)
                    except FloodWaitError as e:
                        self.logger.warning(f"Worker {worker_id} FloodWait: {e.seconds}s")
                        self._error_categories["rate_limit_errors"] += 1
                        
                        # Use rate limiter for intelligent backoff
                        await self.rate_limiter.handle_flood_wait(e, OperationType.MEDIA_DOWNLOAD)
                        
                        # Re-queue the task
                        await self._download_queue.put((client, message_id, media, group_id))
                    except RPCError as e:
                        self.logger.error(f"Worker {worker_id} RPC error: {e}")
                        self._error_categories["network_errors"] += 1
                        
                        # CRITICAL FIX: Get account_id from telegram_manager instead of client.session
                        from backend.app.services.telegram_service import telegram_manager
                        account_id = None
                        for acc_id, cli in telegram_manager.clients.items():
                            if cli == client:
                                account_id = acc_id
                                break
                        
                        # Try session recovery
                        if account_id:
                            recovered_client = await self.session_recovery.handle_disconnection(account_id, e)
                            if recovered_client:
                                self._processing_stats["session_recoveries"] += 1
                                # Re-queue with recovered client
                                await self._download_queue.put((recovered_client, message_id, media, group_id))
                            else:
                                self._processing_stats["failed_downloads"] += 1
                        else:
                            self._processing_stats["failed_downloads"] += 1
                    except Exception as e:
                        self.logger.error(f"Worker {worker_id} unexpected error: {e}")
                        self._error_categories["unknown_errors"] += 1
                        self._processing_stats["failed_downloads"] += 1
                
                self._download_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Worker {worker_id} fatal error: {e}")
                await asyncio.sleep(1)
        
        self.logger.debug(f"Media ingestion worker {worker_id} stopped")
    
    async def _process_media_with_recovery(
        self, 
        client: TelegramClient, 
        message_id: int, 
        msg: any,
        group_id: int,
        worker_id: int
    ) -> Optional[MediaFile]:
        """Process media with session recovery and enhanced error handling."""
        try:
            # CRITICAL FIX: Get account_id from telegram_manager instead of client.session
            from backend.app.services.telegram_service import telegram_manager
            account_id = None
            for acc_id, cli in telegram_manager.clients.items():
                if cli == client:
                    account_id = acc_id
                    break
            
            # Ensure client session is active
            if account_id:
                active_client = await self.session_recovery.ensure_session_active(account_id)
                if not active_client:
                    self.logger.error(f"Could not ensure active session for account {account_id}")
                    return None
                client = active_client
            
            # Process the media
            return await self._process_media(client, message_id, msg, group_id)
            
        except Exception as e:
            self.logger.error(f"Error in media processing with recovery: {e}")
            return None
    
    async def process_message_media(
        self, 
        client: TelegramClient, 
        msg: any,
        db_message_id: int,
        group_id: int,
        validate_media: bool = True,
        detect_duplicates: bool = True
    ) -> Optional[MediaFile]:
        """
        Enhanced media processing with validation and duplicate detection.
        
        Args:
            client: Telegram client
            msg: Message object
            db_message_id: Database message ID
            group_id: Group ID
            validate_media: Whether to validate downloaded media
            detect_duplicates: Whether to detect duplicates
            
        Returns:
            MediaFile object if successful, None otherwise
        """
        if not msg.media:
            return None
        
        self._processing_stats["total_processed"] += 1
        
        async with async_session_maker() as db:
            try:
                result = await self._process_media_enhanced(
                    client, db_message_id, msg, group_id, db, validate_media, detect_duplicates
                )
                
                if result:
                    self._processing_stats["successful_downloads"] += 1
                else:
                    self._processing_stats["failed_downloads"] += 1
                
                return result
                
            except Exception as e:
                self.logger.error(f"Error processing message media: {e}")
                self._processing_stats["failed_downloads"] += 1
                return None
    
    async def _process_media_enhanced(
        self, 
        client: TelegramClient, 
        message_id: int, 
        msg: any,
        group_id: int,
        db: AsyncSession,
        validate_media: bool = True,
        detect_duplicates: bool = True
    ) -> Optional[MediaFile]:
        """
        Enhanced media processing with validation and duplicate detection.
        """
        try:
            media = msg.media if hasattr(msg, 'media') else msg
            
            # Classify media and extract information
            media_type, unique_id, file_info = self._classify_media(media, msg)
            if not media_type:
                self.logger.warning(f"Could not classify media for message {message_id}")
                return None
            
            # Check for existing media file
            existing = await db.execute(
                select(MediaFile).where(MediaFile.message_id == message_id).limit(1)
            )
            if existing.scalars().first():
                self.logger.debug(f"Media already exists for message {message_id}")
                return None
            
            # Enhanced duplicate detection
            if detect_duplicates and unique_id:
                duplicate_result = await self._check_for_duplicates(
                    db, unique_id, file_info, media_type
                )
                if duplicate_result:
                    self._processing_stats["duplicates_detected"] += 1
                    return duplicate_result
            
            # Download the file
            file_path, file_hash = await self._download_file_enhanced(
                client, msg, media_type, group_id
            )
            
            if not file_path:
                self.logger.error(f"Failed to download media for message {message_id}")
                return None
            
            # Validate the downloaded file
            validation_result = None
            if validate_media:
                validation_result = await self.media_validator.validate_media_file(
                    file_path, media_type, file_info.get('file_size')
                )
                
                if validation_result.status == ValidationStatus.INVALID:
                    self.logger.error(f"Media validation failed for {file_path}: {validation_result.error_message}")
                    self._processing_stats["validation_failures"] += 1
                    # Don't return None - still save the file but mark as invalid
                elif validation_result.status == ValidationStatus.CORRUPTED:
                    self.logger.warning(f"Media file appears corrupted: {file_path}")
                    self._processing_stats["validation_failures"] += 1
            
            # Check for hash-based duplicates after download
            if detect_duplicates and file_hash:
                hash_duplicate = await self._check_hash_duplicates(db, file_hash, message_id)
                if hash_duplicate:
                    self._processing_stats["duplicates_detected"] += 1
                    # Remove the downloaded file since it's a duplicate
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        self.logger.warning(f"Could not remove duplicate file {file_path}: {e}")
                    return hash_duplicate
            
            # Create media file record with enhanced fields
            media_file = await self._create_media_file_record(
                message_id, media_type, file_path, file_info, file_hash, 
                unique_id, validation_result, msg
            )
            
            # Use UPSERT to handle potential race conditions
            stmt = insert(MediaFile).values(**media_file.__dict__)
            stmt = stmt.on_conflict_do_update(
                index_elements=['message_id'],
                set_=dict(
                    file_path=stmt.excluded.file_path,
                    file_hash=stmt.excluded.file_hash,
                    download_attempts=MediaFile.download_attempts + 1,
                    last_download_attempt=datetime.utcnow(),
                    validation_status=stmt.excluded.validation_status
                )
            )
            
            result = await db.execute(stmt)
            await db.commit()
            
            # Update tracking sets
            if file_hash:
                self._known_hashes.add(file_hash)
            if unique_id:
                self._known_unique_ids.add(unique_id)
            
            live_stats.record("media_downloaded")
            self.logger.info(f"Successfully processed {media_type}: {file_path}")
            
            return media_file
            
        except Exception as e:
            self.logger.error(f"Error in enhanced media processing: {e}")
            await db.rollback()
            return None
    
    async def _check_for_duplicates(
        self, 
        db: AsyncSession, 
        unique_id: str, 
        file_info: Dict[str, Any], 
        media_type: str
    ) -> Optional[MediaFile]:
        """Check for duplicates based on unique_id."""
        if unique_id and unique_id in self._known_unique_ids:
            self.logger.debug(f"Skipping duplicate (unique_id): {unique_id}")
            return None
        
        if unique_id:
            existing_by_uid = await db.execute(
                select(MediaFile).where(MediaFile.unique_id == unique_id).limit(1)
            )
            existing_media = existing_by_uid.scalars().first()
            if existing_media:
                # Create duplicate reference
                dup_media = MediaFile(
                    telegram_id=file_info.get('telegram_id'),
                    message_id=0,  # Will be set by caller
                    file_type=media_type,
                    unique_id=unique_id,
                    is_duplicate=True,
                    original_media_id=existing_media.id,
                    duplicate_detection_method="unique_id"
                )
                return dup_media
        
        return None
    
    async def _check_hash_duplicates(
        self, 
        db: AsyncSession, 
        file_hash: str, 
        message_id: int
    ) -> Optional[MediaFile]:
        """Check for duplicates based on file hash."""
        if file_hash in self._known_hashes:
            self.logger.debug(f"Skipping duplicate (hash): {file_hash[:16]}...")
            return None
        
        existing_by_hash = await db.execute(
            select(MediaFile).where(MediaFile.file_hash == file_hash).limit(1)
        )
        existing_media = existing_by_hash.scalars().first()
        if existing_media:
            # Create duplicate reference
            dup_media = MediaFile(
                message_id=message_id,
                file_type=existing_media.file_type,
                file_hash=file_hash,
                is_duplicate=True,
                original_media_id=existing_media.id,
                duplicate_detection_method="file_hash"
            )
            return dup_media
        
        return None
    
    async def _create_media_file_record(
        self,
        message_id: int,
        media_type: str,
        file_path: str,
        file_info: Dict[str, Any],
        file_hash: Optional[str],
        unique_id: Optional[str],
        validation_result: Optional[Any],
        msg: Any
    ) -> MediaFile:
        """Create enhanced MediaFile record with all new fields."""
        
        # Determine validation status
        validation_status = "pending"
        if validation_result:
            validation_status = validation_result.status.value
        
        # Extract perceptual hash if available
        perceptual_hash = None
        if validation_result and validation_result.perceptual_hash:
            perceptual_hash = validation_result.perceptual_hash
        
        # Determine processing priority based on media type
        processing_priority = 2  # Normal priority
        if media_type == "document":
            processing_priority = 1  # High priority for documents (high failure rate)
        elif media_type in ["photo", "video"]:
            processing_priority = 2  # Normal priority
        else:
            processing_priority = 3  # Lower priority for other types
        
        media_file = MediaFile(
            telegram_id=file_info.get('telegram_id'),
            message_id=message_id,
            file_type=media_type,
            file_path=file_path,
            file_name=file_info.get('file_name'),
            file_size=file_info.get('file_size'),
            mime_type=file_info.get('mime_type'),
            width=file_info.get('width'),
            height=file_info.get('height'),
            duration=file_info.get('duration'),
            file_hash=file_hash,
            unique_id=unique_id,
            is_self_destructing=getattr(msg, 'ttl_seconds', None) is not None if hasattr(msg, 'ttl_seconds') else False,
            ttl_seconds=getattr(msg, 'ttl_seconds', None) if hasattr(msg, 'ttl_seconds') else None,
            ocr_status="pending" if media_type == "photo" else "skipped",
            
            # Enhanced fields
            download_attempts=1,
            last_download_attempt=datetime.utcnow(),
            validation_status=validation_status,
            processing_status="completed",
            processing_priority=processing_priority,
            perceptual_hash=perceptual_hash,
            duplicate_detection_method=None,  # Set only for duplicates
            metadata_extracted=validation_result.metadata if validation_result else None
        )
        
        return media_file
    
    async def _download_file_enhanced(
        self, 
        client: TelegramClient, 
        msg: any,
        media_type: str,
        group_id: int
    ) -> Tuple[Optional[str], Optional[str]]:
        """Enhanced file download with better error handling and validation."""
        try:
            subdir_map = {
                "photo": "photos",
                "video": "videos",
                "document": "documents",
                "audio": "audio",
                "voice": "voice",
                "sticker": "stickers",
                "gif": "videos",
                "video_note": "videos"
            }
            
            subdir = subdir_map.get(media_type, "documents")
            target_dir = os.path.join(self.media_dir, subdir, str(group_id))
            os.makedirs(target_dir, exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            msg_id = msg.id if hasattr(msg, 'id') else "unknown"
            
            ext_map = {
                "photo": ".jpg",
                "video": ".mp4",
                "voice": ".ogg",
                "audio": ".mp3",
                "sticker": ".webp",
                "gif": ".mp4",
                "video_note": ".mp4"
            }
            ext = ext_map.get(media_type, "")
            
            filename = f"{timestamp}_{msg_id}{ext}"
            file_path = os.path.join(target_dir, filename)
            
            # Determine operation type based on media type
            operation_type = OperationType.MEDIA_DOWNLOAD
            if media_type == "photo":
                operation_type = OperationType.PROFILE_PHOTO
            elif media_type in ["video", "gif", "video_note"]:
                operation_type = OperationType.MEDIA_DOWNLOAD
            elif media_type in ["audio", "voice"]:
                operation_type = OperationType.MEDIA_DOWNLOAD
            elif media_type == "document":
                operation_type = OperationType.MEDIA_DOWNLOAD
            
            # Download with timeout and retry logic using enhanced rate limiter
            max_retries = self._max_retries
            for attempt in range(max_retries):
                try:
                    # Use enhanced rate limiter with operation-specific handling
                    # Using correct parameter name 'message' instead of 'msg'
                    result = await self.rate_limiter.execute_with_rate_limit(
                        client.download_media,
                        operation_type=operation_type,
                        message=msg,
                        file=file_path
                    )
                    
                    # Verify file was downloaded and has content
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                        # Validate the downloaded file
                        is_valid, error_msg = await self.validate_downloaded_file(
                            file_path, 
                            expected_size=None,  # We don't have expected size here
                            media_type=media_type
                        )
                        
                        if not is_valid:
                            await self.enhanced_logger.log_with_context(
                                level=LogLevel.WARNING,
                                message=f"Downloaded file failed validation on attempt {attempt + 1}",
                                service="MediaIngestionService",
                                context={"operation": "_download_file_enhanced", "file_path": file_path,
                                    "attempt": attempt + 1,
                                    "error": error_msg}
                            )
                            # Remove invalid file
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        
                        # File is valid, compute hash and return
                        file_hash = await self._compute_hash(file_path)
                        await self.enhanced_logger.log_with_context(
                            level=LogLevel.DEBUG,
                            message="Successfully downloaded and validated media file",
                            service="MediaIngestionService",
                            context={"operation": "_download_file_enhanced", "media_type": media_type,
                                "file_path": file_path,
                                "file_size": os.path.getsize(file_path),
                                "attempt": attempt + 1}
                        )
                        return file_path, file_hash
                    else:
                        await self.enhanced_logger.log_with_context(
                            level=LogLevel.WARNING,
                            message=f"Download attempt {attempt + 1} resulted in empty file",
                            service="MediaIngestionService",
                            context={"operation": "_download_file_enhanced", "file_path": file_path, "attempt": attempt + 1}
                        )
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        
                except asyncio.TimeoutError:
                    await self.enhanced_logger.log_with_context(
                        level=LogLevel.WARNING,
                        message=f"Download timeout on attempt {attempt + 1}",
                        service="MediaIngestionService",
                        context={"operation": "_download_file_enhanced", "file_path": file_path,
                            "attempt": attempt + 1,
                            "timeout": self._download_timeout}
                    )
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        
                except Exception as e:
                    await self.enhanced_logger.log_with_context(
                        level=LogLevel.ERROR,
                        message=f"Download error on attempt {attempt + 1}",
                        service="MediaIngestionService",
                        context={"operation": "_download_file_enhanced", "file_path": file_path,
                            "attempt": attempt + 1,
                            "error": str(e)},
                        error=e
                    )
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        raise
            
            await self.enhanced_logger.log_with_context(
                level=LogLevel.ERROR,
                message=f"Failed to download after {max_retries} attempts",
                service="MediaIngestionService",
                context={"operation": "_download_file_enhanced", "file_path": file_path, "max_retries": max_retries}
            )
            return None, None
            
        except Exception as e:
            self.logger.error(f"Enhanced download error: {e}")
            self._error_categories["file_system_errors"] += 1
            return None, None
    
    async def _process_media(
        self, 
        client: TelegramClient, 
        message_id: int, 
        msg: any,
        group_id: int,
        db: Optional[AsyncSession] = None
    ) -> Optional[MediaFile]:
        own_session = db is None
        if own_session:
            db = async_session_maker()
            await db.__aenter__()
        
        try:
            media = msg.media if hasattr(msg, 'media') else msg
            
            media_type, unique_id, file_info = self._classify_media(media, msg)
            if not media_type:
                return None
            
            if unique_id and unique_id in self._known_unique_ids:
                await self.enhanced_logger.log_with_context(
                    level=LogLevel.DEBUG,
                    message="Skipping duplicate media (unique_id)",
                    service="MediaIngestionService",
                    context={"operation": "_process_media", "unique_id": unique_id, "message_id": message_id}
                )
                return None
            
            existing = await db.execute(
                select(MediaFile).where(MediaFile.message_id == message_id).limit(1)
            )
            if existing.scalars().first():
                return None
            
            if unique_id:
                existing_by_uid = await db.execute(
                    select(MediaFile).where(MediaFile.unique_id == unique_id).limit(1)
                )
                existing_media = existing_by_uid.scalars().first()
                if existing_media:
                    dup_media = MediaFile(
                        telegram_id=file_info.get('telegram_id'),
                        message_id=message_id,
                        file_type=media_type,
                        unique_id=unique_id,
                        is_duplicate=True,
                        original_media_id=existing_media.id
                    )
                    db.add(dup_media)
                    if own_session:
                        await db.commit()
                    return dup_media
            
            file_path, file_hash = await self._download_file(client, msg, media_type, group_id)
            
            if file_hash and file_hash in self._known_hashes:
                await self.enhanced_logger.log_with_context(
                    level=LogLevel.DEBUG,
                    message="Skipping duplicate media (hash)",
                    service="MediaIngestionService",
                    context={"operation": "_process_media", "file_hash": file_hash[:16], "message_id": message_id}
                )
                return None
            
            if file_hash:
                existing_by_hash = await db.execute(
                    select(MediaFile).where(MediaFile.file_hash == file_hash).limit(1)
                )
                existing_media = existing_by_hash.scalars().first()
                if existing_media:
                    dup_media = MediaFile(
                        telegram_id=file_info.get('telegram_id'),
                        message_id=message_id,
                        file_type=media_type,
                        file_hash=file_hash,
                        unique_id=unique_id,
                        is_duplicate=True,
                        original_media_id=existing_media.id
                    )
                    db.add(dup_media)
                    if own_session:
                        await db.commit()
                    return dup_media
            
            media_file = MediaFile(
                telegram_id=file_info.get('telegram_id'),
                message_id=message_id,
                file_type=media_type,
                file_path=file_path,
                file_name=file_info.get('file_name'),
                file_size=file_info.get('file_size'),
                mime_type=file_info.get('mime_type'),
                width=file_info.get('width'),
                height=file_info.get('height'),
                duration=file_info.get('duration'),
                file_hash=file_hash,
                unique_id=unique_id,
                is_self_destructing=getattr(msg, 'ttl_seconds', None) is not None if hasattr(msg, 'ttl_seconds') else False,
                ttl_seconds=getattr(msg, 'ttl_seconds', None) if hasattr(msg, 'ttl_seconds') else None,
                ocr_status="pending" if media_type == "photo" else "skipped"
            )
            
            db.add(media_file)
            
            if file_hash:
                self._known_hashes.add(file_hash)
            if unique_id:
                self._known_unique_ids.add(unique_id)
            
            if own_session:
                await db.commit()
            
            live_stats.record("media_downloaded")
            await self.enhanced_logger.log_with_context(
                level=LogLevel.INFO,
                message="Successfully saved media file",
                service="MediaIngestionService",
                context={"operation": "_process_media", "media_type": media_type,
                    "file_path": file_path,
                    "message_id": message_id,
                    "file_size": file_info.get('file_size'),
                    "file_hash": file_hash[:16] if file_hash else None}
            )
            return media_file
        
        except Exception as e:
            await self.enhanced_logger.log_with_context(
                level=LogLevel.ERROR,
                message="Error processing media",
                service="MediaIngestionService",
                context={"operation": "_process_media", "message_id": message_id, "error": str(e)},
                error=e
            )
            if own_session:
                await db.rollback()
            return None
        finally:
            if own_session:
                await db.__aexit__(None, None, None)
    
    def _classify_media(self, media: any, msg: any) -> Tuple[Optional[str], Optional[str], dict]:
        file_info = {}
        unique_id = None
        media_type = None
        
        if isinstance(media, MessageMediaPhoto):
            media_type = "photo"
            if media.photo:
                photo = media.photo
                file_info['telegram_id'] = photo.id
                unique_id = f"photo_{photo.id}_{photo.access_hash}"
                if photo.sizes:
                    largest = max(photo.sizes, key=lambda s: getattr(s, 'size', 0) if hasattr(s, 'size') else 0)
                    file_info['width'] = getattr(largest, 'w', None)
                    file_info['height'] = getattr(largest, 'h', None)
        
        elif isinstance(media, MessageMediaDocument):
            doc = media.document
            if doc:
                file_info['telegram_id'] = doc.id
                file_info['file_size'] = doc.size
                file_info['mime_type'] = doc.mime_type
                unique_id = f"doc_{doc.id}_{doc.access_hash}"
                
                for attr in doc.attributes:
                    attr_type = type(attr).__name__
                    
                    if attr_type == 'DocumentAttributeFilename':
                        file_info['file_name'] = attr.file_name
                    elif attr_type == 'DocumentAttributeVideo':
                        if getattr(attr, 'round_message', False):
                            media_type = "video_note"
                        else:
                            media_type = "video"
                        file_info['width'] = attr.w
                        file_info['height'] = attr.h
                        file_info['duration'] = attr.duration
                    elif attr_type == 'DocumentAttributeAudio':
                        media_type = "voice" if getattr(attr, 'voice', False) else "audio"
                        file_info['duration'] = attr.duration
                    elif attr_type == 'DocumentAttributeSticker':
                        media_type = "sticker"
                    elif attr_type == 'DocumentAttributeAnimated':
                        media_type = "gif"
                
                if not media_type:
                    mime = doc.mime_type or ""
                    if mime.startswith("video/"):
                        media_type = "video"
                    elif mime.startswith("audio/"):
                        media_type = "audio"
                    elif mime.startswith("image/"):
                        media_type = "photo"
                    else:
                        media_type = "document"
        
        return media_type, unique_id, file_info
    
    async def _download_file(
        self, 
        client: TelegramClient, 
        msg: any,
        media_type: str,
        group_id: int
    ) -> Tuple[Optional[str], Optional[str]]:
        try:
            subdir_map = {
                "photo": "photos",
                "video": "videos",
                "document": "documents",
                "audio": "audio",
                "voice": "voice",
                "sticker": "stickers",
                "gif": "videos",
                "video_note": "videos"
            }
            
            subdir = subdir_map.get(media_type, "documents")
            target_dir = os.path.join(self.media_dir, subdir, str(group_id))
            os.makedirs(target_dir, exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            msg_id = msg.id if hasattr(msg, 'id') else "unknown"
            
            ext_map = {
                "photo": ".jpg",
                "video": ".mp4",
                "voice": ".ogg",
                "audio": ".mp3",
                "sticker": ".webp",
                "gif": ".mp4",
                "video_note": ".mp4"
            }
            ext = ext_map.get(media_type, "")
            
            filename = f"{timestamp}_{msg_id}{ext}"
            file_path = os.path.join(target_dir, filename)
            
            # Determine operation type based on media type
            operation_type = OperationType.MEDIA_DOWNLOAD
            if media_type == "photo":
                operation_type = OperationType.PROFILE_PHOTO
            elif media_type in ["video", "gif", "video_note"]:
                operation_type = OperationType.MEDIA_DOWNLOAD
            elif media_type in ["audio", "voice"]:
                operation_type = OperationType.MEDIA_DOWNLOAD
            elif media_type == "document":
                operation_type = OperationType.MEDIA_DOWNLOAD
            
            # Use enhanced rate limiter for download
            # Using correct parameter name 'message' instead of 'msg'
            await self.rate_limiter.execute_with_rate_limit(
                client.download_media,
                operation_type=operation_type,
                message=msg,
                file=file_path
            )
            
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                file_hash = await self._compute_hash(file_path)
                return file_path, file_hash
            
            return None, None
        
        except Exception as e:
            self.logger.error(f"Legacy download error: {e}")
            return None, None
    
    async def _compute_hash(self, file_path: str) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._compute_hash_sync, file_path)
    
    def _compute_hash_sync(self, file_path: str) -> str:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                hasher.update(chunk)
        return hasher.hexdigest()


media_ingestion = MediaIngestionService()
