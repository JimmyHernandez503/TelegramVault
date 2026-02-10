import asyncio
import os
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from enum import Enum
from sqlalchemy import select, update, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from backend.app.db.database import async_session_maker
from backend.app.models.media import MediaFile
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_account import TelegramAccount
from backend.app.models.download_task import DownloadTask, BatchProcessing
from backend.app.services.telegram_service import telegram_manager
from backend.app.services.client_load_balancer import load_balancer
from backend.app.core.session_recovery_manager import SessionRecoveryManager
from backend.app.core.media_validator import MediaValidator, ValidationStatus
from backend.app.core.download_queue_manager import DownloadQueueManager, TaskPriority
from backend.app.core.api_rate_limiter import APIRateLimiter, OperationType
from backend.app.core.config_manager import config_manager
from backend.app.core.enhanced_logging_system import enhanced_logging


class ErrorCategory(Enum):
    """Error categories for media retry operations."""
    NETWORK_ERRORS = "network_errors"
    AUTHORIZATION_ERRORS = "authorization_errors"
    RATE_LIMIT_ERRORS = "rate_limit_errors"
    VALIDATION_ERRORS = "validation_errors"
    FILE_SYSTEM_ERRORS = "file_system_errors"
    MEDIA_NOT_FOUND = "media_not_found"
    UNKNOWN_ERRORS = "unknown_errors"


class MediaRetryService:
    def __init__(self):
        # Use enhanced logging system instead of standard logger
        self.logger = enhanced_logging
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        # Enhanced components
        self.session_recovery = SessionRecoveryManager()
        self.media_validator = MediaValidator()
        self.rate_limiter = APIRateLimiter()
        
        # Queue management (optional - can be injected)
        self.queue_manager: Optional[DownloadQueueManager] = None
        
        # Load settings from ConfigManager instead of hardcoded values
        self._settings = {
            "enabled": False,
            "interval_minutes": config_manager.get_int("MEDIA_RETRY_INTERVAL_MINUTES", 5),
            "batch_size": config_manager.get_int("MEDIA_RETRY_BATCH_SIZE", 100),
            "max_retries": config_manager.get_int("MEDIA_RETRY_MAX_ATTEMPTS", 3),
            "parallel_downloads": config_manager.get_int("MEDIA_RETRY_PARALLEL_DOWNLOADS", 10),
            "retry_delay_base": config_manager.get_int("MEDIA_RETRY_DELAY_BASE", 2),
            "exponential_backoff": config_manager.get_bool("MEDIA_RETRY_EXPONENTIAL_BACKOFF", True),
            "jitter_enabled": config_manager.get_bool("MEDIA_RETRY_JITTER_ENABLED", True),
            "validate_downloads": config_manager.get_bool("MEDIA_VALIDATION_ENABLED", True),
            "categorize_errors": True,
            "session_recovery_enabled": True
        }
        
        # Enhanced statistics with proper error categories
        self._stats = {
            "total_retried": 0,
            "successful": 0,
            "failed": 0,
            "last_run": None,
            "pending_count": 0,
            "session_recoveries": 0,
            "validation_failures": 0,
            "error_categories": {
                ErrorCategory.NETWORK_ERRORS.value: 0,
                ErrorCategory.AUTHORIZATION_ERRORS.value: 0,
                ErrorCategory.RATE_LIMIT_ERRORS.value: 0,
                ErrorCategory.VALIDATION_ERRORS.value: 0,
                ErrorCategory.FILE_SYSTEM_ERRORS.value: 0,
                ErrorCategory.MEDIA_NOT_FOUND.value: 0,
                ErrorCategory.UNKNOWN_ERRORS.value: 0
            },
            "retry_by_attempt": {
                "1": 0,
                "2": 0,
                "3": 0,
                "3+": 0
            }
        }
        
        self._semaphore = asyncio.Semaphore(self._settings["parallel_downloads"])
        
        # Batch processing
        self._batch_processor: Optional['BatchRetryProcessor'] = None
    
    def categorize_error(self, error: Exception) -> ErrorCategory:
        """
        Categorize error into one of the defined error categories.
        
        Args:
            error: Exception to categorize
            
        Returns:
            ErrorCategory: The category of the error
        """
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()
        
        # Check for rate limit errors
        if "floodwaiterror" in error_type or "rate limit" in error_str or "flood" in error_str:
            return ErrorCategory.RATE_LIMIT_ERRORS
        
        # Check for authorization errors
        if any(keyword in error_type or keyword in error_str for keyword in 
               ["unauthorized", "auth", "permission", "forbidden", "access denied"]):
            return ErrorCategory.AUTHORIZATION_ERRORS
        
        # Check for network errors
        if any(keyword in error_type or keyword in error_str for keyword in 
               ["network", "connection", "timeout", "connectionerror", "timeouterror"]):
            return ErrorCategory.NETWORK_ERRORS
        
        # Check for file system errors
        if any(keyword in error_type or keyword in error_str for keyword in 
               ["file", "path", "disk", "ioerror", "oserror", "permission"]):
            return ErrorCategory.FILE_SYSTEM_ERRORS
        
        # Check for validation errors
        if any(keyword in error_type or keyword in error_str for keyword in 
               ["validation", "corrupt", "invalid", "validationerror"]):
            return ErrorCategory.VALIDATION_ERRORS
        
        # Check for media not found
        if any(keyword in error_str for keyword in 
               ["not found", "no longer exists", "deleted", "unavailable"]):
            return ErrorCategory.MEDIA_NOT_FOUND
        
        # Default to unknown
        return ErrorCategory.UNKNOWN_ERRORS
    
    def set_queue_manager(self, queue_manager: DownloadQueueManager):
        """Sets the download queue manager for enhanced processing."""
        self.queue_manager = queue_manager
    
    def set_batch_processor(self, batch_processor: 'BatchRetryProcessor'):
        """Sets the batch retry processor."""
        self._batch_processor = batch_processor
    
    def get_status(self) -> dict:
        """Returns comprehensive status information."""
        status = {
            "running": self._running,
            "settings": self._settings.copy(),
            "stats": self._stats.copy(),
            "session_recovery_active": self.session_recovery._is_monitoring,
            "queue_manager_active": self.queue_manager is not None and self.queue_manager._is_running if self.queue_manager else False,
            "batch_processor_active": self._batch_processor is not None and self._batch_processor._is_running if self._batch_processor else False
        }
        
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
                    "failed_tasks": queue_stats.failed_tasks
                }
            except Exception as e:
                status["queue_manager_stats"] = {"error": str(e)}
        
        return status
    
    def update_settings(self, **kwargs):
        """Updates service settings with validation."""
        for key, value in kwargs.items():
            if key in self._settings:
                self._settings[key] = value
                
                # Update dependent components
                if key == "parallel_downloads":
                    self._semaphore = asyncio.Semaphore(value)
                elif key == "session_recovery_enabled" and not value:
                    # Stop session recovery if disabled
                    asyncio.create_task(self.session_recovery.stop_health_monitoring())
        
        self.logger.info(f"Updated settings: {kwargs}")
    
    async def start(self):
        """Starts the retry service with enhanced monitoring."""
        if self._running:
            return
        
        await self.logger.log_with_context(
            "INFO",
            "Starting media retry service with enhanced features",
            "MediaRetryService",
            context={
                "settings": self._settings,
                "config_source": "ConfigManager"
            }
        )
        
        self._running = True
        
        # Start session recovery monitoring if enabled
        if self._settings["session_recovery_enabled"]:
            await self.session_recovery.start_health_monitoring()
        
        # Start the retry loop
        self._task = asyncio.create_task(self._retry_loop())
        
        await self.logger.log_with_context(
            "INFO",
            "Media retry service started successfully",
            "MediaRetryService"
        )
    
    async def stop(self):
        """Stops the retry service with graceful shutdown."""
        if not self._running:
            return
        
        await self.logger.log_with_context(
            "INFO",
            "Stopping media retry service...",
            "MediaRetryService"
        )
        
        self._running = False
        
        # Stop session recovery monitoring
        if self._settings["session_recovery_enabled"]:
            await self.session_recovery.stop_health_monitoring()
        
        # Stop the retry task
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        await self.logger.log_with_context(
            "INFO",
            "Media retry service stopped",
            "MediaRetryService"
        )
    
    async def get_pending_count(self) -> int:
        """Gets count of pending media files with enhanced filtering."""
        async with async_session_maker() as db:
            result = await db.execute(
                select(func.count(MediaFile.id)).where(
                    and_(
                        or_(
                            MediaFile.file_path.is_(None),
                            MediaFile.validation_status == "invalid",
                            MediaFile.validation_status == "corrupted"
                        ),
                        MediaFile.is_duplicate == False,
                        MediaFile.download_attempts < self._settings["max_retries"]
                    )
                )
            )
            return result.scalar() or 0
    
    async def get_pending_by_category(self) -> Dict[str, int]:
        """Gets pending count by error category."""
        async with async_session_maker() as db:
            # Count by different failure reasons
            categories = {
                "no_file_path": 0,
                "validation_failed": 0,
                "corrupted": 0,
                "network_errors": 0,
                "authorization_errors": 0,
                "rate_limit_errors": 0,
                "unknown_errors": 0
            }
            
            # No file path (never downloaded)
            result = await db.execute(
                select(func.count(MediaFile.id)).where(
                    and_(
                        MediaFile.file_path.is_(None),
                        MediaFile.is_duplicate == False,
                        MediaFile.download_attempts < self._settings["max_retries"]
                    )
                )
            )
            categories["no_file_path"] = result.scalar() or 0
            
            # Validation failures
            result = await db.execute(
                select(func.count(MediaFile.id)).where(
                    and_(
                        MediaFile.validation_status == "invalid",
                        MediaFile.is_duplicate == False,
                        MediaFile.download_attempts < self._settings["max_retries"]
                    )
                )
            )
            categories["validation_failed"] = result.scalar() or 0
            
            # Corrupted files
            result = await db.execute(
                select(func.count(MediaFile.id)).where(
                    and_(
                        MediaFile.validation_status == "corrupted",
                        MediaFile.is_duplicate == False,
                        MediaFile.download_attempts < self._settings["max_retries"]
                    )
                )
            )
            categories["corrupted"] = result.scalar() or 0
            
            return categories
    
    async def retry_now(self) -> dict:
        """Performs immediate retry with enhanced reporting."""
        await self.logger.log_with_context(
            "INFO",
            "Starting immediate retry batch",
            "MediaRetryService"
        )
        
        start_time = datetime.utcnow()
        count = await self.process_retry_batch()
        end_time = datetime.utcnow()
        
        processing_time = (end_time - start_time).total_seconds()
        
        result = {
            "processed": count,
            "processing_time_seconds": processing_time,
            "success_rate": self._calculate_recent_success_rate(),
            "pending_remaining": await self.get_pending_count(),
            "categories_processed": await self.get_pending_by_category()
        }
        
        await self.logger.log_with_context(
            "INFO",
            "Immediate retry completed",
            "MediaRetryService",
            context=result
        )
        return result
    
    async def process_retry_batch(self) -> int:
        """
        Process a batch of failed downloads with aggregated metrics.
        
        Returns:
            int: Number of media files processed
        """
        # Start operation tracking
        op_id = await self.logger.log_operation_start(
            "process_retry_batch",
            "MediaRetryService",
            context={"batch_size": self._settings["batch_size"]}
        )
        
        try:
            batch_start = datetime.utcnow()
            count = await self._retry_batch()
            batch_end = datetime.utcnow()
            
            # Calculate aggregated metrics
            processing_time = (batch_end - batch_start).total_seconds()
            
            metrics = {
                "processed_count": count,
                "processing_time_seconds": processing_time,
                "successful_count": self._stats["successful"],
                "failed_count": self._stats["failed"],
                "success_rate": self._calculate_recent_success_rate(),
                "error_categories": self._stats["error_categories"].copy(),
                "retry_attempts": self._stats["retry_by_attempt"].copy(),
                "validation_failures": self._stats["validation_failures"],
                "session_recoveries": self._stats["session_recoveries"]
            }
            
            # Log aggregated metrics
            await self.logger.log_metrics("MediaRetryService", metrics)
            
            # End operation tracking
            await self.logger.log_operation_end(op_id, success=True, context=metrics)
            
            return count
            
        except Exception as e:
            await self.logger.log_operation_end(op_id, success=False, error=e)
            raise
    
    async def process_batch_with_progress(self, total_items: Optional[int] = None) -> dict:
        """
        Process pending media with progress logging every 100 items.
        
        Args:
            total_items: Optional total number of items to process. If None, processes all pending.
            
        Returns:
            dict: Processing statistics including counts and success rate
        """
        await self.logger.log_with_context(
            "INFO",
            "Starting batch processing with progress tracking",
            "MediaRetryService",
            context={"total_items": total_items or "all"}
        )
        
        processed = 0
        successful = 0
        failed = 0
        start_time = datetime.utcnow()
        
        # Get initial pending count
        pending_count = await self.get_pending_count()
        items_to_process = total_items or pending_count
        
        await self.logger.log_with_context(
            "INFO",
            f"Found {pending_count} pending items",
            "MediaRetryService",
            context={"pending_count": pending_count, "items_to_process": items_to_process}
        )
        
        while processed < items_to_process:
            # Get batch of pending items
            async with async_session_maker() as db:
                result = await db.execute(
                    select(MediaFile)
                    .where(
                        and_(
                            or_(
                                MediaFile.file_path.is_(None),
                                MediaFile.validation_status == "invalid",
                                MediaFile.validation_status == "corrupted"
                            ),
                            MediaFile.is_duplicate == False,
                            MediaFile.download_attempts < self._settings["max_retries"]
                        )
                    )
                    .order_by(
                        MediaFile.processing_priority.asc(),
                        MediaFile.download_attempts.asc(),
                        MediaFile.created_at.desc()
                    )
                    .limit(min(self._settings["batch_size"], items_to_process - processed))
                )
                batch_items = result.scalars().all()
            
            if not batch_items:
                await self.logger.log_with_context(
                    "INFO",
                    "No more pending items to process",
                    "MediaRetryService"
                )
                break
            
            # Process batch items in parallel
            tasks = [self.retry_single_media(item.id) for item in batch_items]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count results
            batch_successful = sum(1 for r in results if r is True)
            batch_failed = sum(1 for r in results if r is False or isinstance(r, Exception))
            
            processed += len(batch_items)
            successful += batch_successful
            failed += batch_failed
            
            # Log progress every 100 items
            if processed % 100 == 0 or processed >= items_to_process:
                elapsed_time = (datetime.utcnow() - start_time).total_seconds()
                items_per_second = processed / elapsed_time if elapsed_time > 0 else 0
                remaining = items_to_process - processed
                eta_seconds = remaining / items_per_second if items_per_second > 0 else 0
                
                await self.logger.log_with_context(
                    "INFO",
                    f"Progress: {processed}/{items_to_process} items processed ({processed/items_to_process*100:.1f}%)",
                    "MediaRetryService",
                    context={
                        "processed": processed,
                        "total": items_to_process,
                        "successful": successful,
                        "failed": failed,
                        "success_rate": (successful / processed * 100) if processed > 0 else 0,
                        "items_per_second": round(items_per_second, 2),
                        "eta_seconds": round(eta_seconds, 0),
                        "elapsed_seconds": round(elapsed_time, 2)
                    }
                )
        
        # Final summary
        end_time = datetime.utcnow()
        total_time = (end_time - start_time).total_seconds()
        
        summary = {
            "processed": processed,
            "successful": successful,
            "failed": failed,
            "success_rate": (successful / processed * 100) if processed > 0 else 0,
            "total_time_seconds": total_time,
            "items_per_second": processed / total_time if total_time > 0 else 0,
            "pending_remaining": await self.get_pending_count()
        }
        
        await self.logger.log_with_context(
            "INFO",
            "Batch processing with progress completed",
            "MediaRetryService",
            context=summary
        )
        
        return summary
    
    def _calculate_recent_success_rate(self) -> float:
        """Calculates recent success rate."""
        total_recent = self._stats["successful"] + self._stats["failed"]
        if total_recent == 0:
            return 0.0
        return (self._stats["successful"] / total_recent) * 100
    
    async def _retry_loop(self):
        """Enhanced retry loop with intelligent scheduling."""
        await self.logger.log_with_context(
            "INFO",
            "Media retry loop started",
            "MediaRetryService"
        )
        
        while self._running:
            try:
                if self._settings["enabled"]:
                    await self.process_retry_batch()
                
                # Adaptive interval based on pending count
                pending_count = await self.get_pending_count()
                if pending_count > 1000:
                    # More frequent retries for large backlogs
                    interval = max(self._settings["interval_minutes"] // 2, 5)
                elif pending_count > 100:
                    interval = self._settings["interval_minutes"]
                else:
                    # Less frequent retries for small backlogs
                    interval = self._settings["interval_minutes"] * 2
                
                await self.logger.log_with_context(
                    "DEBUG",
                    f"Next retry in {interval} minutes",
                    "MediaRetryService",
                    context={"pending_count": pending_count, "interval_minutes": interval}
                )
                await asyncio.sleep(interval * 60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.logger.log_with_context(
                    "ERROR",
                    "Error in retry loop",
                    "MediaRetryService",
                    error=e
                )
                await asyncio.sleep(60)
        
        await self.logger.log_with_context(
            "INFO",
            "Media retry loop stopped",
            "MediaRetryService"
        )
    
    async def _retry_batch(self) -> int:
        """Enhanced batch retry with intelligent error categorization."""
        self._stats["last_run"] = datetime.utcnow().isoformat()
        
        async with async_session_maker() as db:
            # Get pending media with priority ordering
            result = await db.execute(
                select(MediaFile)
                .where(
                    and_(
                        or_(
                            MediaFile.file_path.is_(None),
                            MediaFile.validation_status == "invalid",
                            MediaFile.validation_status == "corrupted"
                        ),
                        MediaFile.is_duplicate == False,
                        MediaFile.download_attempts < self._settings["max_retries"]
                    )
                )
                .order_by(
                    MediaFile.processing_priority.asc(),  # Higher priority first
                    MediaFile.download_attempts.asc(),    # Fewer attempts first
                    MediaFile.created_at.desc()           # Newer files first
                )
                .limit(self._settings["batch_size"])
            )
            pending_media = result.scalars().all()
            
            if not pending_media:
                await self.logger.log_with_context(
                    "DEBUG",
                    "No pending media to retry",
                    "MediaRetryService"
                )
                return 0
            
            await self.logger.log_with_context(
                "INFO",
                f"Processing {len(pending_media)} pending media files",
                "MediaRetryService",
                context={"batch_size": len(pending_media)}
            )
            
            # Process with enhanced queue management
            if self.queue_manager:
                return await self._process_batch_with_queue(pending_media)
            else:
                return await self._process_batch_legacy(pending_media)
    
    async def _process_batch_with_queue(self, pending_media: List[MediaFile]) -> int:
        """Process batch using enhanced queue manager."""
        try:
            processed_count = 0
            
            for media in pending_media:
                # Create download task
                download_task = DownloadTask(
                    media_file_id=media.id,
                    task_type="media_retry",
                    priority=media.processing_priority or TaskPriority.NORMAL.value,
                    metadata={
                        "retry_attempt": media.download_attempts + 1,
                        "media_type": media.file_type,
                        "original_error": getattr(media, 'download_error', None)
                    }
                )
                
                # Enqueue with appropriate priority
                priority = TaskPriority.HIGH.value if media.file_type == "document" else TaskPriority.NORMAL.value
                task_id = await self.queue_manager.enqueue_download(download_task, priority)
                
                processed_count += 1
                await self.logger.log_with_context(
                    "DEBUG",
                    f"Queued retry task for media {media.id}",
                    "MediaRetryService",
                    context={"task_id": task_id, "media_id": media.id}
                )
            
            self._stats["total_retried"] += processed_count
            return processed_count
            
        except Exception as e:
            await self.logger.log_with_context(
                "ERROR",
                "Error processing batch with queue",
                "MediaRetryService",
                error=e
            )
            return 0
    
    async def _process_batch_legacy(self, pending_media: List[MediaFile]) -> int:
        """Process batch using legacy method with enhancements."""
        tasks = []
        for media in pending_media:
            tasks.append(self.retry_single_media(media.id))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        fail_count = sum(1 for r in results if r is False or isinstance(r, Exception))
        
        self._stats["total_retried"] += len(pending_media)
        self._stats["successful"] += success_count
        self._stats["failed"] += fail_count
        self._stats["pending_count"] = await self.get_pending_count()
        
        await self.logger.log_with_context(
            "INFO",
            "Batch complete",
            "MediaRetryService",
            context={
                "success_count": success_count,
                "fail_count": fail_count,
                "total_processed": len(pending_media)
            }
        )
        return len(pending_media)
    
    async def retry_single_media(self, media_id: int) -> bool:
        """
        Retry a single media download with detailed logging.
        
        Args:
            media_id: ID of the media file to retry
            
        Returns:
            bool: True if retry successful, False otherwise
        """
        async with self._semaphore:
            # Start operation tracking
            op_id = await self.logger.log_operation_start(
                "retry_single_media",
                "MediaRetryService",
                context={"media_id": media_id}
            )
            
            try:
                async with async_session_maker() as db:
                    # Get media file with related data
                    result = await db.execute(
                        select(MediaFile)
                        .where(MediaFile.id == media_id)
                    )
                    media = result.scalars().first()
                    
                    if not media or (media.file_path and media.validation_status == "valid"):
                        await self.logger.log_operation_end(
                            op_id,
                            success=True,
                            context={"reason": "already_processed"}
                        )
                        return True  # Already processed successfully
                    
                    # Update download attempt count
                    current_attempts = media.download_attempts or 0
                    if current_attempts >= self._settings["max_retries"]:
                        # Mark as permanently failed
                        await db.execute(
                            update(MediaFile)
                            .where(MediaFile.id == media_id)
                            .values(
                                processing_status="permanently_failed",
                                download_error=f"Exceeded maximum retry attempts ({self._settings['max_retries']})"
                            )
                        )
                        await db.commit()
                        
                        await self.logger.log_with_context(
                            "WARNING",
                            f"Media {media_id} marked as permanently failed after {current_attempts} attempts",
                            "MediaRetryService",
                            context={
                                "media_id": media_id,
                                "current_attempts": current_attempts,
                                "max_retries": self._settings["max_retries"]
                            }
                        )
                        await self.logger.log_operation_end(
                            op_id,
                            success=False,
                            context={"reason": "max_retries_exceeded", "marked_permanently_failed": True}
                        )
                        return False
                    
                    # Update attempt count and timestamp
                    await db.execute(
                        update(MediaFile)
                        .where(MediaFile.id == media_id)
                        .values(
                            download_attempts=current_attempts + 1,
                            last_download_attempt=datetime.utcnow(),
                            processing_status="processing"
                        )
                    )
                    await db.commit()
                    
                    # Track retry attempt
                    attempt_key = str(min(current_attempts + 1, 3))
                    if current_attempts + 1 > 3:
                        attempt_key = "3+"
                    self._stats["retry_by_attempt"][attempt_key] += 1
                    
                    await self.logger.log_with_context(
                        "INFO",
                        f"Retrying media download (attempt {current_attempts + 1})",
                        "MediaRetryService",
                        context={
                            "media_id": media_id,
                            "file_type": media.file_type,
                            "attempt": current_attempts + 1,
                            "max_retries": self._settings["max_retries"]
                        }
                    )
                    
                    # Get message and group info
                    msg_result = await db.execute(
                        select(TelegramMessage)
                        .where(TelegramMessage.id == media.message_id)
                    )
                    message = msg_result.scalars().first()
                    
                    if not message:
                        error_category = ErrorCategory.UNKNOWN_ERRORS
                        await self._update_media_error(db, media_id, "Message not found in database", error_category)
                        await self.logger.log_operation_end(
                            op_id,
                            success=False,
                            context={"error_category": error_category.value}
                        )
                        return False
                    
                    group_result = await db.execute(
                        select(TelegramGroup)
                        .where(TelegramGroup.id == message.group_id)
                    )
                    group = group_result.scalars().first()
                    
                    if not group or not group.assigned_account_id:
                        error_category = ErrorCategory.UNKNOWN_ERRORS
                        await self._update_media_error(db, media_id, "Group or account not found", error_category)
                        await self.logger.log_operation_end(
                            op_id,
                            success=False,
                            context={"error_category": error_category.value}
                        )
                        return False
                    
                    # Get client with session recovery
                    client = await self._get_active_client(group.assigned_account_id)
                    if not client:
                        error_category = ErrorCategory.AUTHORIZATION_ERRORS
                        await self._update_media_error(db, media_id, "No active client available", error_category)
                        await self.logger.log_operation_end(
                            op_id,
                            success=False,
                            context={"error_category": error_category.value}
                        )
                        return False
                    
                    # Apply exponential backoff with jitter
                    if self._settings["exponential_backoff"] and current_attempts > 0:
                        delay = self._calculate_retry_delay(current_attempts)
                        await self.logger.log_with_context(
                            "DEBUG",
                            f"Applying retry delay of {delay:.2f}s",
                            "MediaRetryService",
                            context={"media_id": media_id, "delay_seconds": delay}
                        )
                        await asyncio.sleep(delay)
                    
                    # Attempt to download
                    success = await self._attempt_download(client, media, message, group, db)
                    
                    if success:
                        self._stats["successful"] += 1
                        await self.logger.log_with_context(
                            "INFO",
                            f"Successfully retried media {media_id}",
                            "MediaRetryService",
                            context={
                                "media_id": media_id,
                                "file_type": media.file_type,
                                "attempt": current_attempts + 1
                            }
                        )
                        await self.logger.log_operation_end(op_id, success=True)
                    else:
                        self._stats["failed"] += 1
                        await self.logger.log_operation_end(op_id, success=False)
                    
                    return success
                    
            except Exception as e:
                error_category = self.categorize_error(e)
                self._stats["error_categories"][error_category.value] += 1
                
                await self.logger.log_with_context(
                    "ERROR",
                    f"Fatal error retrying media {media_id}",
                    "MediaRetryService",
                    context={
                        "media_id": media_id,
                        "error_category": error_category.value
                    },
                    error=e
                )
                await self.logger.log_operation_end(op_id, success=False, error=e)
                return False
    
    async def _retry_single_enhanced(self, media_id: int) -> bool:
        """Legacy method - redirects to retry_single_media."""
        return await self.retry_single_media(media_id)
    
    async def _get_active_client(self, account_id: int):
        """Get active client with session recovery."""
        if self._settings["session_recovery_enabled"]:
            client = await self.session_recovery.ensure_session_active(account_id)
            if client:
                return client
            
            # Try backup account rotation
            backup_client = await self.session_recovery.rotate_to_backup_account(account_id)
            if backup_client:
                self._stats["session_recoveries"] += 1
                return backup_client
        
        # Fallback to telegram manager
        return telegram_manager.clients.get(account_id)
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff and jitter."""
        base_delay = self._settings["retry_delay_base"]
        delay = base_delay * (2 ** (attempt - 1))
        
        # Cap maximum delay at 5 minutes
        delay = min(delay, 300)
        
        # Add jitter if enabled
        if self._settings["jitter_enabled"]:
            import random
            jitter = random.uniform(0.5, 1.5)
            delay *= jitter
        
        return delay
    
    async def _attempt_download(
        self, 
        client, 
        media: MediaFile, 
        message: TelegramMessage, 
        group: TelegramGroup, 
        db: AsyncSession
    ) -> bool:
        """Attempt to download media with enhanced error handling."""
        try:
            # Check rate limiting
            await self.rate_limiter.wait_if_needed(OperationType.MEDIA_DOWNLOAD)
            
            # Get the Telegram message
            entity = await client.get_entity(group.telegram_id)
            telegram_msg = await client.get_messages(entity, ids=message.telegram_id)
            
            if not telegram_msg or not telegram_msg.media:
                error_category = ErrorCategory.MEDIA_NOT_FOUND
                await self._update_media_error(db, media.id, "Message or media no longer exists on Telegram", error_category)
                return False
            
            # Download the media
            file_path, file_hash = await self._download_media_enhanced(
                client, telegram_msg, media.file_type, group.id
            )
            
            if not file_path:
                error_category = ErrorCategory.NETWORK_ERRORS
                await self._update_media_error(db, media.id, "Download returned empty", error_category)
                return False
            
            # Validate the downloaded file if enabled
            validation_status = "valid"
            if self._settings["validate_downloads"]:
                validation_result = await self.media_validator.validate_media_file(
                    file_path, media.file_type
                )
                validation_status = validation_result.status.value
                
                if validation_result.status == ValidationStatus.INVALID:
                    self._stats["validation_failures"] += 1
                    await self.logger.log_with_context(
                        "WARNING",
                        "Downloaded file failed validation",
                        "MediaRetryService",
                        context={
                            "media_id": media.id,
                            "file_path": file_path,
                            "validation_status": validation_status
                        }
                    )
                elif validation_result.status == ValidationStatus.CORRUPTED:
                    self._stats["validation_failures"] += 1
                    await self.logger.log_with_context(
                        "WARNING",
                        "Downloaded file is corrupted",
                        "MediaRetryService",
                        context={
                            "media_id": media.id,
                            "file_path": file_path,
                            "validation_status": validation_status
                        }
                    )
            
            # Update media record
            await db.execute(
                update(MediaFile)
                .where(MediaFile.id == media.id)
                .values(
                    file_path=file_path,
                    file_hash=file_hash,
                    download_error=None,
                    validation_status=validation_status,
                    processing_status="completed"
                )
            )
            await db.commit()
            
            await self.logger.log_with_context(
                "INFO",
                f"Downloaded media successfully",
                "MediaRetryService",
                context={
                    "media_id": media.id,
                    "file_path": file_path,
                    "file_hash": file_hash[:16] if file_hash else None,
                    "validation_status": validation_status
                }
            )
            return True
            
        except Exception as e:
            error_msg = str(e)[:500]
            error_category = self.categorize_error(e)
            self._stats["error_categories"][error_category.value] += 1
            
            await self._update_media_error(db, media.id, error_msg, error_category)
            await self.logger.log_with_context(
                "ERROR",
                f"Error downloading media {media.id}",
                "MediaRetryService",
                context={
                    "media_id": media.id,
                    "error_category": error_category.value,
                    "error_message": error_msg
                },
                error=e
            )
            return False
    
    async def _update_media_error(self, db: AsyncSession, media_id: int, error_msg: str, category: ErrorCategory):
        """Update media record with error information."""
        await db.execute(
            update(MediaFile)
            .where(MediaFile.id == media_id)
            .values(
                download_error=error_msg,
                processing_status="failed"
            )
        )
        await db.commit()
        
        # Log the error with category
        await self.logger.log_with_context(
            "ERROR",
            f"Media download failed: {error_msg}",
            "MediaRetryService",
            context={
                "media_id": media_id,
                "error_category": category.value,
                "error_message": error_msg
            }
        )
    
    async def _download_media_enhanced(self, client, message, media_type: str, group_id: int):
        """Enhanced media download with better error handling and file verification."""
        try:
            # Validate parameters before API call
            if not hasattr(message, 'media'):
                raise ValueError("Message has no media attribute")
            if not media_type:
                raise ValueError("Media type is required")
            
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
            
            # Use MEDIA_DIR from config
            media_dir = config_manager.get("MEDIA_DIR", "media")
            target_dir = os.path.join(media_dir, subdir, str(group_id))
            os.makedirs(target_dir, exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            msg_id = message.id if hasattr(message, 'id') else "retry"
            
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
            
            filename = f"{timestamp}_{msg_id}_retry{ext}"
            file_path = os.path.join(target_dir, filename)
            
            if not file_path:
                raise ValueError("File path is required")
            
            await self.logger.log_with_context(
                "DEBUG",
                f"Attempting download to: {file_path}",
                "MediaRetryService",
                context={
                    "file_path": file_path,
                    "media_type": media_type,
                    "group_id": group_id
                }
            )
            
            # Download with timeout from config - message as positional parameter
            download_timeout = config_manager.get_int("MEDIA_DOWNLOAD_TIMEOUT", 300)
            download_result = await asyncio.wait_for(
                client.download_media(message, file=file_path),
                timeout=download_timeout
            )
            
            await self.logger.log_with_context(
                "DEBUG",
                f"Download result: {download_result}",
                "MediaRetryService",
                context={"download_result": str(download_result)}
            )
            
            # Wait a moment for file system to sync (especially important in Docker)
            await asyncio.sleep(0.2)
            
            # Verify file exists and has content
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                await self.logger.log_with_context(
                    "DEBUG",
                    f"File exists with size {file_size} bytes",
                    "MediaRetryService",
                    context={
                        "file_path": file_path,
                        "file_size_bytes": file_size
                    }
                )
                
                if file_size > 0:
                    file_hash = await self._compute_hash(file_path)
                    await self.logger.log_with_context(
                        "INFO",
                        f"Successfully downloaded {media_type}",
                        "MediaRetryService",
                        context={
                            "file_path": file_path,
                            "file_size_bytes": file_size,
                            "file_hash": file_hash[:16] if file_hash else None,
                            "media_type": media_type
                        }
                    )
                    return file_path, file_hash
                else:
                    await self.logger.log_with_context(
                        "ERROR",
                        "Downloaded file is empty",
                        "MediaRetryService",
                        context={"file_path": file_path}
                    )
                    # Remove empty file
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                    return None, None
            else:
                await self.logger.log_with_context(
                    "ERROR",
                    "Downloaded file does not exist",
                    "MediaRetryService",
                    context={
                        "file_path": file_path,
                        "download_result": str(download_result)
                    }
                )
                return None, None
            
        except asyncio.TimeoutError:
            await self.logger.log_with_context(
                "ERROR",
                f"Download timeout for {media_type}",
                "MediaRetryService",
                context={
                    "media_type": media_type,
                    "msg_id": msg_id if 'msg_id' in locals() else None,
                    "timeout_seconds": download_timeout if 'download_timeout' in locals() else None
                }
            )
            return None, None
        except Exception as e:
            await self.logger.log_with_context(
                "ERROR",
                f"Enhanced download error for {media_type}",
                "MediaRetryService",
                context={"media_type": media_type},
                error=e
            )
            return None, None
    
    async def _retry_single(self, media_id: int) -> bool:
        async with self._semaphore:
            try:
                async with async_session_maker() as db:
                    result = await db.execute(
                        select(MediaFile)
                        .where(MediaFile.id == media_id)
                    )
                    media = result.scalars().first()
                    
                    if not media or media.file_path:
                        return True
                    
                    msg_result = await db.execute(
                        select(TelegramMessage)
                        .where(TelegramMessage.id == media.message_id)
                    )
                    message = msg_result.scalars().first()
                    
                    if not message:
                        await db.execute(
                            update(MediaFile)
                            .where(MediaFile.id == media_id)
                            .values(download_error="Message not found in database")
                        )
                        await db.commit()
                        return False
                    
                    group_result = await db.execute(
                        select(TelegramGroup)
                        .where(TelegramGroup.id == message.group_id)
                    )
                    group = group_result.scalars().first()
                    
                    if not group or not group.assigned_account_id:
                        await db.execute(
                            update(MediaFile)
                            .where(MediaFile.id == media_id)
                            .values(download_error="Group or account not found")
                        )
                        await db.commit()
                        return False
                    
                    client = telegram_manager.clients.get(group.assigned_account_id)
                    if not client:
                        await db.execute(
                            update(MediaFile)
                            .where(MediaFile.id == media_id)
                            .values(download_error="Telegram client not available")
                        )
                        await db.commit()
                        return False
                    
                    try:
                        entity = await client.get_entity(group.telegram_id)
                        telegram_msg = await client.get_messages(entity, ids=message.telegram_id)
                        
                        if not telegram_msg or not telegram_msg.media:
                            await db.execute(
                                update(MediaFile)
                                .where(MediaFile.id == media_id)
                                .values(download_error="Message or media no longer exists on Telegram")
                            )
                            await db.commit()
                            return False
                        
                        file_path, file_hash = await self._download_media(
                            client, telegram_msg, media.file_type, group.id
                        )
                        
                        if file_path:
                            await db.execute(
                                update(MediaFile)
                                .where(MediaFile.id == media_id)
                                .values(
                                    file_path=file_path,
                                    file_hash=file_hash,
                                    download_error=None
                                )
                            )
                            await db.commit()
                            print(f"[MediaRetry] Downloaded media {media_id}: {file_path}")
                            return True
                        else:
                            await db.execute(
                                update(MediaFile)
                                .where(MediaFile.id == media_id)
                                .values(download_error="Download returned empty")
                            )
                            await db.commit()
                            return False
                            
                    except Exception as e:
                        error_msg = str(e)[:500]
                        await db.execute(
                            update(MediaFile)
                            .where(MediaFile.id == media_id)
                            .values(download_error=error_msg)
                        )
                        await db.commit()
                        print(f"[MediaRetry] Error downloading media {media_id}: {e}")
                        return False
                        
            except Exception as e:
                print(f"[MediaRetry] Fatal error for media {media_id}: {e}")
                return False
    
    async def _download_media(self, client, message, media_type: str, group_id: int):
        try:
            # Validate parameters before API call
            if not hasattr(message, 'media'):
                raise ValueError("Message has no media attribute")
            if not media_type:
                raise ValueError("Media type is required")
            
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
            target_dir = os.path.join("media", subdir, str(group_id))
            os.makedirs(target_dir, exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            msg_id = message.id if hasattr(message, 'id') else "retry"
            
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
            
            filename = f"{timestamp}_{msg_id}_retry{ext}"
            file_path = os.path.join(target_dir, filename)
            
            if not file_path:
                raise ValueError("File path is required")
            
            # Message as positional parameter (not named)
            await client.download_media(message, file=file_path)
            
            if os.path.exists(file_path):
                file_hash = await self._compute_hash(file_path)
                return file_path, file_hash
            
            return None, None
            
        except Exception as e:
            # Use enhanced logging instead of print
            asyncio.create_task(self.logger.log_with_context(
                "ERROR",
                f"Download error: {str(e)}",
                "MediaRetryService",
                error=e
            ))
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


media_retry_service = MediaRetryService()


class BatchRetryProcessor:
    """
    Handles batch processing of failed downloads with checkpoint and resume functionality.
    """
    
    def __init__(self, media_retry_service: MediaRetryService):
        self.logger = logging.getLogger(__name__)
        self.media_retry_service = media_retry_service
        self._is_running = False
        self._current_batch: Optional[BatchProcessing] = None
        self._processing_task: Optional[asyncio.Task] = None
        
        # Configuration - OPTIMIZED FOR PERFORMANCE
        self.batch_size = 200  # Increased from 100
        self.parallel_workers = 10  # Increased from 5
        self.checkpoint_interval = 100  # Save progress every N items
        
    async def start_batch_processing(
        self, 
        batch_name: str,
        filter_criteria: Optional[Dict[str, Any]] = None,
        priority_order: Optional[List[str]] = None
    ) -> str:
        """
        Starts batch processing of failed downloads.
        
        Args:
            batch_name: Name for the batch processing job
            filter_criteria: Optional criteria to filter media files
            priority_order: Optional priority order for media types
            
        Returns:
            Batch processing ID
        """
        if self._is_running:
            raise RuntimeError("Batch processing is already running")
        
        self.logger.info(f"Starting batch processing: {batch_name}")
        
        # Create batch processing record
        async with async_session_maker() as db:
            batch = BatchProcessing(
                batch_name=batch_name,
                status="running",
                filter_criteria=filter_criteria or {},
                priority_order=priority_order or ["document", "photo", "video", "audio"],
                batch_size=self.batch_size,
                parallel_workers=self.parallel_workers
            )
            
            db.add(batch)
            await db.commit()
            await db.refresh(batch)
            
            self._current_batch = batch
            batch_id = batch.id
        
        # Start processing task
        self._is_running = True
        self._processing_task = asyncio.create_task(self._process_batch_loop())
        
        self.logger.info(f"Batch processing started with ID: {batch_id}")
        return str(batch_id)
    
    async def stop_batch_processing(self):
        """Stops batch processing and saves checkpoint."""
        if not self._is_running:
            return
        
        self.logger.info("Stopping batch processing...")
        
        self._is_running = False
        
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
        
        # Update batch status
        if self._current_batch:
            async with async_session_maker() as db:
                await db.execute(
                    update(BatchProcessing)
                    .where(BatchProcessing.id == self._current_batch.id)
                    .values(
                        status="stopped",
                        completed_at=datetime.utcnow()
                    )
                )
                await db.commit()
        
        self.logger.info("Batch processing stopped")
    
    async def get_batch_status(self) -> Optional[Dict[str, Any]]:
        """Gets current batch processing status."""
        if not self._current_batch:
            return None
        
        async with async_session_maker() as db:
            result = await db.execute(
                select(BatchProcessing).where(BatchProcessing.id == self._current_batch.id)
            )
            batch = result.scalars().first()
            
            if not batch:
                return None
            
            return {
                "batch_id": batch.id,
                "batch_name": batch.batch_name,
                "status": batch.status,
                "total_items": batch.total_items,
                "processed_items": batch.processed_items,
                "successful_items": batch.successful_items,
                "failed_items": batch.failed_items,
                "progress_percentage": (batch.processed_items / batch.total_items * 100) if batch.total_items > 0 else 0,
                "started_at": batch.started_at.isoformat() if batch.started_at else None,
                "estimated_completion": batch.estimated_completion.isoformat() if batch.estimated_completion else None,
                "is_running": self._is_running
            }
    
    async def _process_batch_loop(self):
        """Main batch processing loop."""
        try:
            # Get total count of items to process
            total_items = await self._get_total_items()
            
            # Update batch record
            async with async_session_maker() as db:
                await db.execute(
                    update(BatchProcessing)
                    .where(BatchProcessing.id == self._current_batch.id)
                    .values(
                        total_items=total_items,
                        started_at=datetime.utcnow()
                    )
                )
                await db.commit()
            
            processed_count = 0
            
            while self._is_running and processed_count < total_items:
                # Get next batch of items
                batch_items = await self._get_next_batch_items(processed_count)
                
                if not batch_items:
                    break
                
                # Process batch items in parallel
                success_count = await self._process_batch_items(batch_items)
                
                processed_count += len(batch_items)
                
                # Update progress
                await self._update_progress(processed_count, success_count)
                
                # Save checkpoint
                if processed_count % self.checkpoint_interval == 0:
                    await self._save_checkpoint(processed_count)
                
                self.logger.info(f"Batch progress: {processed_count}/{total_items} ({processed_count/total_items*100:.1f}%)")
            
            # Mark as completed
            await self._complete_batch()
            
        except Exception as e:
            self.logger.error(f"Error in batch processing loop: {e}")
            await self._fail_batch(str(e))
    
    async def _get_total_items(self) -> int:
        """Gets total count of items to process."""
        async with async_session_maker() as db:
            result = await db.execute(
                select(func.count(MediaFile.id)).where(
                    and_(
                        or_(
                            MediaFile.file_path.is_(None),
                            MediaFile.validation_status == "invalid",
                            MediaFile.validation_status == "corrupted"
                        ),
                        MediaFile.is_duplicate == False,
                        MediaFile.download_attempts < 3
                    )
                )
            )
            return result.scalar() or 0
    
    async def _get_next_batch_items(self, offset: int) -> List[MediaFile]:
        """Gets next batch of items to process."""
        async with async_session_maker() as db:
            result = await db.execute(
                select(MediaFile)
                .where(
                    and_(
                        or_(
                            MediaFile.file_path.is_(None),
                            MediaFile.validation_status == "invalid",
                            MediaFile.validation_status == "corrupted"
                        ),
                        MediaFile.is_duplicate == False,
                        MediaFile.download_attempts < 3
                    )
                )
                .order_by(
                    MediaFile.processing_priority.asc(),
                    MediaFile.created_at.desc()
                )
                .offset(offset)
                .limit(self.batch_size)
            )
            return result.scalars().all()
    
    async def _process_batch_items(self, items: List[MediaFile]) -> int:
        """Processes a batch of items in parallel."""
        semaphore = asyncio.Semaphore(self.parallel_workers)
        
        async def process_item(item):
            async with semaphore:
                return await self.media_retry_service._retry_single_enhanced(item.id)
        
        tasks = [process_item(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        return success_count
    
    async def _update_progress(self, processed_items: int, successful_items: int):
        """Updates batch processing progress."""
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == self._current_batch.id)
                .values(
                    processed_items=processed_items,
                    successful_items=BatchProcessing.successful_items + successful_items,
                    failed_items=processed_items - BatchProcessing.successful_items
                )
            )
            await db.commit()
    
    async def _save_checkpoint(self, processed_items: int):
        """Saves processing checkpoint."""
        checkpoint_data = {
            "processed_items": processed_items,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == self._current_batch.id)
                .values(checkpoint_data=checkpoint_data)
            )
            await db.commit()
    
    async def _complete_batch(self):
        """Marks batch as completed."""
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == self._current_batch.id)
                .values(
                    status="completed",
                    completed_at=datetime.utcnow()
                )
            )
            await db.commit()
        
        self.logger.info("Batch processing completed successfully")
    
    async def _fail_batch(self, error_message: str):
        """Marks batch as failed."""
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == self._current_batch.id)
                .values(
                    status="failed",
                    error_message=error_message,
                    completed_at=datetime.utcnow()
                )
            )
            await db.commit()
        
        self.logger.error(f"Batch processing failed: {error_message}")


# Create batch processor instance
batch_retry_processor = BatchRetryProcessor(media_retry_service)
