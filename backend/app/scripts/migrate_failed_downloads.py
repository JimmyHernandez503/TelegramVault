#!/usr/bin/env python3
"""
Migration script for existing failed downloads.

This script identifies and categorizes the existing 46,538 failed downloads
and creates batch processing jobs for different failure types with progress
tracking and resumption capability.

Usage:
    python migrate_failed_downloads.py [--dry-run] [--batch-size=100] [--resume]
"""

import asyncio
import argparse
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from pathlib import Path

import sys
sys.path.append('/app')

from sqlalchemy import select, update, func, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from backend.app.db.database import async_session_maker
from backend.app.models.media import MediaFile
from backend.app.models.download_task import DownloadTask, BatchProcessing
from backend.app.core.queue_types import TaskPriority


@dataclass
class FailureCategory:
    """Data class for failure category analysis."""
    category: str
    count: int
    percentage: float
    priority: TaskPriority
    description: str
    examples: List[str]


@dataclass
class MigrationProgress:
    """Data class for migration progress tracking."""
    total_failed: int
    categorized: int
    queued_for_retry: int
    completed: int
    errors: int
    start_time: datetime
    last_checkpoint: datetime
    categories: Dict[str, int]
    batch_id: str


class FailedDownloadMigrator:
    """
    Migrates existing failed downloads to the new retry system.
    
    This class analyzes failed downloads, categorizes them by failure type,
    creates prioritized batch processing jobs, and provides progress tracking
    with checkpoint/resume functionality.
    """
    
    def __init__(self, batch_size: int = 100, checkpoint_interval: int = 1000):
        self.logger = logging.getLogger(__name__)
        self.batch_size = batch_size
        self.checkpoint_interval = checkpoint_interval
        
        # Progress tracking
        self.progress_file = Path("/app/data/migration_progress.json")
        self.progress: Optional[MigrationProgress] = None
        
        # Failure categories with priorities
        self.failure_categories = {
            "no_error_message": {
                "priority": TaskPriority.HIGH,
                "description": "Files with no error message (likely never attempted)",
                "retry_strategy": "immediate"
            },
            "empty_download": {
                "priority": TaskPriority.CRITICAL,
                "description": "Downloads that returned empty files",
                "retry_strategy": "with_validation"
            },
            "message_not_exists": {
                "priority": TaskPriority.LOW,
                "description": "Message or media no longer exists on Telegram",
                "retry_strategy": "single_attempt"
            },
            "disconnected": {
                "priority": TaskPriority.HIGH,
                "description": "Cannot send requests while disconnected",
                "retry_strategy": "with_session_recovery"
            },
            "timeout": {
                "priority": TaskPriority.NORMAL,
                "description": "Download timeout errors",
                "retry_strategy": "with_extended_timeout"
            },
            "rate_limit": {
                "priority": TaskPriority.NORMAL,
                "description": "Rate limit or flood wait errors",
                "retry_strategy": "with_backoff"
            },
            "file_system": {
                "priority": TaskPriority.HIGH,
                "description": "File system or permission errors",
                "retry_strategy": "with_path_validation"
            },
            "unknown": {
                "priority": TaskPriority.NORMAL,
                "description": "Other unknown errors",
                "retry_strategy": "standard"
            }
        }
        
        # Statistics
        self.stats = {
            "total_analyzed": 0,
            "total_categorized": 0,
            "total_queued": 0,
            "categories_found": {},
            "processing_time": 0.0
        }
    
    async def analyze_failed_downloads(self, dry_run: bool = False) -> List[FailureCategory]:
        """
        Analyze existing failed downloads and categorize them.
        
        Args:
            dry_run: If True, only analyze without making changes
            
        Returns:
            List of failure categories with statistics
        """
        self.logger.info("Starting analysis of failed downloads...")
        
        async with async_session_maker() as db:
            # Get total count of failed downloads
            total_query = select(func.count(MediaFile.id)).where(
                or_(
                    MediaFile.file_path.is_(None),
                    MediaFile.file_path == '',
                    MediaFile.download_error.isnot(None)
                )
            )
            total_result = await db.execute(total_query)
            total_failed = total_result.scalar()
            
            self.logger.info(f"Found {total_failed} failed downloads to analyze")
            
            # Analyze by error categories
            categories = []
            
            # 1. No error message (likely never attempted)
            no_error_query = select(func.count(MediaFile.id)).where(
                and_(
                    or_(MediaFile.file_path.is_(None), MediaFile.file_path == ''),
                    MediaFile.download_error.is_(None)
                )
            )
            no_error_count = (await db.execute(no_error_query)).scalar()
            
            if no_error_count > 0:
                categories.append(FailureCategory(
                    category="no_error_message",
                    count=no_error_count,
                    percentage=(no_error_count / total_failed) * 100,
                    priority=self.failure_categories["no_error_message"]["priority"],
                    description=self.failure_categories["no_error_message"]["description"],
                    examples=[]
                ))
            
            # 2. Empty download errors
            empty_query = select(func.count(MediaFile.id)).where(
                MediaFile.download_error.like('%empty%')
            )
            empty_count = (await db.execute(empty_query)).scalar()
            
            if empty_count > 0:
                categories.append(FailureCategory(
                    category="empty_download",
                    count=empty_count,
                    percentage=(empty_count / total_failed) * 100,
                    priority=self.failure_categories["empty_download"]["priority"],
                    description=self.failure_categories["empty_download"]["description"],
                    examples=[]
                ))
            
            # 3. Message not exists errors
            not_exists_query = select(func.count(MediaFile.id)).where(
                or_(
                    MediaFile.download_error.like('%no longer exists%'),
                    MediaFile.download_error.like('%not found%'),
                    MediaFile.download_error.like('%deleted%')
                )
            )
            not_exists_count = (await db.execute(not_exists_query)).scalar()
            
            if not_exists_count > 0:
                categories.append(FailureCategory(
                    category="message_not_exists",
                    count=not_exists_count,
                    percentage=(not_exists_count / total_failed) * 100,
                    priority=self.failure_categories["message_not_exists"]["priority"],
                    description=self.failure_categories["message_not_exists"]["description"],
                    examples=[]
                ))
            
            # 4. Disconnection errors
            disconnected_query = select(func.count(MediaFile.id)).where(
                or_(
                    MediaFile.download_error.like('%disconnected%'),
                    MediaFile.download_error.like('%connection%'),
                    MediaFile.download_error.like('%network%')
                )
            )
            disconnected_count = (await db.execute(disconnected_query)).scalar()
            
            if disconnected_count > 0:
                categories.append(FailureCategory(
                    category="disconnected",
                    count=disconnected_count,
                    percentage=(disconnected_count / total_failed) * 100,
                    priority=self.failure_categories["disconnected"]["priority"],
                    description=self.failure_categories["disconnected"]["description"],
                    examples=[]
                ))
            
            # 5. Timeout errors
            timeout_query = select(func.count(MediaFile.id)).where(
                or_(
                    MediaFile.download_error.like('%timeout%'),
                    MediaFile.download_error.like('%timed out%')
                )
            )
            timeout_count = (await db.execute(timeout_query)).scalar()
            
            if timeout_count > 0:
                categories.append(FailureCategory(
                    category="timeout",
                    count=timeout_count,
                    percentage=(timeout_count / total_failed) * 100,
                    priority=self.failure_categories["timeout"]["priority"],
                    description=self.failure_categories["timeout"]["description"],
                    examples=[]
                ))
            
            # 6. Rate limit errors
            rate_limit_query = select(func.count(MediaFile.id)).where(
                or_(
                    MediaFile.download_error.like('%rate limit%'),
                    MediaFile.download_error.like('%flood%'),
                    MediaFile.download_error.like('%429%')
                )
            )
            rate_limit_count = (await db.execute(rate_limit_query)).scalar()
            
            if rate_limit_count > 0:
                categories.append(FailureCategory(
                    category="rate_limit",
                    count=rate_limit_count,
                    percentage=(rate_limit_count / total_failed) * 100,
                    priority=self.failure_categories["rate_limit"]["priority"],
                    description=self.failure_categories["rate_limit"]["description"],
                    examples=[]
                ))
            
            # 7. File system errors
            fs_query = select(func.count(MediaFile.id)).where(
                or_(
                    MediaFile.download_error.like('%permission%'),
                    MediaFile.download_error.like('%disk%'),
                    MediaFile.download_error.like('%space%'),
                    MediaFile.download_error.like('%path%')
                )
            )
            fs_count = (await db.execute(fs_query)).scalar()
            
            if fs_count > 0:
                categories.append(FailureCategory(
                    category="file_system",
                    count=fs_count,
                    percentage=(fs_count / total_failed) * 100,
                    priority=self.failure_categories["file_system"]["priority"],
                    description=self.failure_categories["file_system"]["description"],
                    examples=[]
                ))
            
            # 8. Unknown errors (everything else with error messages)
            categorized_count = sum(cat.count for cat in categories)
            unknown_count = total_failed - categorized_count
            
            if unknown_count > 0:
                categories.append(FailureCategory(
                    category="unknown",
                    count=unknown_count,
                    percentage=(unknown_count / total_failed) * 100,
                    priority=self.failure_categories["unknown"]["priority"],
                    description=self.failure_categories["unknown"]["description"],
                    examples=[]
                ))
            
            # Sort by priority (critical first)
            categories.sort(key=lambda x: x.priority.value)
            
            # Log analysis results
            self.logger.info(f"Analysis complete. Found {len(categories)} failure categories:")
            for cat in categories:
                self.logger.info(f"  {cat.category}: {cat.count} files ({cat.percentage:.1f}%) - {cat.description}")
            
            return categories
    
    async def create_batch_processing_jobs(self, categories: List[FailureCategory], dry_run: bool = False) -> str:
        """
        Create batch processing jobs for each failure category.
        
        Args:
            categories: List of failure categories to process
            dry_run: If True, only simulate without creating jobs
            
        Returns:
            Batch processing ID
        """
        batch_id = f"migration_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        if dry_run:
            self.logger.info(f"DRY RUN: Would create batch processing jobs with ID: {batch_id}")
            return batch_id
        
        self.logger.info(f"Creating batch processing jobs with ID: {batch_id}")
        
        async with async_session_maker() as db:
            total_jobs_created = 0
            
            for category in categories:
                # Create batch processing record
                batch_processing = BatchProcessing(
                    batch_id=f"{batch_id}_{category.category}",
                    batch_type="failed_download_migration",
                    total_items=category.count,
                    priority=category.priority.value,
                    metadata={
                        "category": category.category,
                        "description": category.description,
                        "retry_strategy": self.failure_categories[category.category]["retry_strategy"],
                        "parent_batch_id": batch_id
                    }
                )
                
                db.add(batch_processing)
                
                # Create individual download tasks for this category
                tasks_created = await self._create_download_tasks_for_category(
                    db, category, batch_processing.batch_id
                )
                
                total_jobs_created += tasks_created
                
                self.logger.info(f"Created {tasks_created} download tasks for category: {category.category}")
            
            await db.commit()
            
            self.logger.info(f"Successfully created {total_jobs_created} download tasks across {len(categories)} categories")
            
            return batch_id
    
    async def _create_download_tasks_for_category(
        self, 
        db: AsyncSession, 
        category: FailureCategory, 
        batch_id: str
    ) -> int:
        """Create download tasks for a specific failure category."""
        
        # Build query based on category
        if category.category == "no_error_message":
            query = select(MediaFile).where(
                and_(
                    or_(MediaFile.file_path.is_(None), MediaFile.file_path == ''),
                    MediaFile.download_error.is_(None)
                )
            )
        elif category.category == "empty_download":
            query = select(MediaFile).where(
                MediaFile.download_error.like('%empty%')
            )
        elif category.category == "message_not_exists":
            query = select(MediaFile).where(
                or_(
                    MediaFile.download_error.like('%no longer exists%'),
                    MediaFile.download_error.like('%not found%'),
                    MediaFile.download_error.like('%deleted%')
                )
            )
        elif category.category == "disconnected":
            query = select(MediaFile).where(
                or_(
                    MediaFile.download_error.like('%disconnected%'),
                    MediaFile.download_error.like('%connection%'),
                    MediaFile.download_error.like('%network%')
                )
            )
        elif category.category == "timeout":
            query = select(MediaFile).where(
                or_(
                    MediaFile.download_error.like('%timeout%'),
                    MediaFile.download_error.like('%timed out%')
                )
            )
        elif category.category == "rate_limit":
            query = select(MediaFile).where(
                or_(
                    MediaFile.download_error.like('%rate limit%'),
                    MediaFile.download_error.like('%flood%'),
                    MediaFile.download_error.like('%429%')
                )
            )
        elif category.category == "file_system":
            query = select(MediaFile).where(
                or_(
                    MediaFile.download_error.like('%permission%'),
                    MediaFile.download_error.like('%disk%'),
                    MediaFile.download_error.like('%space%'),
                    MediaFile.download_error.like('%path%')
                )
            )
        else:  # unknown category
            # Get all failed downloads not in other categories
            query = select(MediaFile).where(
                and_(
                    or_(
                        MediaFile.file_path.is_(None),
                        MediaFile.file_path == '',
                        MediaFile.download_error.isnot(None)
                    ),
                    # Exclude other categories
                    ~and_(
                        or_(MediaFile.file_path.is_(None), MediaFile.file_path == ''),
                        MediaFile.download_error.is_(None)
                    ),
                    ~MediaFile.download_error.like('%empty%'),
                    ~or_(
                        MediaFile.download_error.like('%no longer exists%'),
                        MediaFile.download_error.like('%not found%'),
                        MediaFile.download_error.like('%deleted%')
                    ),
                    ~or_(
                        MediaFile.download_error.like('%disconnected%'),
                        MediaFile.download_error.like('%connection%'),
                        MediaFile.download_error.like('%network%')
                    ),
                    ~or_(
                        MediaFile.download_error.like('%timeout%'),
                        MediaFile.download_error.like('%timed out%')
                    ),
                    ~or_(
                        MediaFile.download_error.like('%rate limit%'),
                        MediaFile.download_error.like('%flood%'),
                        MediaFile.download_error.like('%429%')
                    ),
                    ~or_(
                        MediaFile.download_error.like('%permission%'),
                        MediaFile.download_error.like('%disk%'),
                        MediaFile.download_error.like('%space%'),
                        MediaFile.download_error.like('%path%')
                    )
                )
            )
        
        # Process in batches to avoid memory issues
        tasks_created = 0
        offset = 0
        
        while True:
            batch_query = query.offset(offset).limit(self.batch_size)
            result = await db.execute(batch_query)
            media_files = result.scalars().all()
            
            if not media_files:
                break
            
            # Create download tasks for this batch
            for media_file in media_files:
                download_task = DownloadTask(
                    media_file_id=media_file.id,
                    task_type="retry_failed_download",
                    priority=category.priority.value,
                    batch_id=batch_id,
                    metadata={
                        "original_error": media_file.error_message,
                        "failure_category": category.category,
                        "retry_strategy": self.failure_categories[category.category]["retry_strategy"],
                        "media_type": media_file.file_type,
                        "telegram_id": media_file.telegram_id
                    }
                )
                
                db.add(download_task)
                tasks_created += 1
            
            offset += self.batch_size
            
            # Commit periodically to avoid large transactions
            if tasks_created % (self.batch_size * 10) == 0:
                await db.commit()
                self.logger.debug(f"Created {tasks_created} tasks for category {category.category}")
        
        return tasks_created
    
    async def save_checkpoint(self, progress: MigrationProgress):
        """Save migration progress to checkpoint file."""
        try:
            os.makedirs(self.progress_file.parent, exist_ok=True)
            with open(self.progress_file, 'w') as f:
                json.dump(asdict(progress), f, indent=2, default=str)
            self.logger.debug(f"Checkpoint saved: {progress.completed}/{progress.total_failed} completed")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    async def load_checkpoint(self) -> Optional[MigrationProgress]:
        """Load migration progress from checkpoint file."""
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    # Convert string dates back to datetime
                    data['start_time'] = datetime.fromisoformat(data['start_time'])
                    data['last_checkpoint'] = datetime.fromisoformat(data['last_checkpoint'])
                    return MigrationProgress(**data)
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
        return None
    
    async def run_migration(self, dry_run: bool = False, resume: bool = False) -> Dict[str, Any]:
        """
        Run the complete migration process.
        
        Args:
            dry_run: If True, only analyze without making changes
            resume: If True, try to resume from checkpoint
            
        Returns:
            Migration results and statistics
        """
        start_time = datetime.utcnow()
        
        # Try to resume from checkpoint if requested
        if resume:
            self.progress = await self.load_checkpoint()
            if self.progress:
                self.logger.info(f"Resuming migration from checkpoint: {self.progress.completed}/{self.progress.total_failed} completed")
            else:
                self.logger.info("No checkpoint found, starting fresh migration")
        
        try:
            # Step 1: Analyze failed downloads
            self.logger.info("Step 1: Analyzing failed downloads...")
            categories = await self.analyze_failed_downloads(dry_run)
            
            if not categories:
                self.logger.warning("No failed downloads found to migrate")
                return {"status": "no_work", "categories": []}
            
            # Step 2: Create batch processing jobs
            self.logger.info("Step 2: Creating batch processing jobs...")
            batch_id = await self.create_batch_processing_jobs(categories, dry_run)
            
            # Step 3: Update statistics
            total_failed = sum(cat.count for cat in categories)
            
            results = {
                "status": "completed" if not dry_run else "dry_run",
                "batch_id": batch_id,
                "total_failed_downloads": total_failed,
                "categories": [asdict(cat) for cat in categories],
                "processing_time": (datetime.utcnow() - start_time).total_seconds(),
                "dry_run": dry_run
            }
            
            if not dry_run:
                self.logger.info(f"Migration completed successfully!")
                self.logger.info(f"  - Batch ID: {batch_id}")
                self.logger.info(f"  - Total failed downloads: {total_failed}")
                self.logger.info(f"  - Categories processed: {len(categories)}")
                self.logger.info(f"  - Processing time: {results['processing_time']:.2f} seconds")
            else:
                self.logger.info(f"Dry run completed. Would have processed {total_failed} failed downloads in {len(categories)} categories")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            raise


async def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(description="Migrate existing failed downloads to new retry system")
    parser.add_argument("--dry-run", action="store_true", help="Analyze only, don't create jobs")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting failed downloads migration script")
    
    try:
        migrator = FailedDownloadMigrator(batch_size=args.batch_size)
        results = await migrator.run_migration(dry_run=args.dry_run, resume=args.resume)
        
        # Print results summary
        print("\n" + "="*60)
        print("MIGRATION RESULTS SUMMARY")
        print("="*60)
        print(f"Status: {results['status']}")
        print(f"Total failed downloads: {results['total_failed_downloads']}")
        print(f"Processing time: {results['processing_time']:.2f} seconds")
        
        if not args.dry_run:
            print(f"Batch ID: {results['batch_id']}")
        
        print(f"\nFailure Categories ({len(results['categories'])}):")
        for cat in results['categories']:
            print(f"  {cat['category']}: {cat['count']} files ({cat['percentage']:.1f}%)")
            print(f"    Priority: {cat['priority']}")
            print(f"    Description: {cat['description']}")
        
        print("\nNext steps:")
        if args.dry_run:
            print("  1. Run without --dry-run to create batch processing jobs")
            print("  2. Start the MediaRetryService to process the jobs")
        else:
            print("  1. Start the MediaRetryService to process the created jobs")
            print("  2. Monitor progress through the batch processing system")
        
        return 0
        
    except Exception as e:
        logger.error(f"Migration script failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))