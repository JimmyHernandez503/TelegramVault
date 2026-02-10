#!/usr/bin/env python3
"""
Batch Processor Script for Failed Media Downloads

This script processes the 4 batch jobs created by the migration script:
1. empty_download (CRITICAL) - Files with empty downloads
2. no_error_message (HIGH) - Files with no error message
3. disconnected (HIGH) - Files failed due to disconnection
4. message_not_exists (LOW) - Files where message no longer exists

Usage:
    python backend/app/scripts/start_batch_processor.py [--batch-id BATCH_ID] [--all]
"""

import asyncio
import sys
import os
import argparse
import logging
from datetime import datetime
from typing import List, Optional
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.db.database import async_session_maker
from backend.app.models.download_task import DownloadTask, BatchProcessing
from backend.app.models.media import MediaFile
# Import MediaRetryService components without initializing telegram_manager
# from backend.app.services.media_retry_service import MediaRetryService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/batch_processor.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class BatchProcessor:
    """
    Processes batch jobs for failed media downloads.
    """
    
    def __init__(self):
        # Don't initialize MediaRetryService to avoid event loop issues
        # self.media_retry_service = MediaRetryService()
        self.parallel_workers = 5
        self.checkpoint_interval = 50
        self.max_retries = 3
        
    async def process_batch(self, batch_id: int) -> dict:
        """
        Process a single batch job.
        
        Args:
            batch_id: ID of the batch processing job
            
        Returns:
            dict: Processing results
        """
        logger.info(f"Starting batch processing for batch ID: {batch_id}")
        
        async with async_session_maker() as db:
            # Get batch processing record
            result = await db.execute(
                select(BatchProcessing).where(BatchProcessing.id == batch_id)
            )
            batch = result.scalar_one_or_none()
            
            if not batch:
                logger.error(f"Batch {batch_id} not found")
                return {"success": False, "error": "Batch not found"}
            
            if batch.status not in ["pending", "stopped"]:
                logger.warning(f"Batch {batch_id} is in status '{batch.status}', skipping")
                return {"success": False, "error": f"Batch is {batch.status}"}
            
            # Update batch status to running
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == batch_id)
                .values(
                    status="running",
                    started_at=datetime.utcnow()
                )
            )
            await db.commit()
            
            logger.info(f"Processing batch: {batch.batch_id} ({batch.batch_type})")
            logger.info(f"Total items: {batch.total_items}")
        
        try:
            # Get all download tasks for this batch
            tasks = await self._get_batch_tasks(batch_id)
            
            if not tasks:
                logger.warning(f"No tasks found for batch {batch_id}")
                await self._complete_batch(batch_id, 0, 0, 0)
                return {
                    "success": True,
                    "batch_id": batch_id,
                    "processed": 0,
                    "successful": 0,
                    "failed": 0
                }
            
            logger.info(f"Found {len(tasks)} tasks to process")
            
            # Process tasks in batches
            processed = 0
            successful = 0
            failed = 0
            
            batch_size = 100
            for i in range(0, len(tasks), batch_size):
                batch_tasks = tasks[i:i + batch_size]
                
                # Process batch
                batch_results = await self._process_task_batch(batch_tasks)
                
                processed += len(batch_tasks)
                successful += batch_results["successful"]
                failed += batch_results["failed"]
                
                # Update progress
                await self._update_progress(batch_id, processed, successful, failed)
                
                # Save checkpoint
                if processed % self.checkpoint_interval == 0:
                    await self._save_checkpoint(batch_id, processed)
                
                progress_pct = (processed / len(tasks)) * 100
                logger.info(f"Progress: {processed}/{len(tasks)} ({progress_pct:.1f}%) - "
                          f"Success: {successful}, Failed: {failed}")
            
            # Mark batch as completed
            await self._complete_batch(batch_id, processed, successful, failed)
            
            logger.info(f"Batch {batch_id} completed: {successful}/{processed} successful")
            
            return {
                "success": True,
                "batch_id": batch_id,
                "processed": processed,
                "successful": successful,
                "failed": failed,
                "success_rate": (successful / processed * 100) if processed > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}", exc_info=True)
            await self._fail_batch(batch_id, str(e))
            return {
                "success": False,
                "batch_id": batch_id,
                "error": str(e)
            }
    
    async def _get_batch_tasks(self, batch_id: int) -> List[DownloadTask]:
        """Get all download tasks for a batch."""
        async with async_session_maker() as db:
            result = await db.execute(
                select(DownloadTask)
                .where(
                    and_(
                        DownloadTask.batch_id == batch_id,
                        DownloadTask.status == "queued"
                    )
                )
                .order_by(DownloadTask.priority.asc(), DownloadTask.created_at.asc())
            )
            return result.scalars().all()
    
    async def _process_task_batch(self, tasks: List[DownloadTask]) -> dict:
        """Process a batch of download tasks in parallel."""
        semaphore = asyncio.Semaphore(self.parallel_workers)
        
        async def process_task(task):
            async with semaphore:
                try:
                    # Update task status to processing
                    async with async_session_maker() as db:
                        await db.execute(
                            update(DownloadTask)
                            .where(DownloadTask.id == task.id)
                            .values(
                                status="processing",
                                started_at=datetime.utcnow()
                            )
                        )
                        await db.commit()
                    
                    # Retry the media download using direct method
                    success = await self._retry_media_file(task.media_file_id)
                    
                    # Update task status
                    async with async_session_maker() as db:
                        await db.execute(
                            update(DownloadTask)
                            .where(DownloadTask.id == task.id)
                            .values(
                                status="completed" if success else "failed",
                                completed_at=datetime.utcnow(),
                                error_message=None if success else "Download failed"
                            )
                        )
                        await db.commit()
                    
                    return success
                    
                except Exception as e:
                    logger.error(f"Error processing task {task.id}: {e}")
                    
                    # Mark task as failed
                    async with async_session_maker() as db:
                        await db.execute(
                            update(DownloadTask)
                            .where(DownloadTask.id == task.id)
                            .values(
                                status="failed",
                                completed_at=datetime.utcnow(),
                                error_message=str(e)[:500]
                            )
                        )
                        await db.commit()
                    
                    return False
        
        # Process all tasks in parallel
        results = await asyncio.gather(*[process_task(task) for task in tasks], return_exceptions=True)
        
        successful = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is False or isinstance(r, Exception))
        
        return {"successful": successful, "failed": failed}
    
    async def _retry_media_file(self, media_file_id: int) -> bool:
        """
        Retry downloading a single media file.
        
        This is a simplified version that just marks the task as ready for retry
        by the actual media retry service.
        """
        try:
            async with async_session_maker() as db:
                # Get media file
                result = await db.execute(
                    select(MediaFile).where(MediaFile.id == media_file_id)
                )
                media = result.scalar_one_or_none()
                
                if not media:
                    logger.error(f"Media file {media_file_id} not found")
                    return False
                
                # Check if already downloaded
                if media.file_path and os.path.exists(media.file_path):
                    logger.info(f"Media {media_file_id} already downloaded")
                    return True
                
                # Check retry attempts
                if media.download_attempts >= self.max_retries:
                    logger.warning(f"Media {media_file_id} exceeded max retries")
                    return False
                
                # Update download attempt count and mark for retry
                await db.execute(
                    update(MediaFile)
                    .where(MediaFile.id == media_file_id)
                    .values(
                        download_attempts=(media.download_attempts or 0) + 1,
                        last_download_attempt=datetime.utcnow(),
                        processing_status="queued"
                    )
                )
                await db.commit()
                
                logger.info(f"Media {media_file_id} marked for retry (attempt {(media.download_attempts or 0) + 1})")
                return True
                
        except Exception as e:
            logger.error(f"Error retrying media {media_file_id}: {e}")
            return False
    
    async def _update_progress(self, batch_id: int, processed: int, successful: int, failed: int):
        """Update batch processing progress."""
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == batch_id)
                .values(
                    processed_items=processed,
                    successful_items=successful,
                    failed_items=failed
                )
            )
            await db.commit()
    
    async def _save_checkpoint(self, batch_id: int, processed_items: int):
        """Save processing checkpoint."""
        checkpoint_data = f'{{"processed_items": {processed_items}, "timestamp": "{datetime.utcnow().isoformat()}"}}'
        
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == batch_id)
                .values(
                    last_checkpoint=datetime.utcnow(),
                    checkpoint_data=checkpoint_data
                )
            )
            await db.commit()
    
    async def _complete_batch(self, batch_id: int, processed: int, successful: int, failed: int):
        """Mark batch as completed."""
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == batch_id)
                .values(
                    status="completed",
                    processed_items=processed,
                    successful_items=successful,
                    failed_items=failed,
                    completed_at=datetime.utcnow()
                )
            )
            await db.commit()
    
    async def _fail_batch(self, batch_id: int, error_message: str):
        """Mark batch as failed."""
        async with async_session_maker() as db:
            await db.execute(
                update(BatchProcessing)
                .where(BatchProcessing.id == batch_id)
                .values(
                    status="failed",
                    error_summary=error_message[:1000],
                    completed_at=datetime.utcnow()
                )
            )
            await db.commit()
    
    async def process_all_pending_batches(self) -> List[dict]:
        """Process all pending batch jobs in priority order."""
        logger.info("Processing all pending batches")
        
        # Define priority order
        priority_order = {
            "empty_download": 1,      # CRITICAL
            "no_error_message": 2,    # HIGH
            "disconnected": 3,        # HIGH
            "message_not_exists": 4   # LOW
        }
        
        async with async_session_maker() as db:
            # Get all pending batches
            result = await db.execute(
                select(BatchProcessing)
                .where(BatchProcessing.status == "pending")
                .order_by(BatchProcessing.created_at.desc())
            )
            batches = result.scalars().all()
        
        if not batches:
            logger.info("No pending batches found")
            return []
        
        # Sort by priority
        def get_priority(batch):
            batch_type = batch.batch_id.split("_")[2] if "_" in batch.batch_id else "unknown"
            return priority_order.get(batch_type, 999)
        
        batches = sorted(batches, key=get_priority)
        
        logger.info(f"Found {len(batches)} pending batches")
        
        results = []
        for batch in batches:
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing batch: {batch.batch_id}")
            logger.info(f"Type: {batch.batch_type}, Items: {batch.total_items}")
            logger.info(f"{'='*80}\n")
            
            result = await self.process_batch(batch.id)
            results.append(result)
            
            # Brief pause between batches
            await asyncio.sleep(2)
        
        return results


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Process failed media download batches")
    parser.add_argument("--batch-id", type=int, help="Process specific batch ID")
    parser.add_argument("--all", action="store_true", help="Process all pending batches")
    parser.add_argument("--list", action="store_true", help="List all pending batches")
    
    args = parser.parse_args()
    
    processor = BatchProcessor()
    
    if args.list:
        # List all pending batches
        async with async_session_maker() as db:
            result = await db.execute(
                select(BatchProcessing)
                .where(BatchProcessing.status == "pending")
                .order_by(BatchProcessing.created_at.desc())
            )
            batches = result.scalars().all()
            
            if not batches:
                print("No pending batches found")
                return
            
            print(f"\nFound {len(batches)} pending batches:\n")
            print(f"{'ID':<5} {'Batch ID':<45} {'Type':<10} {'Items':<10} {'Status':<10}")
            print("-" * 90)
            
            for batch in batches:
                print(f"{batch.id:<5} {batch.batch_id:<45} {batch.batch_type:<10} {batch.total_items:<10} {batch.status:<10}")
            
            print()
            return
    
    if args.batch_id:
        # Process specific batch
        logger.info(f"Processing batch ID: {args.batch_id}")
        result = await processor.process_batch(args.batch_id)
        
        print("\n" + "="*80)
        print("BATCH PROCESSING RESULT")
        print("="*80)
        print(f"Success: {result['success']}")
        if result['success']:
            print(f"Processed: {result['processed']}")
            print(f"Successful: {result['successful']}")
            print(f"Failed: {result['failed']}")
            print(f"Success Rate: {result.get('success_rate', 0):.2f}%")
        else:
            print(f"Error: {result.get('error', 'Unknown error')}")
        print("="*80 + "\n")
        
    elif args.all:
        # Process all pending batches
        logger.info("Processing all pending batches")
        results = await processor.process_all_pending_batches()
        
        print("\n" + "="*80)
        print("ALL BATCHES PROCESSING RESULTS")
        print("="*80)
        
        total_processed = 0
        total_successful = 0
        total_failed = 0
        
        for result in results:
            if result['success']:
                total_processed += result['processed']
                total_successful += result['successful']
                total_failed += result['failed']
                print(f"Batch {result['batch_id']}: {result['successful']}/{result['processed']} successful ({result.get('success_rate', 0):.2f}%)")
            else:
                print(f"Batch {result['batch_id']}: FAILED - {result.get('error', 'Unknown error')}")
        
        print("-" * 80)
        print(f"TOTAL: {total_successful}/{total_processed} successful")
        if total_processed > 0:
            print(f"Overall Success Rate: {(total_successful/total_processed*100):.2f}%")
        print("="*80 + "\n")
        
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
