#!/usr/bin/env python3
"""
Bulk User Enrichment Script

This script queues all existing users in the database for enrichment.
It's designed to be run once to enrich historical users that were created
before the user enricher worker was implemented.
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from sqlalchemy import select
from backend.app.db.database import async_session_maker
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_account import TelegramAccount
from backend.app.services.telegram_service import telegram_manager
from backend.app.services.user_enricher import user_enricher
from backend.app.core.logging_config import get_logger

logger = get_logger("bulk_enrich")


async def bulk_enrich_users(batch_size: int = 100, skip_enriched: bool = True):
    """
    Queue all users for enrichment in batches.
    
    Args:
        batch_size: Number of users to process in each batch
        skip_enriched: If True, skip users that already have username or first_name
    """
    logger.info("Starting bulk user enrichment...")
    
    # Get a connected Telegram client
    async with async_session_maker() as db:
        result = await db.execute(
            select(TelegramAccount).where(TelegramAccount.is_active == True)
        )
        accounts = result.scalars().all()
        
        if not accounts:
            logger.error("No active Telegram accounts found. Please connect an account first.")
            return
        
        # Try to get a connected client
        client = None
        for account in accounts:
            test_client = telegram_manager.clients.get(account.id)
            if test_client and test_client.is_connected():
                client = test_client
                logger.info(f"Using account {account.id} ({account.phone}) for enrichment")
                break
        
        if not client:
            # Try to connect the first account
            logger.info(f"No connected clients found. Connecting account {accounts[0].id}...")
            await telegram_manager.connect_account(accounts[0].id, db)
            client = telegram_manager.clients.get(accounts[0].id)
            
            if not client or not client.is_connected():
                logger.error("Failed to connect to Telegram. Cannot proceed with enrichment.")
                return
    
    # Start the enricher worker if not already running
    status = user_enricher.get_status()
    if not status["running"]:
        logger.info("Starting user enricher worker...")
        await user_enricher.start_worker()
        await asyncio.sleep(2)  # Give worker time to start
    
    # Query users in batches
    offset = 0
    total_queued = 0
    total_skipped = 0
    
    while True:
        async with async_session_maker() as db:
            query = select(TelegramUser).order_by(TelegramUser.id)
            
            if skip_enriched:
                # Skip users that already have basic info
                query = query.where(
                    (TelegramUser.username.is_(None)) & 
                    (TelegramUser.first_name.is_(None))
                )
            
            query = query.offset(offset).limit(batch_size)
            
            result = await db.execute(query)
            users = result.scalars().all()
            
            if not users:
                break
            
            logger.info(f"Processing batch at offset {offset}, found {len(users)} users")
            
            for user in users:
                # Skip deleted users
                if user.is_deleted:
                    total_skipped += 1
                    continue
                
                # Skip users without access_hash (can't enrich them)
                if not user.access_hash:
                    total_skipped += 1
                    continue
                
                # Queue for enrichment
                await user_enricher.queue_enrichment(
                    client=client,
                    telegram_id=user.telegram_id,
                    group_id=None,
                    source="bulk_enrich"
                )
                total_queued += 1
            
            offset += batch_size
            
            # Log progress every 10 batches
            if offset % (batch_size * 10) == 0:
                queue_status = user_enricher.get_status()
                logger.info(
                    f"Progress: {total_queued} users queued, {total_skipped} skipped, "
                    f"queue size: {queue_status['queue_size']}, "
                    f"processed: {queue_status['processed_users']}"
                )
    
    logger.info(
        f"Bulk enrichment queueing complete! "
        f"Total queued: {total_queued}, Total skipped: {total_skipped}"
    )
    
    # Wait for queue to be processed
    logger.info("Waiting for enrichment queue to be processed...")
    while True:
        status = user_enricher.get_status()
        queue_size = status["queue_size"]
        processed = status["processed_users"]
        
        if queue_size == 0:
            logger.info(f"All users processed! Total enriched: {processed}")
            break
        
        logger.info(f"Queue size: {queue_size}, Processed: {processed}")
        await asyncio.sleep(10)


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Bulk enrich all users in the database")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of users to process in each batch (default: 100)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Enrich all users, including those already enriched"
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Don't wait for queue to be processed"
    )
    
    args = parser.parse_args()
    
    try:
        await bulk_enrich_users(
            batch_size=args.batch_size,
            skip_enriched=not args.all
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error during bulk enrichment: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
