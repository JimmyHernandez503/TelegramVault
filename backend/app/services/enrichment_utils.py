"""
Enrichment Utilities

Shared utility functions for triggering user enrichment consistently across all components.
"""

from typing import Optional
from telethon import TelegramClient
from backend.app.services.enhanced_user_enricher_service import EnhancedUserEnricherService
from backend.app.services.enrichment_status_tracker import EnrichmentStatusTracker
from backend.app.core.download_queue_manager import TaskPriority

# Global instances
_enricher_service: Optional[EnhancedUserEnricherService] = None
_status_tracker: Optional[EnrichmentStatusTracker] = None


def get_enricher_service() -> EnhancedUserEnricherService:
    """Get or create the global enricher service instance"""
    global _enricher_service
    if _enricher_service is None:
        _enricher_service = EnhancedUserEnricherService()
    return _enricher_service


def get_status_tracker() -> EnrichmentStatusTracker:
    """Get or create the global status tracker instance"""
    global _status_tracker
    if _status_tracker is None:
        _status_tracker = EnrichmentStatusTracker()
    return _status_tracker


async def trigger_user_enrichment(
    client: TelegramClient,
    telegram_id: int,
    group_id: Optional[int] = None,
    source: str = "unknown"
) -> bool:
    """
    Trigger user enrichment with automatic WebSocket notification.
    
    This is the standard way to trigger enrichment across all components.
    
    Args:
        client: Telegram client
        telegram_id: User's Telegram ID
        group_id: Optional group ID for context
        source: Source component triggering enrichment
        
    Returns:
        bool: True if enrichment was queued, False if skipped
    """
    # Skip if telegram_id is negative (channel/group, not a user)
    if telegram_id < 0:
        return False
    
    enricher = get_enricher_service()
    tracker = get_status_tracker()
    
    # Check if enrichment is needed (cache check)
    if not await tracker.is_enrichment_needed(telegram_id):
        return False
    
    # Check if user already has complete data
    if await is_user_data_complete(telegram_id):
        return False
    
    # Queue enrichment with notification
    await enricher.queue_enrichment(
        client=client,
        telegram_id=telegram_id,
        group_id=group_id,
        priority=TaskPriority.NORMAL
    )
    
    return True


async def is_user_data_complete(telegram_id: int) -> bool:
    """
    Check if user has complete data (name, username, and photo).
    
    Args:
        telegram_id: User's Telegram ID
        
    Returns:
        bool: True if user has complete data, False otherwise
    """
    from backend.app.db.database import async_session_maker
    from backend.app.models.telegram_user import TelegramUser
    from sqlalchemy import select
    from datetime import datetime, timedelta
    
    try:
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramUser).where(TelegramUser.telegram_id == telegram_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                return False
            
            # Check if user has complete data
            has_name = bool(user.first_name or user.username)
            has_photo = bool(user.current_photo_path)
            
            # If user has both name and photo, check if photo is recent (within 30 days)
            # This allows re-enrichment of users to get updated photos
            if has_name and has_photo:
                # Check last_photo_scan timestamp
                if user.last_photo_scan:
                    days_since_scan = (datetime.utcnow() - user.last_photo_scan).days
                    # Re-enrich if photo scan is older than 30 days
                    if days_since_scan > 30:
                        return False
                return True
            
            # User is incomplete if missing name or photo
            return False
            
    except Exception:
        # If there's an error checking, assume incomplete to be safe
        return False


async def get_enrichment_metrics() -> dict:
    """
    Get enrichment metrics and statistics.
    
    Returns:
        dict: Enrichment metrics including success rates, cache stats, and timing
    """
    tracker = get_status_tracker()
    return await tracker.get_metrics()


async def get_enrichment_statistics() -> dict:
    """
    Get detailed enrichment statistics.
    
    Returns:
        dict: Detailed statistics including status counts and recent failures
    """
    tracker = get_status_tracker()
    return await tracker.get_statistics()
