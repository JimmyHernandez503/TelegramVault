"""
Property-based test for Retry Attempt Limits.

Feature: telegram-vault-enrichment-media-fixes
Property 10: Retry Attempt Limits

Tests that media items are marked as permanently failed after 3 failed download attempts.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock dependencies
sys.modules['backend.app.services.telegram_service'] = MagicMock()

from app.services.media_retry_service import MediaRetryService


# Custom strategies for generating media with various attempt counts
@st.composite
def media_with_attempts_strategy(draw):
    """
    Generate media file with specific download attempt count.
    Returns (media, should_be_retried, should_be_marked_failed)
    """
    attempt_count = draw(st.integers(min_value=0, max_value=5))
    max_retries = 3
    
    media = Mock()
    media.id = draw(st.integers(min_value=1, max_value=999999))
    media.message_id = draw(st.integers(min_value=1, max_value=999999))
    media.file_type = draw(st.sampled_from(["photo", "video", "document", "audio"]))
    media.file_path = None  # Pending download
    media.download_attempts = attempt_count
    media.validation_status = "pending"
    media.processing_status = "pending"
    media.is_duplicate = False
    
    should_be_retried = attempt_count < max_retries
    should_be_marked_failed = attempt_count >= max_retries
    
    return media, should_be_retried, should_be_marked_failed, max_retries


# Feature: telegram-vault-enrichment-media-fixes, Property 10: Retry Attempt Limits
@given(media_data=media_with_attempts_strategy())
@settings(max_examples=50, deadline=None)
@pytest.mark.asyncio
async def test_property_retry_attempt_limits(media_data):
    """
    **Validates: Requirements 4.3, 5.4**
    
    Property: For any media item, after 3 failed download attempts it should be 
    marked as permanently failed and not retried further.
    
    This test verifies that:
    1. Items with < 3 attempts are retried
    2. Items with >= 3 attempts are marked as permanently failed
    3. Permanently failed items are not retried again
    """
    media, should_be_retried, should_be_marked_failed, max_retries = media_data
    
    # Arrange
    service = MediaRetryService()
    service._settings["max_retries"] = max_retries
    
    # Mock database session
    mock_db = AsyncMock()
    
    # Mock the media query
    mock_result = AsyncMock()
    mock_result.scalars.return_value.first.return_value = media
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()
    
    # Track what values were set in the update
    update_values = {}
    
    original_execute = mock_db.execute
    async def capture_execute(*args, **kwargs):
        # Capture update values if this is an UPDATE statement
        if len(args) > 0:
            stmt = args[0]
            if hasattr(stmt, '_values') and stmt._values:
                update_values.update(stmt._values)
        return await original_execute(*args, **kwargs)
    
    mock_db.execute = capture_execute
    
    # Act
    with patch('backend.app.services.media_retry_service.async_session_maker') as mock_session_maker:
        mock_session_maker.return_value.__aenter__.return_value = mock_db
        
        # Call retry_single_media
        result = await service.retry_single_media(media.id)
    
    # Assert
    if should_be_marked_failed:
        # Should be marked as permanently failed
        assert "processing_status" in update_values, \
            f"Media with {media.download_attempts} attempts should have processing_status updated"
        assert update_values.get("processing_status") == "permanently_failed", \
            f"Media with {media.download_attempts} attempts should be marked as permanently_failed"
        assert "download_error" in update_values, \
            "Permanently failed items should have error message"
        assert result is False, \
            f"retry_single_media should return False for media with {media.download_attempts} attempts"
    elif should_be_retried:
        # Should attempt retry (increment attempts)
        if "download_attempts" in update_values:
            assert update_values["download_attempts"] == media.download_attempts + 1, \
                f"Download attempts should be incremented from {media.download_attempts}"


# Feature: telegram-vault-enrichment-media-fixes, Property 10: Retry Attempt Limits
@given(
    initial_attempts=st.integers(min_value=0, max_value=2),
    num_failures=st.integers(min_value=1, max_value=5)
)
@settings(max_examples=50, deadline=None)
@pytest.mark.asyncio
async def test_property_max_retries_enforced(initial_attempts, num_failures):
    """
    **Validates: Requirements 4.3, 5.4**
    
    Property: For any media item, regardless of how many times retry is called,
    it should never exceed the maximum retry attempts.
    
    This test verifies that:
    1. The max_retries limit is enforced
    2. Items are marked as permanently failed when limit is reached
    3. Further retry attempts are rejected
    """
    # Arrange
    service = MediaRetryService()
    max_retries = 3
    service._settings["max_retries"] = max_retries
    
    media = Mock()
    media.id = 123456
    media.message_id = 789012
    media.file_type = "photo"
    media.file_path = None
    media.download_attempts = initial_attempts
    media.validation_status = "pending"
    media.processing_status = "pending"
    media.is_duplicate = False
    
    # Mock database
    mock_db = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalars.return_value.first.return_value = media
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()
    
    # Track updates
    updates = []
    
    original_execute = mock_db.execute
    async def capture_execute(*args, **kwargs):
        if len(args) > 0:
            stmt = args[0]
            if hasattr(stmt, '_values') and stmt._values:
                updates.append(stmt._values.copy())
        return await original_execute(*args, **kwargs)
    
    mock_db.execute = capture_execute
    
    # Act - Try to retry multiple times
    with patch('backend.app.services.media_retry_service.async_session_maker') as mock_session_maker:
        mock_session_maker.return_value.__aenter__.return_value = mock_db
        
        for i in range(num_failures):
            # Update media attempts to simulate failures
            if media.download_attempts < max_retries:
                media.download_attempts += 1
            
            result = await service.retry_single_media(media.id)
            
            # Once we hit max_retries, all subsequent calls should return False
            if media.download_attempts >= max_retries:
                assert result is False, \
                    f"Should return False when attempts ({media.download_attempts}) >= max_retries ({max_retries})"
    
    # Assert - Check that permanently_failed was set when limit reached
    if initial_attempts + num_failures >= max_retries:
        permanently_failed_updates = [u for u in updates if u.get("processing_status") == "permanently_failed"]
        assert len(permanently_failed_updates) > 0, \
            "Should have at least one update marking item as permanently_failed"


# Unit tests for specific scenarios
@pytest.mark.asyncio
async def test_media_with_0_attempts_is_retried():
    """
    Unit test: Media with 0 attempts should be retried.
    """
    service = MediaRetryService()
    service._settings["max_retries"] = 3
    
    media = Mock()
    media.id = 123456
    media.message_id = 789012
    media.file_type = "photo"
    media.file_path = None
    media.download_attempts = 0
    media.validation_status = "pending"
    media.processing_status = "pending"
    media.is_duplicate = False
    
    mock_db = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalars.return_value.first.return_value = media
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()
    
    update_values = {}
    original_execute = mock_db.execute
    async def capture_execute(*args, **kwargs):
        if len(args) > 0:
            stmt = args[0]
            if hasattr(stmt, '_values') and stmt._values:
                update_values.update(stmt._values)
        return await original_execute(*args, **kwargs)
    
    mock_db.execute = capture_execute
    
    with patch('backend.app.services.media_retry_service.async_session_maker') as mock_session_maker:
        mock_session_maker.return_value.__aenter__.return_value = mock_db
        
        await service.retry_single_media(media.id)
    
    # Should increment attempts
    assert update_values.get("download_attempts") == 1, \
        "Should increment attempts from 0 to 1"


@pytest.mark.asyncio
async def test_media_with_2_attempts_is_retried():
    """
    Unit test: Media with 2 attempts should be retried (one more time).
    """
    service = MediaRetryService()
    service._settings["max_retries"] = 3
    
    media = Mock()
    media.id = 123456
    media.message_id = 789012
    media.file_type = "photo"
    media.file_path = None
    media.download_attempts = 2
    media.validation_status = "pending"
    media.processing_status = "pending"
    media.is_duplicate = False
    
    mock_db = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalars.return_value.first.return_value = media
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()
    
    update_values = {}
    original_execute = mock_db.execute
    async def capture_execute(*args, **kwargs):
        if len(args) > 0:
            stmt = args[0]
            if hasattr(stmt, '_values') and stmt._values:
                update_values.update(stmt._values)
        return await original_execute(*args, **kwargs)
    
    mock_db.execute = capture_execute
    
    with patch('backend.app.services.media_retry_service.async_session_maker') as mock_session_maker:
        mock_session_maker.return_value.__aenter__.return_value = mock_db
        
        await service.retry_single_media(media.id)
    
    # Should increment attempts
    assert update_values.get("download_attempts") == 3, \
        "Should increment attempts from 2 to 3"


@pytest.mark.asyncio
async def test_media_with_3_attempts_is_marked_permanently_failed():
    """
    Unit test: Media with 3 attempts should be marked as permanently failed.
    """
    service = MediaRetryService()
    service._settings["max_retries"] = 3
    
    media = Mock()
    media.id = 123456
    media.message_id = 789012
    media.file_type = "photo"
    media.file_path = None
    media.download_attempts = 3
    media.validation_status = "pending"
    media.processing_status = "pending"
    media.is_duplicate = False
    
    mock_db = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalars.return_value.first.return_value = media
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()
    
    update_values = {}
    original_execute = mock_db.execute
    async def capture_execute(*args, **kwargs):
        if len(args) > 0:
            stmt = args[0]
            if hasattr(stmt, '_values') and stmt._values:
                update_values.update(stmt._values)
        return await original_execute(*args, **kwargs)
    
    mock_db.execute = capture_execute
    
    with patch('backend.app.services.media_retry_service.async_session_maker') as mock_session_maker:
        mock_session_maker.return_value.__aenter__.return_value = mock_db
        
        result = await service.retry_single_media(media.id)
    
    # Should be marked as permanently failed
    assert result is False, "Should return False for media with 3 attempts"
    assert update_values.get("processing_status") == "permanently_failed", \
        "Should mark as permanently_failed"
    assert "download_error" in update_values, \
        "Should set error message"


@pytest.mark.asyncio
async def test_media_with_4_attempts_is_not_retried():
    """
    Unit test: Media with 4 attempts (already exceeded) should not be retried.
    """
    service = MediaRetryService()
    service._settings["max_retries"] = 3
    
    media = Mock()
    media.id = 123456
    media.message_id = 789012
    media.file_type = "photo"
    media.file_path = None
    media.download_attempts = 4
    media.validation_status = "pending"
    media.processing_status = "permanently_failed"
    media.is_duplicate = False
    
    mock_db = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalars.return_value.first.return_value = media
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()
    
    with patch('backend.app.services.media_retry_service.async_session_maker') as mock_session_maker:
        mock_session_maker.return_value.__aenter__.return_value = mock_db
        
        result = await service.retry_single_media(media.id)
    
    # Should return False without attempting retry
    assert result is False, "Should return False for media with 4 attempts"


@pytest.mark.asyncio
async def test_permanently_failed_status_persists():
    """
    Unit test: Once marked as permanently_failed, the status should persist.
    """
    service = MediaRetryService()
    service._settings["max_retries"] = 3
    
    media = Mock()
    media.id = 123456
    media.message_id = 789012
    media.file_type = "photo"
    media.file_path = None
    media.download_attempts = 3
    media.validation_status = "pending"
    media.processing_status = "pending"
    media.is_duplicate = False
    
    mock_db = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalars.return_value.first.return_value = media
    mock_db.execute = AsyncMock(return_value=mock_result)
    mock_db.commit = AsyncMock()
    
    updates = []
    original_execute = mock_db.execute
    async def capture_execute(*args, **kwargs):
        if len(args) > 0:
            stmt = args[0]
            if hasattr(stmt, '_values') and stmt._values:
                updates.append(stmt._values.copy())
        return await original_execute(*args, **kwargs)
    
    mock_db.execute = capture_execute
    
    with patch('backend.app.services.media_retry_service.async_session_maker') as mock_session_maker:
        mock_session_maker.return_value.__aenter__.return_value = mock_db
        
        # First call - should mark as permanently failed
        result1 = await service.retry_single_media(media.id)
        assert result1 is False
        
        # Update media to reflect the change
        media.processing_status = "permanently_failed"
        
        # Second call - should still return False
        result2 = await service.retry_single_media(media.id)
        assert result2 is False
    
    # Should have marked as permanently_failed
    permanently_failed_updates = [u for u in updates if u.get("processing_status") == "permanently_failed"]
    assert len(permanently_failed_updates) > 0, \
        "Should have marked as permanently_failed"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
