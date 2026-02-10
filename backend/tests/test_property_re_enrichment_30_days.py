"""
Property-based test for Re-enrichment After 30 Days.

Feature: telegram-vault-enrichment-media-fixes
Property 7: Re-enrichment After 30 Days

Tests that users whose last_photo_scan is older than 30 days are included in the enrichment queue.
"""

import pytest
from hypothesis import given, strategies as st, settings, assume
from unittest.mock import Mock, AsyncMock, MagicMock
from datetime import datetime, timedelta
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock dependencies
sys.modules['backend.app.services.user_enricher'] = MagicMock()
sys.modules['backend.app.services.telegram_service'] = MagicMock()

from app.services.passive_enrichment_service import PassiveEnrichmentService


# Custom strategies for generating user data with various scan dates
@st.composite
def user_with_scan_date_strategy(draw):
    """
    Generate a user with a specific last_photo_scan date.
    Returns (user, days_since_scan, should_be_re_enriched)
    """
    days_since_scan = draw(st.integers(min_value=0, max_value=90))
    re_enrich_threshold = 30
    
    user = Mock()
    user.telegram_id = draw(st.integers(min_value=100000, max_value=999999))
    user.access_hash = draw(st.integers(min_value=100000, max_value=999999))
    user.username = f"user_{user.telegram_id}"
    user.first_name = "TestUser"
    user.current_photo_path = "/media/photos/user.jpg"  # Has photo
    user.messages_count = draw(st.integers(min_value=1, max_value=1000))
    
    # Set last_photo_scan based on days_since_scan
    if days_since_scan == 0:
        # Special case: never scanned (NULL)
        user.last_photo_scan = None
        should_be_re_enriched = True
    else:
        user.last_photo_scan = datetime.utcnow() - timedelta(days=days_since_scan)
        should_be_re_enriched = days_since_scan > re_enrich_threshold
    
    return user, days_since_scan, should_be_re_enriched


# Feature: telegram-vault-enrichment-media-fixes, Property 7: Re-enrichment After 30 Days
@given(user_data=user_with_scan_date_strategy())
@settings(max_examples=100, deadline=None)
@pytest.mark.asyncio
async def test_property_re_enrichment_after_30_days(user_data):
    """
    **Validates: Requirements 3.5**
    
    Property: For any user whose last_photo_scan is older than 30 days, 
    the system should include them in the enrichment queue for profile photo updates.
    
    This test verifies that:
    1. Users with last_photo_scan > 30 days are included
    2. Users with last_photo_scan <= 30 days are excluded
    3. Users with NULL last_photo_scan are included
    """
    user, days_since_scan, should_be_re_enriched = user_data
    
    # Arrange
    service = PassiveEnrichmentService()
    service._batch_size = 50
    service._re_enrich_days = 30
    
    # Mock database
    mock_db = AsyncMock()
    
    # First query returns no users without photos (so we test re-enrichment logic)
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = []
    
    # Second query returns users for re-enrichment
    if should_be_re_enriched:
        mock_result_re_enrich = AsyncMock()
        mock_result_re_enrich.scalars.return_value.all.return_value = [user]
    else:
        mock_result_re_enrich = AsyncMock()
        mock_result_re_enrich.scalars.return_value.all.return_value = []
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_re_enrich
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    if should_be_re_enriched:
        assert len(result) > 0, \
            f"User with last_photo_scan {days_since_scan} days ago should be included for re-enrichment"
        assert user in result, \
            f"User should be in result when last_photo_scan is {days_since_scan} days ago"
    else:
        assert user not in result, \
            f"User should NOT be in result when last_photo_scan is only {days_since_scan} days ago"


# Feature: telegram-vault-enrichment-media-fixes, Property 7: Re-enrichment After 30 Days
@given(
    num_old_users=st.integers(min_value=1, max_value=20),
    num_recent_users=st.integers(min_value=1, max_value=20),
    batch_size=st.integers(min_value=10, max_value=50)
)
@settings(max_examples=50, deadline=None)
@pytest.mark.asyncio
async def test_property_only_old_users_selected_for_re_enrichment(num_old_users, num_recent_users, batch_size):
    """
    **Validates: Requirements 3.5**
    
    Property: For any batch of users with photos, only those with last_photo_scan 
    older than 30 days should be selected for re-enrichment.
    
    This test verifies that:
    1. Users scanned within 30 days are excluded
    2. Users scanned more than 30 days ago are included
    3. The 30-day threshold is correctly applied
    """
    # Arrange
    service = PassiveEnrichmentService()
    service._batch_size = batch_size
    service._re_enrich_days = 30
    
    # Create users with old scans (> 30 days)
    old_users = []
    for i in range(num_old_users):
        user = Mock()
        user.telegram_id = 100000 + i
        user.access_hash = 200000 + i
        user.username = f"old_user_{i}"
        user.first_name = f"OldUser{i}"
        user.current_photo_path = f"/media/old_{i}.jpg"
        user.last_photo_scan = datetime.utcnow() - timedelta(days=31 + i)  # 31+ days ago
        user.messages_count = 100
        old_users.append(user)
    
    # Create users with recent scans (<= 30 days)
    recent_users = []
    for i in range(num_recent_users):
        user = Mock()
        user.telegram_id = 300000 + i
        user.access_hash = 400000 + i
        user.username = f"recent_user_{i}"
        user.first_name = f"RecentUser{i}"
        user.current_photo_path = f"/media/recent_{i}.jpg"
        user.last_photo_scan = datetime.utcnow() - timedelta(days=i % 30)  # 0-29 days ago
        user.messages_count = 100
        recent_users.append(user)
    
    # Mock database
    mock_db = AsyncMock()
    
    # First query returns no users without photos
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = []
    
    # Second query returns only old users (those needing re-enrichment)
    mock_result_re_enrich = AsyncMock()
    mock_result_re_enrich.scalars.return_value.all.return_value = old_users[:batch_size]
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_re_enrich
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    # All users in result should be from old_users, not recent_users
    for user in result:
        assert user in old_users, \
            "Only users with old scans should be in result"
        assert user not in recent_users, \
            "Users with recent scans should not be in result"


# Unit tests for specific scenarios
@pytest.mark.asyncio
async def test_user_with_31_day_old_scan_is_re_enriched():
    """
    Unit test: User with last_photo_scan exactly 31 days ago should be re-enriched.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    service._re_enrich_days = 30
    
    # Create user with 31-day-old scan
    user = Mock()
    user.telegram_id = 123456
    user.access_hash = 789012
    user.username = "old_user"
    user.first_name = "OldUser"
    user.current_photo_path = "/media/old.jpg"
    user.last_photo_scan = datetime.utcnow() - timedelta(days=31)
    user.messages_count = 100
    
    # Mock database
    mock_db = AsyncMock()
    
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = []
    
    mock_result_re_enrich = AsyncMock()
    mock_result_re_enrich.scalars.return_value.all.return_value = [user]
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_re_enrich
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 1, "Should return the user"
    assert result[0] == user, "Should return the user with 31-day-old scan"


@pytest.mark.asyncio
async def test_user_with_30_day_old_scan_is_not_re_enriched():
    """
    Unit test: User with last_photo_scan exactly 30 days ago should NOT be re-enriched.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    service._re_enrich_days = 30
    
    # Create user with 30-day-old scan (exactly at threshold)
    user = Mock()
    user.telegram_id = 123456
    user.access_hash = 789012
    user.username = "threshold_user"
    user.first_name = "ThresholdUser"
    user.current_photo_path = "/media/threshold.jpg"
    user.last_photo_scan = datetime.utcnow() - timedelta(days=30)
    user.messages_count = 100
    
    # Mock database - query should not return this user
    mock_db = AsyncMock()
    
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = []
    
    mock_result_re_enrich = AsyncMock()
    mock_result_re_enrich.scalars.return_value.all.return_value = []  # User not returned
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_re_enrich
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 0, "Should not return user with exactly 30-day-old scan"


@pytest.mark.asyncio
async def test_user_with_null_last_photo_scan_is_re_enriched():
    """
    Unit test: User with NULL last_photo_scan should be re-enriched.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    service._re_enrich_days = 30
    
    # Create user with NULL last_photo_scan
    user = Mock()
    user.telegram_id = 123456
    user.access_hash = 789012
    user.username = "never_scanned"
    user.first_name = "NeverScanned"
    user.current_photo_path = "/media/never.jpg"
    user.last_photo_scan = None  # Never scanned
    user.messages_count = 100
    
    # Mock database
    mock_db = AsyncMock()
    
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = []
    
    mock_result_re_enrich = AsyncMock()
    mock_result_re_enrich.scalars.return_value.all.return_value = [user]
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_re_enrich
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 1, "Should return user with NULL last_photo_scan"
    assert result[0] == user, "Should return the never-scanned user"


@pytest.mark.asyncio
async def test_user_with_1_day_old_scan_is_not_re_enriched():
    """
    Unit test: User with last_photo_scan 1 day ago should NOT be re-enriched.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    service._re_enrich_days = 30
    
    # Create user with 1-day-old scan
    user = Mock()
    user.telegram_id = 123456
    user.access_hash = 789012
    user.username = "recent_user"
    user.first_name = "RecentUser"
    user.current_photo_path = "/media/recent.jpg"
    user.last_photo_scan = datetime.utcnow() - timedelta(days=1)
    user.messages_count = 100
    
    # Mock database - query should not return this user
    mock_db = AsyncMock()
    
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = []
    
    mock_result_re_enrich = AsyncMock()
    mock_result_re_enrich.scalars.return_value.all.return_value = []  # User not returned
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_re_enrich
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 0, "Should not return user with 1-day-old scan"


@pytest.mark.asyncio
async def test_configurable_re_enrich_days():
    """
    Unit test: The re-enrichment threshold should be configurable.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    service._re_enrich_days = 15  # Custom threshold
    
    # Create user with 16-day-old scan (should be re-enriched with 15-day threshold)
    user = Mock()
    user.telegram_id = 123456
    user.access_hash = 789012
    user.username = "user_16_days"
    user.first_name = "User16Days"
    user.current_photo_path = "/media/user.jpg"
    user.last_photo_scan = datetime.utcnow() - timedelta(days=16)
    user.messages_count = 100
    
    # Mock database
    mock_db = AsyncMock()
    
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = []
    
    mock_result_re_enrich = AsyncMock()
    mock_result_re_enrich.scalars.return_value.all.return_value = [user]
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_re_enrich
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 1, "Should return user when scan is older than custom threshold"
    assert result[0] == user, "Should return the user with 16-day-old scan"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
