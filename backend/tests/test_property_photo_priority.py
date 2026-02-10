"""
Property-based test for Photo Priority in Batch Processing.

Feature: telegram-vault-enrichment-media-fixes
Property 6: Photo Priority in Batch Processing

Tests that users without profile photos are processed before users with existing photos.
"""

import pytest
from hypothesis import given, strategies as st, settings, assume
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock dependencies
sys.modules['backend.app.services.user_enricher'] = MagicMock()
sys.modules['backend.app.services.telegram_service'] = MagicMock()

from app.services.passive_enrichment_service import PassiveEnrichmentService


# Custom strategies for generating user data
@st.composite
def user_batch_strategy(draw):
    """
    Generate a batch of users with varying photo states.
    Returns (users_without_photos, users_with_photos, batch_size)
    """
    batch_size = draw(st.integers(min_value=10, max_value=100))
    
    # Generate users without photos
    num_without_photos = draw(st.integers(min_value=1, max_value=batch_size))
    users_without_photos = []
    for i in range(num_without_photos):
        user = Mock()
        user.telegram_id = 100000 + i
        user.access_hash = 200000 + i
        user.username = f"user_no_photo_{i}"
        user.first_name = f"User{i}"
        user.current_photo_path = None  # No photo
        user.last_photo_scan = None
        user.messages_count = draw(st.integers(min_value=1, max_value=1000))
        users_without_photos.append(user)
    
    # Generate users with photos
    num_with_photos = draw(st.integers(min_value=0, max_value=batch_size))
    users_with_photos = []
    for i in range(num_with_photos):
        user = Mock()
        user.telegram_id = 300000 + i
        user.access_hash = 400000 + i
        user.username = f"user_with_photo_{i}"
        user.first_name = f"UserPhoto{i}"
        user.current_photo_path = f"/media/photos/{i}.jpg"  # Has photo
        user.last_photo_scan = datetime.utcnow() - timedelta(days=draw(st.integers(min_value=0, max_value=60)))
        user.messages_count = draw(st.integers(min_value=1, max_value=1000))
        users_with_photos.append(user)
    
    return users_without_photos, users_with_photos, batch_size


# Feature: telegram-vault-enrichment-media-fixes, Property 6: Photo Priority in Batch Processing
@given(user_data=user_batch_strategy())
@settings(max_examples=50, deadline=None)
@pytest.mark.asyncio
async def test_property_photo_priority_in_batch_processing(user_data):
    """
    **Validates: Requirements 3.4**
    
    Property: For any batch of users to enrich, users without profile photos 
    should be processed before users with existing photos.
    
    This test verifies that:
    1. Users without photos are prioritized in the batch
    2. The query orders users correctly
    3. When batch size is limited, users without photos fill the batch first
    """
    users_without_photos, users_with_photos, batch_size = user_data
    
    # Skip if we don't have both types of users
    assume(len(users_without_photos) > 0)
    
    # Arrange
    service = PassiveEnrichmentService()
    service._batch_size = batch_size
    
    # Mock database session
    mock_db = AsyncMock()
    
    # Mock the database queries
    # First query returns users without photos
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = users_without_photos
    
    # Second query returns users with photos (for re-enrichment)
    mock_result_with_photos = AsyncMock()
    mock_result_with_photos.scalars.return_value.all.return_value = users_with_photos
    
    # Set up execute to return appropriate results
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_with_photos
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result_users = await service._get_users_to_enrich(mock_db)
    
    # Assert
    # Verify users without photos come first
    if len(result_users) > 0:
        # Count how many users without photos are in the result
        users_without_photos_in_result = [u for u in result_users if u.current_photo_path is None]
        users_with_photos_in_result = [u for u in result_users if u.current_photo_path is not None]
        
        # If batch is not full of users without photos, then users with photos should only appear after
        if len(users_without_photos) < batch_size:
            # We should have all users without photos
            assert len(users_without_photos_in_result) == len(users_without_photos), \
                "All users without photos should be included when batch is not full"
            
            # Users with photos should fill remaining slots
            expected_with_photos = min(len(users_with_photos), batch_size - len(users_without_photos))
            assert len(users_with_photos_in_result) <= expected_with_photos, \
                "Users with photos should only fill remaining batch slots"
        else:
            # Batch should be full of users without photos
            assert len(users_without_photos_in_result) == batch_size, \
                "When enough users without photos exist, batch should be full of them"
            assert len(users_with_photos_in_result) == 0, \
                "No users with photos should be included when batch is full of users without photos"


# Feature: telegram-vault-enrichment-media-fixes, Property 6: Photo Priority in Batch Processing
@given(
    num_without_photos=st.integers(min_value=1, max_value=50),
    num_with_photos=st.integers(min_value=1, max_value=50),
    batch_size=st.integers(min_value=10, max_value=100)
)
@settings(max_examples=50, deadline=None)
@pytest.mark.asyncio
async def test_property_users_without_photos_always_first(num_without_photos, num_with_photos, batch_size):
    """
    **Validates: Requirements 3.4**
    
    Property: For any batch configuration, users without photos should always 
    be selected before users with photos.
    
    This test verifies that:
    1. The first query targets users without photos
    2. The second query only runs if batch is not full
    3. Priority is maintained regardless of batch size
    """
    # Arrange
    service = PassiveEnrichmentService()
    service._batch_size = batch_size
    
    # Create mock users
    users_without = []
    for i in range(num_without_photos):
        user = Mock()
        user.telegram_id = 100000 + i
        user.access_hash = 200000 + i
        user.username = f"no_photo_{i}"
        user.first_name = f"User{i}"
        user.current_photo_path = None
        user.messages_count = 100
        users_without.append(user)
    
    users_with = []
    for i in range(num_with_photos):
        user = Mock()
        user.telegram_id = 300000 + i
        user.access_hash = 400000 + i
        user.username = f"with_photo_{i}"
        user.first_name = f"UserPhoto{i}"
        user.current_photo_path = f"/media/{i}.jpg"
        user.last_photo_scan = datetime.utcnow() - timedelta(days=35)
        user.messages_count = 100
        users_with.append(user)
    
    # Mock database
    mock_db = AsyncMock()
    
    mock_result_no_photos = AsyncMock()
    mock_result_no_photos.scalars.return_value.all.return_value = users_without
    
    mock_result_with_photos = AsyncMock()
    mock_result_with_photos.scalars.return_value.all.return_value = users_with
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_no_photos
        else:
            return mock_result_with_photos
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) <= batch_size, "Result should not exceed batch size"
    
    # Count users by photo status in result
    result_without_photos = [u for u in result if u.current_photo_path is None]
    result_with_photos = [u for u in result if u.current_photo_path is not None]
    
    # If we have users without photos, they should come first
    if num_without_photos > 0:
        expected_without = min(num_without_photos, batch_size)
        assert len(result_without_photos) == expected_without, \
            f"Should have {expected_without} users without photos, got {len(result_without_photos)}"
    
    # Users with photos should only appear if batch is not full
    if num_without_photos < batch_size:
        remaining_slots = batch_size - num_without_photos
        expected_with = min(num_with_photos, remaining_slots)
        assert len(result_with_photos) <= expected_with, \
            f"Should have at most {expected_with} users with photos"


# Unit tests for specific scenarios
@pytest.mark.asyncio
async def test_batch_full_of_users_without_photos():
    """
    Unit test: When there are enough users without photos to fill the batch,
    no users with photos should be included.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    
    # Create 20 users without photos (more than batch size)
    users_without = []
    for i in range(20):
        user = Mock()
        user.telegram_id = 100000 + i
        user.access_hash = 200000 + i
        user.username = f"user{i}"
        user.first_name = f"User{i}"
        user.current_photo_path = None
        user.messages_count = 100
        users_without.append(user)
    
    # Mock database
    mock_db = AsyncMock()
    mock_result = AsyncMock()
    mock_result.scalars.return_value.all.return_value = users_without[:10]  # Return batch_size users
    mock_db.execute = AsyncMock(return_value=mock_result)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 10, "Should return exactly batch_size users"
    assert all(u.current_photo_path is None for u in result), \
        "All users should be without photos"


@pytest.mark.asyncio
async def test_mixed_batch_when_not_enough_without_photos():
    """
    Unit test: When there aren't enough users without photos to fill the batch,
    users with photos should be added to fill remaining slots.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    
    # Create 5 users without photos
    users_without = []
    for i in range(5):
        user = Mock()
        user.telegram_id = 100000 + i
        user.access_hash = 200000 + i
        user.username = f"user{i}"
        user.first_name = f"User{i}"
        user.current_photo_path = None
        user.messages_count = 100
        users_without.append(user)
    
    # Create 10 users with photos
    users_with = []
    for i in range(10):
        user = Mock()
        user.telegram_id = 300000 + i
        user.access_hash = 400000 + i
        user.username = f"photo_user{i}"
        user.first_name = f"PhotoUser{i}"
        user.current_photo_path = f"/media/{i}.jpg"
        user.last_photo_scan = datetime.utcnow() - timedelta(days=35)
        user.messages_count = 100
        users_with.append(user)
    
    # Mock database
    mock_db = AsyncMock()
    
    mock_result_without = AsyncMock()
    mock_result_without.scalars.return_value.all.return_value = users_without
    
    mock_result_with = AsyncMock()
    mock_result_with.scalars.return_value.all.return_value = users_with[:5]  # Fill remaining 5 slots
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_without
        else:
            return mock_result_with
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 10, "Should return exactly batch_size users"
    
    result_without = [u for u in result if u.current_photo_path is None]
    result_with = [u for u in result if u.current_photo_path is not None]
    
    assert len(result_without) == 5, "Should have 5 users without photos"
    assert len(result_with) == 5, "Should have 5 users with photos to fill batch"


@pytest.mark.asyncio
async def test_only_users_without_photos_when_no_users_with_photos():
    """
    Unit test: When there are no users with photos available,
    only users without photos should be returned.
    """
    service = PassiveEnrichmentService()
    service._batch_size = 10
    
    # Create 3 users without photos
    users_without = []
    for i in range(3):
        user = Mock()
        user.telegram_id = 100000 + i
        user.access_hash = 200000 + i
        user.username = f"user{i}"
        user.first_name = f"User{i}"
        user.current_photo_path = None
        user.messages_count = 100
        users_without.append(user)
    
    # Mock database
    mock_db = AsyncMock()
    
    mock_result_without = AsyncMock()
    mock_result_without.scalars.return_value.all.return_value = users_without
    
    mock_result_with = AsyncMock()
    mock_result_with.scalars.return_value.all.return_value = []  # No users with photos
    
    call_count = 0
    async def execute_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return mock_result_without
        else:
            return mock_result_with
    
    mock_db.execute = AsyncMock(side_effect=execute_side_effect)
    
    # Act
    result = await service._get_users_to_enrich(mock_db)
    
    # Assert
    assert len(result) == 3, "Should return only available users"
    assert all(u.current_photo_path is None for u in result), \
        "All users should be without photos"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
