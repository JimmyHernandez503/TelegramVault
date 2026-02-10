"""
Property-based test for API Parameter Validation.

Feature: telegram-vault-enrichment-media-fixes
Property 3: API Parameter Validation

Tests that parameter validation occurs before making Telethon API calls.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, AsyncMock, MagicMock, patch
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


# Mock the telegram_manager to avoid initialization issues
sys.modules['backend.app.services.telegram_service'] = MagicMock()

from app.services.media_retry_service import MediaRetryService


# Custom strategies for test data
@st.composite
def message_strategy(draw):
    """Generate message objects with various states."""
    has_media = draw(st.booleans())
    has_id = draw(st.booleans())
    
    message = Mock()
    if has_media:
        message.media = Mock()
    else:
        # Message without media attribute - remove it if it exists
        message = Mock(spec=[])
    
    if has_id and has_media:
        message.id = draw(st.integers(min_value=1, max_value=999999))
    
    return message, has_media


@st.composite
def media_type_strategy(draw):
    """Generate media types including valid and invalid ones."""
    valid_types = ["photo", "video", "document", "audio", "voice", "sticker", "gif", "video_note"]
    
    choice = draw(st.integers(min_value=0, max_value=2))
    if choice == 0:
        # Valid media type
        return draw(st.sampled_from(valid_types)), True
    elif choice == 1:
        # Empty string
        return "", False
    else:
        # None
        return None, False


# Feature: telegram-vault-enrichment-media-fixes, Property 3: API Parameter Validation
@given(
    message_data=message_strategy(),
    media_type_data=media_type_strategy(),
    group_id=st.integers(min_value=1, max_value=999999)
)
@settings(max_examples=100, deadline=None)
@pytest.mark.asyncio
async def test_property_api_parameter_validation(message_data, media_type_data, group_id):
    """
    **Validates: Requirements 2.2**
    
    Property: For any media download attempt, parameter validation should occur 
    before making the Telethon API call.
    
    This test verifies that:
    1. Invalid parameters are caught BEFORE the API call
    2. The function returns None for invalid parameters
    3. The API is never called with invalid parameters
    """
    # Arrange
    service = MediaRetryService()
    message, has_media = message_data
    media_type, is_valid_type = media_type_data
    
    # Create a mock client that tracks if download_media was called
    client = AsyncMock()
    client.download_media = AsyncMock(return_value="/tmp/test.jpg")
    
    # Act
    result = await service._download_media_enhanced(client, message, media_type if is_valid_type else (media_type or ""), group_id)
    
    # Assert
    if not has_media or not is_valid_type:
        # Invalid parameters should result in None return (validation failed)
        assert result == (None, None), \
            f"Should return (None, None) for invalid parameters, got {result}"
        
        # Verify API was NOT called (validation happened first)
        assert not client.download_media.called, \
            "API should not be called when parameters are invalid"
    else:
        # Valid parameters - API call should be attempted
        # Verify API WAS called (validation passed)
        assert client.download_media.called, \
            "API should be called when all parameters are valid"


# Feature: telegram-vault-enrichment-media-fixes, Property 3: API Parameter Validation
@given(
    message_data=message_strategy(),
    media_type_data=media_type_strategy(),
    group_id=st.integers(min_value=1, max_value=999999)
)
@settings(max_examples=100, deadline=None)
@pytest.mark.asyncio
async def test_property_api_parameter_validation_basic(message_data, media_type_data, group_id):
    """
    **Validates: Requirements 2.2**
    
    Property: For any media download attempt, parameter validation should occur 
    before making the Telethon API call.
    
    Tests the basic _download_media function.
    """
    # Arrange
    service = MediaRetryService()
    message, has_media = message_data
    media_type, is_valid_type = media_type_data
    
    # Create a mock client that tracks if download_media was called
    client = AsyncMock()
    client.download_media = AsyncMock(return_value="/tmp/test.jpg")
    
    # Act
    result = await service._download_media(client, message, media_type if is_valid_type else (media_type or ""), group_id)
    
    # Assert
    if not has_media or not is_valid_type:
        # Invalid parameters should result in None return (validation failed)
        assert result == (None, None), \
            f"Should return (None, None) for invalid parameters, got {result}"
        
        # Verify API was NOT called (validation happened first)
        assert not client.download_media.called, \
            "API should not be called when parameters are invalid"
    else:
        # Valid parameters - API call should be attempted
        # Verify API WAS called (validation passed)
        assert client.download_media.called, \
            "API should be called when all parameters are valid"


# Unit test for specific edge cases
@pytest.mark.asyncio
async def test_validation_prevents_api_call_with_no_media():
    """
    Unit test: Verify that validation prevents API call when message has no media.
    """
    service = MediaRetryService()
    
    # Create message without media attribute
    message = Mock(spec=[])  # Empty spec means no attributes
    
    client = AsyncMock()
    client.download_media = AsyncMock()
    
    # Should return None (validation failed)
    result = await service._download_media_enhanced(client, message, "photo", 123)
    assert result == (None, None), "Should return (None, None) when message has no media"
    
    # API should NOT have been called
    assert not client.download_media.called


@pytest.mark.asyncio
async def test_validation_prevents_api_call_with_empty_media_type():
    """
    Unit test: Verify that validation prevents API call when media type is empty.
    """
    service = MediaRetryService()
    
    # Create valid message
    message = Mock()
    message.media = Mock()
    message.id = 12345
    
    client = AsyncMock()
    client.download_media = AsyncMock()
    
    # Should return None for empty media type
    result = await service._download_media_enhanced(client, message, "", 123)
    assert result == (None, None), "Should return (None, None) when media type is empty"
    
    # API should NOT have been called
    assert not client.download_media.called


@pytest.mark.asyncio
async def test_validation_allows_api_call_with_valid_parameters():
    """
    Unit test: Verify that validation allows API call when all parameters are valid.
    """
    service = MediaRetryService()
    
    # Create valid message
    message = Mock()
    message.media = Mock()
    message.id = 12345
    
    client = AsyncMock()
    # Mock download_media to return a file path
    client.download_media = AsyncMock(return_value="/tmp/test.jpg")
    
    # Should not raise ValueError
    try:
        await service._download_media_enhanced(client, message, "photo", 123)
    except Exception as e:
        # May fail for other reasons (file system, etc.) but not validation
        assert "no media attribute" not in str(e)
        assert "media type is required" not in str(e)
    
    # API SHOULD have been called (validation passed)
    assert client.download_media.called


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
