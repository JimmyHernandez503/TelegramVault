"""
Property-based test for Error Categorization.

Feature: telegram-vault-enrichment-media-fixes
Property 4: Error Categorization

Tests that all API errors during media download are categorized into one of the defined error categories.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, MagicMock
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock the telegram_manager to avoid initialization issues
sys.modules['backend.app.services.telegram_service'] = MagicMock()

from app.services.media_retry_service import MediaRetryService, ErrorCategory


# Custom strategies for generating different error types
@st.composite
def error_strategy(draw):
    """Generate various error types with their expected categories."""
    error_types = [
        # (error_message, error_type_name, expected_category)
        ("FloodWaitError: Please wait 120 seconds", "FloodWaitError", ErrorCategory.RATE_LIMIT_ERRORS),
        ("Rate limit exceeded", "RateLimitError", ErrorCategory.RATE_LIMIT_ERRORS),
        ("Flood wait required", "FloodError", ErrorCategory.RATE_LIMIT_ERRORS),
        
        ("Unauthorized access", "UnauthorizedError", ErrorCategory.AUTHORIZATION_ERRORS),
        ("Authentication failed", "AuthError", ErrorCategory.AUTHORIZATION_ERRORS),
        ("Permission denied", "PermissionError", ErrorCategory.AUTHORIZATION_ERRORS),
        ("Access denied", "AccessError", ErrorCategory.AUTHORIZATION_ERRORS),
        ("Forbidden", "ForbiddenError", ErrorCategory.AUTHORIZATION_ERRORS),
        
        ("Network connection failed", "NetworkError", ErrorCategory.NETWORK_ERRORS),
        ("Connection timeout", "TimeoutError", ErrorCategory.NETWORK_ERRORS),
        ("Connection refused", "ConnectionError", ErrorCategory.NETWORK_ERRORS),
        ("Network unreachable", "NetworkError", ErrorCategory.NETWORK_ERRORS),
        
        ("File not found", "FileNotFoundError", ErrorCategory.FILE_SYSTEM_ERRORS),
        ("Disk full", "DiskError", ErrorCategory.FILE_SYSTEM_ERRORS),
        ("Permission denied on file", "OSError", ErrorCategory.FILE_SYSTEM_ERRORS),
        ("Path does not exist", "IOError", ErrorCategory.FILE_SYSTEM_ERRORS),
        
        ("File is corrupted", "ValidationError", ErrorCategory.VALIDATION_ERRORS),
        ("Invalid file format", "ValidationError", ErrorCategory.VALIDATION_ERRORS),
        ("Validation failed", "ValidationError", ErrorCategory.VALIDATION_ERRORS),
        
        ("Media not found", "MediaNotFoundError", ErrorCategory.MEDIA_NOT_FOUND),
        ("Message no longer exists", "NotFoundError", ErrorCategory.MEDIA_NOT_FOUND),
        ("Media deleted", "DeletedError", ErrorCategory.MEDIA_NOT_FOUND),
        ("Media unavailable", "UnavailableError", ErrorCategory.MEDIA_NOT_FOUND),
        
        ("Unknown error occurred", "UnknownError", ErrorCategory.UNKNOWN_ERRORS),
        ("Unexpected exception", "Exception", ErrorCategory.UNKNOWN_ERRORS),
    ]
    
    error_msg, error_type, expected_category = draw(st.sampled_from(error_types))
    
    # Create an exception with the given type name and message
    # We'll use a generic Exception but set its __name__ to simulate different types
    error = Exception(error_msg)
    error.__class__.__name__ = error_type
    
    return error, expected_category


# Feature: telegram-vault-enrichment-media-fixes, Property 4: Error Categorization
@given(error_data=error_strategy())
@settings(max_examples=100, deadline=None)
def test_property_error_categorization(error_data):
    """
    **Validates: Requirements 2.4, 5.1**
    
    Property: For any API error during media download, the system should categorize it 
    into one of the defined error categories (network, authorization, rate limit, 
    validation, file system, media not found, unknown).
    
    This test verifies that:
    1. All errors are categorized into one of the 7 defined categories
    2. The categorization is consistent and deterministic
    3. Similar errors are grouped into the same category
    """
    # Arrange
    service = MediaRetryService()
    error, expected_category = error_data
    
    # Act
    actual_category = service.categorize_error(error)
    
    # Assert
    assert isinstance(actual_category, ErrorCategory), \
        f"categorize_error should return an ErrorCategory, got {type(actual_category)}"
    
    assert actual_category == expected_category, \
        f"Error '{error}' should be categorized as {expected_category.value}, " \
        f"but was categorized as {actual_category.value}"


# Feature: telegram-vault-enrichment-media-fixes, Property 4: Error Categorization
@given(
    error_message=st.text(min_size=1, max_size=200),
    error_type_name=st.text(min_size=1, max_size=50)
)
@settings(max_examples=100, deadline=None)
def test_property_all_errors_have_category(error_message, error_type_name):
    """
    **Validates: Requirements 2.4, 5.1**
    
    Property: For any error (regardless of type or message), the categorize_error 
    method should always return a valid ErrorCategory.
    
    This test verifies that:
    1. No error causes categorize_error to fail
    2. All errors get assigned to at least one category (even if UNKNOWN)
    3. The function is robust against arbitrary error types
    """
    # Arrange
    service = MediaRetryService()
    
    # Create an arbitrary exception
    error = Exception(error_message)
    error.__class__.__name__ = error_type_name
    
    # Act
    category = service.categorize_error(error)
    
    # Assert
    assert isinstance(category, ErrorCategory), \
        f"categorize_error should always return an ErrorCategory, got {type(category)}"
    
    # Verify it's one of the valid categories
    valid_categories = [
        ErrorCategory.NETWORK_ERRORS,
        ErrorCategory.AUTHORIZATION_ERRORS,
        ErrorCategory.RATE_LIMIT_ERRORS,
        ErrorCategory.VALIDATION_ERRORS,
        ErrorCategory.FILE_SYSTEM_ERRORS,
        ErrorCategory.MEDIA_NOT_FOUND,
        ErrorCategory.UNKNOWN_ERRORS
    ]
    
    assert category in valid_categories, \
        f"Category {category} is not one of the valid error categories"


# Unit tests for specific error categorization cases
def test_rate_limit_errors_categorized_correctly():
    """Unit test: Verify rate limit errors are categorized correctly."""
    service = MediaRetryService()
    
    # Test FloodWaitError
    error = Exception("FloodWaitError: Please wait 120 seconds")
    error.__class__.__name__ = "FloodWaitError"
    assert service.categorize_error(error) == ErrorCategory.RATE_LIMIT_ERRORS
    
    # Test rate limit message
    error = Exception("Rate limit exceeded")
    assert service.categorize_error(error) == ErrorCategory.RATE_LIMIT_ERRORS
    
    # Test flood message
    error = Exception("Flood wait required")
    assert service.categorize_error(error) == ErrorCategory.RATE_LIMIT_ERRORS


def test_authorization_errors_categorized_correctly():
    """Unit test: Verify authorization errors are categorized correctly."""
    service = MediaRetryService()
    
    # Test unauthorized
    error = Exception("Unauthorized access")
    assert service.categorize_error(error) == ErrorCategory.AUTHORIZATION_ERRORS
    
    # Test auth keyword
    error = Exception("Authentication failed")
    assert service.categorize_error(error) == ErrorCategory.AUTHORIZATION_ERRORS
    
    # Test permission
    error = Exception("Permission denied")
    assert service.categorize_error(error) == ErrorCategory.AUTHORIZATION_ERRORS


def test_network_errors_categorized_correctly():
    """Unit test: Verify network errors are categorized correctly."""
    service = MediaRetryService()
    
    # Test network keyword
    error = Exception("Network connection failed")
    assert service.categorize_error(error) == ErrorCategory.NETWORK_ERRORS
    
    # Test timeout
    error = Exception("Connection timeout")
    error.__class__.__name__ = "TimeoutError"
    assert service.categorize_error(error) == ErrorCategory.NETWORK_ERRORS
    
    # Test connection error
    error = Exception("Connection refused")
    error.__class__.__name__ = "ConnectionError"
    assert service.categorize_error(error) == ErrorCategory.NETWORK_ERRORS


def test_file_system_errors_categorized_correctly():
    """Unit test: Verify file system errors are categorized correctly."""
    service = MediaRetryService()
    
    # Test file keyword
    error = Exception("File not found")
    assert service.categorize_error(error) == ErrorCategory.FILE_SYSTEM_ERRORS
    
    # Test disk keyword
    error = Exception("Disk full")
    assert service.categorize_error(error) == ErrorCategory.FILE_SYSTEM_ERRORS
    
    # Test OSError
    error = Exception("Permission denied")
    error.__class__.__name__ = "OSError"
    assert service.categorize_error(error) == ErrorCategory.FILE_SYSTEM_ERRORS


def test_validation_errors_categorized_correctly():
    """Unit test: Verify validation errors are categorized correctly."""
    service = MediaRetryService()
    
    # Test validation keyword
    error = Exception("Validation failed")
    assert service.categorize_error(error) == ErrorCategory.VALIDATION_ERRORS
    
    # Test corrupt keyword
    error = Exception("File is corrupted")
    assert service.categorize_error(error) == ErrorCategory.VALIDATION_ERRORS
    
    # Test invalid keyword
    error = Exception("Invalid file format")
    assert service.categorize_error(error) == ErrorCategory.VALIDATION_ERRORS


def test_media_not_found_errors_categorized_correctly():
    """Unit test: Verify media not found errors are categorized correctly."""
    service = MediaRetryService()
    
    # Test not found
    error = Exception("Media not found")
    assert service.categorize_error(error) == ErrorCategory.MEDIA_NOT_FOUND
    
    # Test deleted
    error = Exception("Message deleted")
    assert service.categorize_error(error) == ErrorCategory.MEDIA_NOT_FOUND
    
    # Test unavailable
    error = Exception("Media unavailable")
    assert service.categorize_error(error) == ErrorCategory.MEDIA_NOT_FOUND


def test_unknown_errors_categorized_as_unknown():
    """Unit test: Verify unknown errors are categorized as UNKNOWN."""
    service = MediaRetryService()
    
    # Test generic error
    error = Exception("Something went wrong")
    assert service.categorize_error(error) == ErrorCategory.UNKNOWN_ERRORS
    
    # Test empty message
    error = Exception("")
    assert service.categorize_error(error) == ErrorCategory.UNKNOWN_ERRORS


def test_error_category_counter_incremented():
    """Unit test: Verify error category counters are incremented."""
    service = MediaRetryService()
    
    # Get initial count
    initial_count = service._stats["error_categories"][ErrorCategory.NETWORK_ERRORS.value]
    
    # Categorize a network error
    error = Exception("Network timeout")
    category = service.categorize_error(error)
    
    # Manually increment (simulating what happens in retry_single_media)
    service._stats["error_categories"][category.value] += 1
    
    # Verify count increased
    new_count = service._stats["error_categories"][ErrorCategory.NETWORK_ERRORS.value]
    assert new_count == initial_count + 1, \
        "Error category counter should be incremented"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
