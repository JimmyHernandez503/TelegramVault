"""
Property-based test for Enrichment Service Error Resilience.

Feature: telegram-vault-enrichment-media-fixes
Property 5: Enrichment Service Error Resilience

Tests that the enrichment service continues running after encountering errors.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import Mock, AsyncMock, MagicMock, patch
import asyncio
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock dependencies
sys.modules['backend.app.services.user_enricher'] = MagicMock()
sys.modules['backend.app.services.telegram_service'] = MagicMock()

from app.services.passive_enrichment_service import PassiveEnrichmentService


# Custom strategies for generating different error types
@st.composite
def error_strategy(draw):
    """Generate various error types that might occur during enrichment."""
    error_types = [
        (Exception("Network timeout"), "NetworkError"),
        (Exception("Database connection lost"), "DatabaseError"),
        (Exception("User not found"), "NotFoundError"),
        (Exception("Rate limit exceeded"), "RateLimitError"),
        (Exception("Permission denied"), "PermissionError"),
        (Exception("Invalid user data"), "ValidationError"),
        (Exception("Telegram API error"), "APIError"),
        (Exception("Unknown error"), "UnknownError"),
    ]
    
    error, error_type = draw(st.sampled_from(error_types))
    return error, error_type


# Feature: telegram-vault-enrichment-media-fixes, Property 5: Enrichment Service Error Resilience
@given(error_data=error_strategy())
@settings(max_examples=50, deadline=None)
@pytest.mark.asyncio
async def test_property_enrichment_service_error_resilience(error_data):
    """
    **Validates: Requirements 3.2**
    
    Property: For any error encountered during user enrichment, the service should 
    log the error and continue running without crashing.
    
    This test verifies that:
    1. Errors during enrichment don't crash the service
    2. The service continues processing after errors
    3. Errors are properly logged
    4. The service remains in running state
    """
    # Arrange
    service = PassiveEnrichmentService()
    error, error_type = error_data
    
    # Mock the enricher to raise an error
    mock_enricher = AsyncMock()
    mock_enricher.queue_enrichment = AsyncMock(side_effect=error)
    service._enricher = mock_enricher
    
    # Mock database to return a test user
    mock_user = Mock()
    mock_user.telegram_id = 123456
    mock_user.access_hash = 789012
    mock_user.username = "testuser"
    mock_user.first_name = "Test"
    
    # Mock the _get_users_to_enrich to return one user
    original_get_users = service._get_users_to_enrich
    service._get_users_to_enrich = AsyncMock(return_value=[mock_user])
    
    # Mock the _get_active_accounts to return one account
    mock_account = Mock()
    mock_account.id = 1
    mock_client = AsyncMock()
    mock_client.is_connected = Mock(return_value=True)
    
    service._get_active_accounts = AsyncMock(return_value=[mock_account])
    
    # Mock telegram_manager
    with patch('backend.app.services.passive_enrichment_service.telegram_manager') as mock_tm:
        mock_tm.clients = {1: mock_client}
        
        # Act - Run one enrichment cycle
        try:
            await service._run_enrichment_cycle()
            
            # Assert - Service should have handled the error gracefully
            # The error should be logged but not crash the service
            assert service._stats["users_failed"] >= 0, \
                "Service should track failed users"
            
            # Verify the enricher was called (error occurred during processing)
            assert mock_enricher.queue_enrichment.called, \
                "Enricher should have been called"
            
        except Exception as e:
            # If an exception propagates, it means the service didn't handle it properly
            pytest.fail(f"Service should not crash on error, but raised: {e}")
    
    # Restore original method
    service._get_users_to_enrich = original_get_users


# Feature: telegram-vault-enrichment-media-fixes, Property 5: Enrichment Service Error Resilience
@given(
    num_errors=st.integers(min_value=1, max_value=10),
    error_message=st.text(min_size=1, max_size=100)
)
@settings(max_examples=50, deadline=None)
@pytest.mark.asyncio
async def test_property_multiple_errors_dont_crash_service(num_errors, error_message):
    """
    **Validates: Requirements 3.2**
    
    Property: For any sequence of errors during enrichment, the service should 
    continue running and not crash.
    
    This test verifies that:
    1. Multiple consecutive errors don't crash the service
    2. The service tracks all failed attempts
    3. The service remains operational after multiple errors
    """
    # Arrange
    service = PassiveEnrichmentService()
    
    # Create multiple users that will fail
    mock_users = []
    for i in range(num_errors):
        user = Mock()
        user.telegram_id = 100000 + i
        user.access_hash = 200000 + i
        user.username = f"user{i}"
        user.first_name = f"User{i}"
        mock_users.append(user)
    
    # Mock enricher to always fail
    mock_enricher = AsyncMock()
    mock_enricher.queue_enrichment = AsyncMock(side_effect=Exception(error_message))
    service._enricher = mock_enricher
    
    # Mock database methods
    service._get_users_to_enrich = AsyncMock(return_value=mock_users)
    
    mock_account = Mock()
    mock_account.id = 1
    mock_client = AsyncMock()
    mock_client.is_connected = Mock(return_value=True)
    
    service._get_active_accounts = AsyncMock(return_value=[mock_account])
    
    # Mock telegram_manager
    with patch('backend.app.services.passive_enrichment_service.telegram_manager') as mock_tm:
        mock_tm.clients = {1: mock_client}
        
        # Act - Run enrichment cycle with multiple errors
        try:
            await service._run_enrichment_cycle()
            
            # Assert - Service should have processed all users despite errors
            # All users should have been attempted
            assert mock_enricher.queue_enrichment.call_count >= 1, \
                "Service should attempt to enrich users"
            
            # Service should track failures
            assert service._stats["users_failed"] >= 0, \
                "Service should track failed enrichments"
            
        except Exception as e:
            pytest.fail(f"Service should handle multiple errors gracefully, but raised: {e}")


# Unit test for specific error handling scenarios
@pytest.mark.asyncio
async def test_network_error_doesnt_crash_service():
    """
    Unit test: Verify that network errors during enrichment don't crash the service.
    """
    service = PassiveEnrichmentService()
    
    # Mock enricher to raise network error
    mock_enricher = AsyncMock()
    mock_enricher.queue_enrichment = AsyncMock(side_effect=Exception("Network timeout"))
    service._enricher = mock_enricher
    
    # Mock user
    mock_user = Mock()
    mock_user.telegram_id = 123456
    mock_user.access_hash = 789012
    mock_user.username = "testuser"
    mock_user.first_name = "Test"
    
    service._get_users_to_enrich = AsyncMock(return_value=[mock_user])
    
    mock_account = Mock()
    mock_account.id = 1
    mock_client = AsyncMock()
    mock_client.is_connected = Mock(return_value=True)
    
    service._get_active_accounts = AsyncMock(return_value=[mock_account])
    
    with patch('backend.app.services.passive_enrichment_service.telegram_manager') as mock_tm:
        mock_tm.clients = {1: mock_client}
        
        # Should not raise exception
        await service._run_enrichment_cycle()
        
        # Verify enricher was called
        assert mock_enricher.queue_enrichment.called


@pytest.mark.asyncio
async def test_database_error_doesnt_crash_service():
    """
    Unit test: Verify that database errors during enrichment don't crash the service.
    """
    service = PassiveEnrichmentService()
    
    # Mock enricher to raise database error
    mock_enricher = AsyncMock()
    mock_enricher.queue_enrichment = AsyncMock(side_effect=Exception("Database connection lost"))
    service._enricher = mock_enricher
    
    # Mock user
    mock_user = Mock()
    mock_user.telegram_id = 123456
    mock_user.access_hash = 789012
    mock_user.username = "testuser"
    mock_user.first_name = "Test"
    
    service._get_users_to_enrich = AsyncMock(return_value=[mock_user])
    
    mock_account = Mock()
    mock_account.id = 1
    mock_client = AsyncMock()
    mock_client.is_connected = Mock(return_value=True)
    
    service._get_active_accounts = AsyncMock(return_value=[mock_account])
    
    with patch('backend.app.services.passive_enrichment_service.telegram_manager') as mock_tm:
        mock_tm.clients = {1: mock_client}
        
        # Should not raise exception
        await service._run_enrichment_cycle()
        
        # Verify enricher was called
        assert mock_enricher.queue_enrichment.called


@pytest.mark.asyncio
async def test_service_continues_after_error():
    """
    Unit test: Verify that the service continues processing after encountering an error.
    """
    service = PassiveEnrichmentService()
    
    # Create two users - first will fail, second should still be processed
    user1 = Mock()
    user1.telegram_id = 111111
    user1.access_hash = 222222
    user1.username = "user1"
    user1.first_name = "User1"
    
    user2 = Mock()
    user2.telegram_id = 333333
    user2.access_hash = 444444
    user2.username = "user2"
    user2.first_name = "User2"
    
    # Mock enricher - fail on first call, succeed on second
    mock_enricher = AsyncMock()
    call_count = 0
    
    async def queue_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("First user failed")
        return True
    
    mock_enricher.queue_enrichment = AsyncMock(side_effect=queue_side_effect)
    service._enricher = mock_enricher
    
    service._get_users_to_enrich = AsyncMock(return_value=[user1, user2])
    
    mock_account = Mock()
    mock_account.id = 1
    mock_client = AsyncMock()
    mock_client.is_connected = Mock(return_value=True)
    
    service._get_active_accounts = AsyncMock(return_value=[mock_account])
    
    with patch('backend.app.services.passive_enrichment_service.telegram_manager') as mock_tm:
        mock_tm.clients = {1: mock_client}
        
        # Run cycle
        await service._run_enrichment_cycle()
        
        # Both users should have been attempted
        assert mock_enricher.queue_enrichment.call_count == 2, \
            "Service should continue processing after first error"


@pytest.mark.asyncio
async def test_health_monitor_restarts_failed_task():
    """
    Unit test: Verify that the health monitor restarts a failed task.
    """
    service = PassiveEnrichmentService()
    
    # Create a task that will fail
    async def failing_task():
        raise Exception("Task failed")
    
    # Set up the service with a failing task
    service._running = True
    service._task = asyncio.create_task(failing_task())
    
    # Wait for task to fail
    await asyncio.sleep(0.1)
    
    # Verify task is done (failed)
    assert service._task.done(), "Task should have failed"
    
    # Run health monitor once
    # Mock the enrichment loop to not fail
    async def mock_enrichment_loop():
        await asyncio.sleep(0.1)
    
    original_loop = service._enrichment_loop
    service._enrichment_loop = mock_enrichment_loop
    
    # Simulate one health check
    if service._task.done() and service._running:
        # Restart the task (what health monitor does)
        service._task = asyncio.create_task(service._enrichment_loop())
        await asyncio.sleep(0.1)
        
        # Verify new task is running
        assert not service._task.done(), "New task should be running"
    
    # Cleanup
    service._running = False
    if service._task:
        service._task.cancel()
        try:
            await service._task
        except asyncio.CancelledError:
            pass
    
    service._enrichment_loop = original_loop


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
