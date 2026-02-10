"""
Property-Based Test for Transient Error Retry

Feature: telegram-vault-enrichment-media-fixes
Property 28: Transient Error Retry

For any transient database error (connection timeout, deadlock),
the system should retry the operation up to 3 times before failing.

Validates: Requirements 8.7
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.exc import OperationalError, DBAPIError
from backend.app.core.session_manager import session_manager, SessionRecoveryError
from backend.app.services.user_management_service import (
    user_management_service,
    TelegramUserData
)


class TestPropertyTransientErrorRetry:
    """
    Property-Based Test: Transient Error Retry
    
    This test validates that for ANY transient database error,
    the system retries the operation up to 3 times before failing.
    """
    
    @pytest.mark.asyncio
    async def test_property_retry_on_connection_error(self):
        """
        Test that connection errors trigger retry logic.
        
        **Validates: Requirements 8.7**
        """
        telegram_id = 4000000001
        user_data = TelegramUserData(
            telegram_id=telegram_id,
            username="retry_test_user",
            first_name="Retry"
        )
        
        # Track number of attempts
        attempt_count = 0
        
        async def mock_operation_with_retries(session):
            nonlocal attempt_count
            attempt_count += 1
            
            # Fail first 2 attempts with connection error
            if attempt_count <= 2:
                raise OperationalError(
                    "connection timeout",
                    params=None,
                    orig=Exception("Connection timed out")
                )
            
            # Succeed on 3rd attempt
            # Return a mock user object
            mock_user = MagicMock()
            mock_user.telegram_id = telegram_id
            mock_user.username = "retry_test_user"
            return mock_user
        
        # Execute with retry
        result = await session_manager.execute_with_retry(
            mock_operation_with_retries,
            max_retries=3
        )
        
        # Assert: Should have retried and eventually succeeded
        assert attempt_count == 3, (
            f"Expected 3 attempts (2 failures + 1 success), but got {attempt_count}"
        )
        assert result is not None, "Operation should succeed after retries"
        assert result.telegram_id == telegram_id
    
    @pytest.mark.asyncio
    async def test_property_retry_on_deadlock_error(self):
        """
        Test that deadlock errors trigger retry logic.
        
        **Validates: Requirements 8.7**
        """
        attempt_count = 0
        
        async def mock_operation_with_deadlock(session):
            nonlocal attempt_count
            attempt_count += 1
            
            # Fail first attempt with deadlock
            if attempt_count == 1:
                raise OperationalError(
                    "deadlock detected",
                    params=None,
                    orig=Exception("Deadlock detected")
                )
            
            # Succeed on 2nd attempt
            return {"success": True, "attempt": attempt_count}
        
        # Execute with retry
        result = await session_manager.execute_with_retry(
            mock_operation_with_deadlock,
            max_retries=3
        )
        
        # Assert: Should have retried once and succeeded
        assert attempt_count == 2, f"Expected 2 attempts, but got {attempt_count}"
        assert result["success"] is True
    
    @pytest.mark.asyncio
    async def test_property_retry_limit_reached(self):
        """
        Test that operation fails after max retries are exhausted.
        
        **Validates: Requirements 8.7**
        """
        attempt_count = 0
        
        async def mock_operation_always_fails(session):
            nonlocal attempt_count
            attempt_count += 1
            
            # Always fail with connection error
            raise OperationalError(
                "connection timeout",
                params=None,
                orig=Exception("Connection timed out")
            )
        
        # Execute with retry - should fail after 3 attempts
        with pytest.raises(SessionRecoveryError) as exc_info:
            await session_manager.execute_with_retry(
                mock_operation_always_fails,
                max_retries=3
            )
        
        # Assert: Should have attempted 4 times (initial + 3 retries)
        assert attempt_count == 4, (
            f"Expected 4 attempts (1 initial + 3 retries), but got {attempt_count}"
        )
        
        # Assert: Error message should indicate max retries reached
        assert "failed after" in str(exc_info.value).lower(), (
            f"Error message should mention retry failure: {exc_info.value}"
        )
    
    @pytest.mark.asyncio
    async def test_property_exponential_backoff(self):
        """
        Test that retry delays follow exponential backoff pattern.
        
        **Validates: Requirements 8.7**
        """
        attempt_times = []
        
        async def mock_operation_track_timing(session):
            attempt_times.append(asyncio.get_event_loop().time())
            
            # Fail first 2 attempts
            if len(attempt_times) <= 2:
                raise OperationalError(
                    "connection timeout",
                    params=None,
                    orig=Exception("Connection timed out")
                )
            
            # Succeed on 3rd attempt
            return {"success": True}
        
        # Execute with retry
        result = await session_manager.execute_with_retry(
            mock_operation_track_timing,
            max_retries=3
        )
        
        # Assert: Should have 3 attempts
        assert len(attempt_times) == 3, f"Expected 3 attempts, got {len(attempt_times)}"
        
        # Assert: Delays should increase (exponential backoff)
        # First retry delay: ~0.1s (2^0 * 0.1)
        # Second retry delay: ~0.2s (2^1 * 0.1)
        if len(attempt_times) >= 3:
            delay1 = attempt_times[1] - attempt_times[0]
            delay2 = attempt_times[2] - attempt_times[1]
            
            # Allow some tolerance for timing variations
            assert delay1 >= 0.08, f"First retry delay too short: {delay1}s (expected ~0.1s)"
            assert delay2 >= 0.15, f"Second retry delay too short: {delay2}s (expected ~0.2s)"
            assert delay2 > delay1, (
                f"Exponential backoff not working: delay2 ({delay2}s) should be > delay1 ({delay1}s)"
            )
    
    @pytest.mark.asyncio
    async def test_property_no_retry_on_non_transient_error(self):
        """
        Test that non-transient errors don't trigger retries.
        
        **Validates: Requirements 8.7**
        """
        attempt_count = 0
        
        async def mock_operation_non_transient_error(session):
            nonlocal attempt_count
            attempt_count += 1
            
            # Raise a non-transient error (ValueError)
            raise ValueError("Invalid data - not a transient error")
        
        # Execute with retry - should fail immediately without retries
        with pytest.raises(SessionRecoveryError):
            await session_manager.execute_with_retry(
                mock_operation_non_transient_error,
                max_retries=3
            )
        
        # Assert: Should only attempt once (no retries for non-transient errors)
        assert attempt_count == 1, (
            f"Expected 1 attempt for non-transient error, but got {attempt_count}. "
            f"Non-transient errors should not trigger retries."
        )
    
    @pytest.mark.asyncio
    async def test_property_retry_with_different_error_types(self):
        """
        Test retry behavior with various transient error types.
        
        **Validates: Requirements 8.7**
        """
        test_cases = [
            ("connection timeout", True, "Connection errors should be retried"),
            ("deadlock detected", True, "Deadlock errors should be retried"),
            ("lock timeout", True, "Lock timeout errors should be retried"),
            ("temporary failure", True, "Temporary failures should be retried"),
            ("invalid syntax", False, "Syntax errors should not be retried"),
            ("permission denied", False, "Permission errors should not be retried"),
        ]
        
        for error_msg, should_retry, description in test_cases:
            attempt_count = 0
            
            async def mock_operation(session):
                nonlocal attempt_count
                attempt_count += 1
                
                # Fail first attempt
                if attempt_count == 1:
                    raise OperationalError(
                        error_msg,
                        params=None,
                        orig=Exception(error_msg)
                    )
                
                # Succeed on retry
                return {"success": True}
            
            try:
                result = await session_manager.execute_with_retry(
                    mock_operation,
                    max_retries=3
                )
                
                if should_retry:
                    # Should have retried and succeeded
                    assert attempt_count == 2, (
                        f"{description}: Expected 2 attempts, got {attempt_count}"
                    )
                else:
                    # Should have failed without retry
                    # If we get here, the error was retried when it shouldn't have been
                    if attempt_count > 1:
                        pytest.fail(
                            f"{description}: Error was retried {attempt_count} times "
                            f"but should not have been retried"
                        )
            
            except SessionRecoveryError:
                # Operation failed after retries
                if should_retry:
                    # This is unexpected - retriable errors should eventually succeed
                    # in our mock (they succeed on 2nd attempt)
                    pytest.fail(
                        f"{description}: Operation failed after retries, "
                        f"but should have succeeded on retry"
                    )
                else:
                    # Expected - non-retriable errors should fail
                    assert attempt_count == 1, (
                        f"{description}: Expected 1 attempt, got {attempt_count}"
                    )
    
    @pytest.mark.asyncio
    async def test_property_retry_count_configurable(self):
        """
        Test that max_retries parameter is respected.
        
        **Validates: Requirements 8.7**
        """
        # Test with max_retries=1
        attempt_count_1 = 0
        
        async def mock_operation_1(session):
            nonlocal attempt_count_1
            attempt_count_1 += 1
            raise OperationalError("connection timeout", params=None, orig=Exception())
        
        with pytest.raises(SessionRecoveryError):
            await session_manager.execute_with_retry(mock_operation_1, max_retries=1)
        
        assert attempt_count_1 == 2, f"With max_retries=1, expected 2 attempts, got {attempt_count_1}"
        
        # Test with max_retries=5
        attempt_count_5 = 0
        
        async def mock_operation_5(session):
            nonlocal attempt_count_5
            attempt_count_5 += 1
            raise OperationalError("connection timeout", params=None, orig=Exception())
        
        with pytest.raises(SessionRecoveryError):
            await session_manager.execute_with_retry(mock_operation_5, max_retries=5)
        
        assert attempt_count_5 == 6, f"With max_retries=5, expected 6 attempts, got {attempt_count_5}"
    
    @pytest.mark.asyncio
    async def test_property_successful_operation_no_retry(self):
        """
        Test that successful operations don't trigger retries.
        
        **Validates: Requirements 8.7**
        """
        attempt_count = 0
        
        async def mock_successful_operation(session):
            nonlocal attempt_count
            attempt_count += 1
            return {"success": True, "data": "test"}
        
        # Execute operation
        result = await session_manager.execute_with_retry(
            mock_successful_operation,
            max_retries=3
        )
        
        # Assert: Should only execute once (no retries needed)
        assert attempt_count == 1, (
            f"Successful operation should not retry. Expected 1 attempt, got {attempt_count}"
        )
        assert result["success"] is True
        assert result["data"] == "test"
