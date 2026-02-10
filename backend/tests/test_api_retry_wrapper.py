"""
Unit tests for APIRetryWrapper.

Tests cover:
- Successful execution on first attempt
- Retry logic with temporary errors
- No retry for permanent errors
- Exponential backoff calculation
- Jitter application
- Error categorization
- Logging of retry attempts
- Timeout handling
- Custom retry parameters
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from backend.app.core.api_retry_wrapper import (
    APIRetryWrapper,
    ErrorCategory,
    RetryResult
)
from backend.app.core.config_manager import ConfigManager
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem


class TestAPIRetryWrapper:
    """Test suite for APIRetryWrapper."""
    
    @pytest.fixture
    def config_manager(self):
        """Create a mock ConfigManager."""
        config = MagicMock(spec=ConfigManager)
        config.get_int.side_effect = lambda key, default=0: {
            "TELEGRAM_API_RETRY_MAX_ATTEMPTS": 3,
            "TELEGRAM_API_RETRY_DELAY_BASE": 1,
            "TELEGRAM_API_TIMEOUT": 2  # Short timeout for tests
        }.get(key, default)
        config.get_bool.side_effect = lambda key, default=False: {
            "TELEGRAM_API_RETRY_JITTER": True
        }.get(key, default)
        return config
    
    @pytest.fixture
    def logger(self):
        """Create a mock EnhancedLoggingSystem."""
        logger = MagicMock(spec=EnhancedLoggingSystem)
        logger.log_info = AsyncMock()
        logger.log_debug = AsyncMock()
        logger.log_warning = AsyncMock()
        logger.log_error = AsyncMock()
        return logger
    
    @pytest.fixture
    def retry_wrapper(self, config_manager, logger):
        """Create an APIRetryWrapper instance."""
        return APIRetryWrapper(config_manager, logger)
    
    @pytest.mark.asyncio
    async def test_successful_first_attempt(self, retry_wrapper, logger):
        """Test successful execution on first attempt."""
        # Create a mock function that succeeds
        mock_func = AsyncMock(return_value="success")
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            operation_name="test_operation"
        )
        
        # Verify result
        assert result.success == True
        assert result.result == "success"
        assert result.attempts == 1
        assert result.error is None
        
        # Verify function was called once
        assert mock_func.call_count == 1
        
        # Verify logging
        assert logger.log_info.call_count >= 2  # Start and success
    
    @pytest.mark.asyncio
    async def test_retry_with_temporary_error(self, retry_wrapper, logger):
        """Test retry logic with temporary errors."""
        # Create a mock function that fails twice then succeeds
        mock_func = AsyncMock(
            side_effect=[
                ConnectionError("Network error"),
                TimeoutError("Timeout"),
                "success"
            ]
        )
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            operation_name="test_operation"
        )
        
        # Verify result
        assert result.success == True
        assert result.result == "success"
        assert result.attempts == 3
        
        # Verify function was called 3 times
        assert mock_func.call_count == 3
        
        # Verify warnings were logged for failures
        assert logger.log_warning.call_count >= 2
    
    @pytest.mark.asyncio
    async def test_no_retry_for_permanent_error(self, retry_wrapper, logger):
        """Test that permanent errors are not retried."""
        # Create a mock function that raises a permanent error
        permanent_error = ValueError("Invalid data")
        mock_func = AsyncMock(side_effect=permanent_error)
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            operation_name="test_operation"
        )
        
        # Verify result
        assert result.success == False
        assert result.error == permanent_error
        assert result.attempts == 1  # Should not retry
        
        # Verify function was called only once
        assert mock_func.call_count == 1
        
        # Verify error was logged
        assert logger.log_error.call_count >= 1
    
    @pytest.mark.asyncio
    async def test_max_attempts_exhausted(self, retry_wrapper, logger):
        """Test behavior when max attempts are exhausted."""
        # Create a mock function that always fails with temporary error
        mock_func = AsyncMock(side_effect=ConnectionError("Network error"))
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            operation_name="test_operation"
        )
        
        # Verify result
        assert result.success == False
        assert isinstance(result.error, ConnectionError)
        assert result.attempts == 3  # Max attempts from config
        
        # Verify function was called max_attempts times
        assert mock_func.call_count == 3
        
        # Verify final error was logged
        assert logger.log_error.call_count >= 1
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self, retry_wrapper, logger):
        """Test handling of timeout errors."""
        # Create a mock function that times out
        async def slow_func():
            await asyncio.sleep(10)  # Longer than timeout (2 seconds)
            return "success"
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            slow_func,
            operation_name="test_operation"
        )
        
        # Verify result - should fail after retries
        assert result.success == False
        assert result.attempts == 3  # Max attempts
        
        # Verify timeout warnings were logged
        assert logger.log_warning.call_count >= 1
    
    def test_calculate_backoff_exponential(self, retry_wrapper):
        """Test exponential backoff calculation."""
        # Disable jitter for predictable testing
        retry_wrapper.jitter_enabled = False
        retry_wrapper.delay_base = 2
        
        # Test exponential growth: 2 * (2^(n-1))
        assert retry_wrapper.calculate_backoff(1) == 2  # 2 * 2^0 = 2
        assert retry_wrapper.calculate_backoff(2) == 4  # 2 * 2^1 = 4
        assert retry_wrapper.calculate_backoff(3) == 8  # 2 * 2^2 = 8
        assert retry_wrapper.calculate_backoff(4) == 16  # 2 * 2^3 = 16
    
    def test_calculate_backoff_with_jitter(self, retry_wrapper):
        """Test backoff calculation with jitter."""
        retry_wrapper.jitter_enabled = True
        retry_wrapper.delay_base = 2
        
        # Calculate multiple times to verify jitter is applied
        delays = [retry_wrapper.calculate_backoff(2) for _ in range(10)]
        
        # All delays should be >= base delay (4)
        assert all(d >= 4 for d in delays)
        
        # All delays should be <= base delay + 10% (4.4)
        assert all(d <= 4.4 for d in delays)
        
        # Delays should vary (not all the same)
        assert len(set(delays)) > 1
    
    def test_categorize_error_temporary(self, retry_wrapper):
        """Test categorization of temporary errors."""
        temporary_errors = [
            ConnectionError("Connection failed"),
            TimeoutError("Timeout"),
            OSError("OS error"),
            IOError("IO error"),
        ]
        
        for error in temporary_errors:
            category = retry_wrapper.categorize_error(error)
            assert category == ErrorCategory.TEMPORARY, f"Failed for {type(error).__name__}"
    
    def test_categorize_error_permanent(self, retry_wrapper):
        """Test categorization of permanent errors."""
        permanent_errors = [
            ValueError("Invalid value"),
            TypeError("Type error"),
            PermissionError("Permission denied"),
        ]
        
        for error in permanent_errors:
            category = retry_wrapper.categorize_error(error)
            assert category == ErrorCategory.PERMANENT, f"Failed for {type(error).__name__}"
    
    def test_categorize_error_by_message(self, retry_wrapper):
        """Test error categorization based on error message."""
        # Permanent error indicators in message
        permanent_error = Exception("User not found")
        assert retry_wrapper.categorize_error(permanent_error) == ErrorCategory.PERMANENT
        
        invalid_error = Exception("Invalid request")
        assert retry_wrapper.categorize_error(invalid_error) == ErrorCategory.PERMANENT
        
        # Rate limit indicators in message
        rate_limit_error = Exception("Too many requests")
        assert retry_wrapper.categorize_error(rate_limit_error) == ErrorCategory.RATE_LIMIT
        
        flood_error = Exception("Flood wait required")
        assert retry_wrapper.categorize_error(flood_error) == ErrorCategory.RATE_LIMIT
    
    def test_is_temporary_error(self, retry_wrapper):
        """Test is_temporary_error helper method."""
        assert retry_wrapper.is_temporary_error(ConnectionError()) == True
        assert retry_wrapper.is_temporary_error(TimeoutError()) == True
        assert retry_wrapper.is_temporary_error(ValueError()) == False
        assert retry_wrapper.is_temporary_error(PermissionError()) == False
    
    @pytest.mark.asyncio
    async def test_execute_with_custom_retry(self, retry_wrapper, logger):
        """Test execution with custom retry parameters."""
        # Create a mock function that fails twice then succeeds
        mock_func = AsyncMock(
            side_effect=[
                ConnectionError("Error 1"),
                ConnectionError("Error 2"),
                "success"
            ]
        )
        
        # Execute with custom parameters
        result = await retry_wrapper.execute_with_custom_retry(
            mock_func,
            max_attempts=5,
            delay_base=2,
            jitter_enabled=False,
            operation_name="test_operation"
        )
        
        # Verify result
        assert result.success == True
        assert result.result == "success"
        assert result.attempts == 3
        
        # Verify original config is restored
        assert retry_wrapper.max_attempts == 3  # Original value
        assert retry_wrapper.delay_base == 1  # Original value
        assert retry_wrapper.jitter_enabled == True  # Original value
    
    @pytest.mark.asyncio
    async def test_logging_of_retry_attempts(self, retry_wrapper, logger):
        """Test that all retry attempts are logged."""
        # Create a mock function that fails twice then succeeds
        mock_func = AsyncMock(
            side_effect=[
                ConnectionError("Error 1"),
                ConnectionError("Error 2"),
                "success"
            ]
        )
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            operation_name="test_operation"
        )
        
        # Verify logging calls
        # Should have: start log, 3 debug logs (attempts), 2 warnings (errors), 1 success
        assert logger.log_info.call_count >= 2  # Start and success
        assert logger.log_debug.call_count >= 3  # Each attempt
        assert logger.log_warning.call_count >= 2  # Each error
    
    @pytest.mark.asyncio
    async def test_function_with_arguments(self, retry_wrapper, logger):
        """Test retry wrapper with function arguments."""
        # Create a mock function that uses arguments
        mock_func = AsyncMock(return_value="result")
        
        # Execute with args and kwargs
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            "arg1", "arg2",
            operation_name="test_operation",
            kwarg1="value1",
            kwarg2="value2"
        )
        
        # Verify result
        assert result.success == True
        assert result.result == "result"
        
        # Verify function was called with correct arguments
        mock_func.assert_called_once_with("arg1", "arg2", kwarg1="value1", kwarg2="value2")
    
    @pytest.mark.asyncio
    async def test_retry_result_metadata(self, retry_wrapper, logger):
        """Test that RetryResult contains correct metadata."""
        # Create a mock function that fails once then succeeds
        mock_func = AsyncMock(
            side_effect=[
                ConnectionError("Error"),
                "success"
            ]
        )
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            operation_name="test_operation"
        )
        
        # Verify metadata
        assert result.success == True
        assert result.result == "success"
        assert result.error is None
        assert result.attempts == 2
        assert result.total_delay_ms > 0  # Should have some delay from retry
    
    @pytest.mark.asyncio
    async def test_error_details_in_logs(self, retry_wrapper, logger):
        """Test that error details are included in logs."""
        # Create a mock function that raises an error
        error = ConnectionError("Network connection failed")
        mock_func = AsyncMock(side_effect=error)
        
        # Execute
        result = await retry_wrapper.execute_with_retry(
            mock_func,
            operation_name="test_operation"
        )
        
        # Verify error logging includes details
        warning_calls = logger.log_warning.call_args_list
        assert len(warning_calls) > 0
        
        # Check that error details are in the log call
        for call in warning_calls:
            kwargs = call[1]
            if 'details' in kwargs:
                details = kwargs['details']
                assert 'error_type' in details
                assert 'error_message' in details
                assert 'error_category' in details


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
