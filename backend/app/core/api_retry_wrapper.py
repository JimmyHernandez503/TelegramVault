"""
API Retry Wrapper for TelegramVault.

This module provides a robust retry mechanism for API calls with:
- Exponential backoff with optional jitter
- Error categorization (temporary vs permanent)
- Detailed logging of retry attempts
- Integration with ConfigManager and EnhancedLoggingSystem
"""

import asyncio
import random
from typing import Any, Callable, Optional, TypeVar, Union
from enum import Enum
from dataclasses import dataclass
from datetime import datetime

from backend.app.core.config_manager import ConfigManager
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem


T = TypeVar('T')


class ErrorCategory(Enum):
    """Categories of errors for retry logic."""
    TEMPORARY = "temporary"  # Network, timeout, temporary API issues
    PERMANENT = "permanent"  # Authorization, validation, not found
    RATE_LIMIT = "rate_limit"  # Rate limiting, flood wait


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    attempts: int = 0
    total_delay_ms: float = 0.0


class APIRetryWrapper:
    """
    Wrapper for API calls with automatic retry logic.
    
    Features:
    - Exponential backoff with configurable base delay
    - Optional jitter to prevent thundering herd
    - Error categorization (temporary vs permanent)
    - Detailed logging of all retry attempts
    - Integration with ConfigManager for configuration
    - Integration with EnhancedLoggingSystem for structured logging
    
    Example:
        retry_wrapper = APIRetryWrapper(config_manager, logger)
        
        result = await retry_wrapper.execute_with_retry(
            my_api_call,
            arg1, arg2,
            operation_name="fetch_user_data",
            kwarg1=value1
        )
        
        if result.success:
            print(f"Success after {result.attempts} attempts")
            return result.result
        else:
            print(f"Failed after {result.attempts} attempts: {result.error}")
    """
    
    def __init__(
        self,
        config_manager: ConfigManager,
        logger: EnhancedLoggingSystem
    ):
        """
        Initialize the API retry wrapper.
        
        Args:
            config_manager: Configuration manager instance
            logger: Enhanced logging system instance
        """
        self.config = config_manager
        self.logger = logger
        
        # Load configuration
        self.max_attempts = self.config.get_int("TELEGRAM_API_RETRY_MAX_ATTEMPTS", 5)
        self.delay_base = self.config.get_int("TELEGRAM_API_RETRY_DELAY_BASE", 1)
        self.jitter_enabled = self.config.get_bool("TELEGRAM_API_RETRY_JITTER", True)
        self.timeout = self.config.get_int("TELEGRAM_API_TIMEOUT", 30)
    
    async def execute_with_retry(
        self,
        func: Callable[..., T],
        *args,
        operation_name: str = "api_call",
        **kwargs
    ) -> RetryResult:
        """
        Execute a function with automatic retry logic.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            operation_name: Name of the operation for logging
            **kwargs: Keyword arguments for the function
            
        Returns:
            RetryResult: Result object with success status, result/error, and metadata
            
        Example:
            result = await retry_wrapper.execute_with_retry(
                client.get_entity,
                user_id,
                operation_name="get_user_entity"
            )
        """
        attempt = 0
        total_delay_ms = 0.0
        last_error = None
        
        # Log operation start
        await self.logger.log_info(
            "APIRetryWrapper",
            operation_name,
            f"Starting operation: {operation_name}",
            details={
                "max_attempts": self.max_attempts,
                "delay_base": self.delay_base,
                "jitter_enabled": self.jitter_enabled
            }
        )
        
        while attempt < self.max_attempts:
            attempt += 1
            
            try:
                # Log attempt
                await self.logger.log_debug(
                    "APIRetryWrapper",
                    operation_name,
                    f"Attempt {attempt}/{self.max_attempts}",
                    details={
                        "attempt": attempt,
                        "max_attempts": self.max_attempts
                    }
                )
                
                # Execute the function with timeout
                start_time = datetime.now()
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.timeout
                )
                duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                
                # Success - log and return
                await self.logger.log_info(
                    "APIRetryWrapper",
                    operation_name,
                    f"Operation succeeded on attempt {attempt}",
                    details={
                        "attempt": attempt,
                        "duration_ms": duration_ms,
                        "total_delay_ms": total_delay_ms
                    }
                )
                
                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempt,
                    total_delay_ms=total_delay_ms
                )
                
            except asyncio.TimeoutError as e:
                last_error = e
                error_category = ErrorCategory.TEMPORARY
                
                await self.logger.log_warning(
                    "APIRetryWrapper",
                    operation_name,
                    f"Timeout on attempt {attempt}/{self.max_attempts}",
                    details={
                        "attempt": attempt,
                        "timeout": self.timeout,
                        "error_category": error_category.value
                    }
                )
                
            except Exception as e:
                last_error = e
                error_category = self.categorize_error(e)
                
                # Log the error
                await self.logger.log_warning(
                    "APIRetryWrapper",
                    operation_name,
                    f"Error on attempt {attempt}/{self.max_attempts}: {type(e).__name__}",
                    error=e,
                    details={
                        "attempt": attempt,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "error_category": error_category.value
                    }
                )
                
                # Don't retry permanent errors
                if error_category == ErrorCategory.PERMANENT:
                    await self.logger.log_error(
                        "APIRetryWrapper",
                        operation_name,
                        f"Permanent error detected, not retrying: {type(e).__name__}",
                        error=e,
                        details={
                            "attempt": attempt,
                            "error_category": error_category.value
                        }
                    )
                    
                    return RetryResult(
                        success=False,
                        error=e,
                        attempts=attempt,
                        total_delay_ms=total_delay_ms
                    )
            
            # If we haven't returned yet, we need to retry
            if attempt < self.max_attempts:
                # Calculate backoff delay
                delay_seconds = self.calculate_backoff(attempt)
                delay_ms = delay_seconds * 1000
                total_delay_ms += delay_ms
                
                await self.logger.log_debug(
                    "APIRetryWrapper",
                    operation_name,
                    f"Waiting {delay_seconds:.2f}s before retry",
                    details={
                        "attempt": attempt,
                        "delay_seconds": delay_seconds,
                        "total_delay_ms": total_delay_ms
                    }
                )
                
                # Wait before retrying
                await asyncio.sleep(delay_seconds)
        
        # All attempts exhausted
        await self.logger.log_error(
            "APIRetryWrapper",
            operation_name,
            f"Operation failed after {self.max_attempts} attempts",
            error=last_error,
            details={
                "attempts": self.max_attempts,
                "total_delay_ms": total_delay_ms,
                "last_error_type": type(last_error).__name__ if last_error else "Unknown",
                "last_error_message": str(last_error) if last_error else "Unknown"
            }
        )
        
        return RetryResult(
            success=False,
            error=last_error,
            attempts=attempt,
            total_delay_ms=total_delay_ms
        )
    
    def is_temporary_error(self, error: Exception) -> bool:
        """
        Determine if an error is temporary and should be retried.
        
        Args:
            error: Exception to categorize
            
        Returns:
            bool: True if error is temporary and should be retried
            
        Example:
            if retry_wrapper.is_temporary_error(error):
                # Retry the operation
        """
        return self.categorize_error(error) == ErrorCategory.TEMPORARY
    
    def categorize_error(self, error: Exception) -> ErrorCategory:
        """
        Categorize an error for retry logic.
        
        Args:
            error: Exception to categorize
            
        Returns:
            ErrorCategory: Category of the error
            
        Categories:
        - TEMPORARY: Network errors, timeouts, temporary API issues (should retry)
        - PERMANENT: Authorization errors, validation errors, not found (should not retry)
        - RATE_LIMIT: Rate limiting errors (should retry with longer delay)
        """
        error_type = type(error).__name__
        error_message = str(error).lower()
        
        # Network and connection errors - temporary
        if error_type in [
            "ConnectionError",
            "TimeoutError",
            "NetworkError",
            "ConnectionResetError",
            "ConnectionAbortedError",
            "ConnectionRefusedError",
            "BrokenPipeError",
            "OSError",
            "IOError"
        ]:
            return ErrorCategory.TEMPORARY
        
        # Telegram-specific temporary errors
        if error_type in [
            "ServerError",
            "RpcCallFailError",
            "RpcMcgetFailError",
            "TimedOutError"
        ]:
            return ErrorCategory.TEMPORARY
        
        # Rate limiting errors
        if error_type in [
            "FloodWaitError",
            "FloodError",
            "SlowModeWaitError"
        ]:
            return ErrorCategory.RATE_LIMIT
        
        # Check error message for rate limit indicators
        if any(keyword in error_message for keyword in [
            "flood",
            "rate limit",
            "too many requests",
            "slow mode"
        ]):
            return ErrorCategory.RATE_LIMIT
        
        # Authorization and permission errors - permanent
        if error_type in [
            "AuthorizationError",
            "PermissionError",
            "AuthKeyError",
            "SessionPasswordNeededError",
            "UnauthorizedError",
            "ForbiddenError"
        ]:
            return ErrorCategory.PERMANENT
        
        # Validation and data errors - permanent
        if error_type in [
            "ValidationError",
            "ValueError",
            "TypeError",
            "InvalidDataError",
            "BadRequestError",
            "UserNotFoundError",
            "ChatNotFoundError",
            "ChannelPrivateError",
            "UsernameInvalidError",
            "UsernameNotOccupiedError"
        ]:
            return ErrorCategory.PERMANENT
        
        # Check error message for permanent error indicators
        if any(keyword in error_message for keyword in [
            "not found",
            "invalid",
            "forbidden",
            "unauthorized",
            "permission denied",
            "access denied",
            "bad request"
        ]):
            return ErrorCategory.PERMANENT
        
        # Default to temporary for unknown errors (safer to retry)
        return ErrorCategory.TEMPORARY
    
    def calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay with optional jitter.
        
        Args:
            attempt: Current attempt number (1-indexed)
            
        Returns:
            float: Delay in seconds
            
        Formula:
        - Base delay: delay_base * (2 ^ (attempt - 1))
        - With jitter: base_delay + random(0, base_delay * 0.1)
        
        Example:
            delay = retry_wrapper.calculate_backoff(3)  # Returns ~4 seconds (+ jitter)
        """
        # Calculate exponential backoff: delay_base * (2 ^ (attempt - 1))
        base_delay = self.delay_base * (2 ** (attempt - 1))
        
        # Add jitter if enabled (0-10% of base delay)
        if self.jitter_enabled:
            jitter = random.uniform(0, base_delay * 0.1)
            return base_delay + jitter
        
        return base_delay
    
    async def execute_with_custom_retry(
        self,
        func: Callable[..., T],
        *args,
        max_attempts: Optional[int] = None,
        delay_base: Optional[int] = None,
        jitter_enabled: Optional[bool] = None,
        operation_name: str = "api_call",
        **kwargs
    ) -> RetryResult:
        """
        Execute a function with custom retry parameters.
        
        This method allows overriding the default retry configuration for specific operations.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            max_attempts: Override max attempts (default: from config)
            delay_base: Override base delay (default: from config)
            jitter_enabled: Override jitter setting (default: from config)
            operation_name: Name of the operation for logging
            **kwargs: Keyword arguments for the function
            
        Returns:
            RetryResult: Result object with success status, result/error, and metadata
        """
        # Save original config
        original_max_attempts = self.max_attempts
        original_delay_base = self.delay_base
        original_jitter_enabled = self.jitter_enabled
        
        try:
            # Apply custom config
            if max_attempts is not None:
                self.max_attempts = max_attempts
            if delay_base is not None:
                self.delay_base = delay_base
            if jitter_enabled is not None:
                self.jitter_enabled = jitter_enabled
            
            # Execute with retry
            return await self.execute_with_retry(
                func,
                *args,
                operation_name=operation_name,
                **kwargs
            )
            
        finally:
            # Restore original config
            self.max_attempts = original_max_attempts
            self.delay_base = original_delay_base
            self.jitter_enabled = original_jitter_enabled
