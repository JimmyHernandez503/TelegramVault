"""
Enhanced API Rate Limiter Component

This component handles Telegram API rate limiting, FloodWaitError handling,
HTTP 429 responses, and implements proactive throttling to prevent quota violations.
Enhanced with media-specific rate limiting and multi-account load balancing.

Requirements: 5.1, 5.2, 5.3, 5.4, 9.1, 9.2, 9.3, 9.4
"""

import asyncio
import time
import logging
import random
from typing import Callable, Any, Optional, Dict, NamedTuple, List
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
from telethon.errors import FloodWaitError
from telethon import TelegramClient
from backend.app.core.logging_config import get_logger

logger = get_logger("api_rate_limiter")


class OperationType(Enum):
    """Types of operations for specialized rate limiting"""
    GENERAL = "general"
    MEDIA_DOWNLOAD = "media_download"
    PROFILE_PHOTO = "profile_photo"
    STORY_DOWNLOAD = "story_download"
    USER_INFO = "user_info"
    MESSAGE_FETCH = "message_fetch"


class AccountStatus(Enum):
    """Account status for load balancing"""
    ACTIVE = "active"
    FLOOD_WAIT = "flood_wait"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"
    DISABLED = "disabled"


class RateLimitStatus(NamedTuple):
    """Current rate limit status information"""
    requests_in_window: int
    window_start: float
    window_duration: int
    quota_remaining: int
    quota_reset_time: Optional[datetime]
    is_throttled: bool
    next_available_time: Optional[datetime]
    operation_type: Optional[str] = None
    account_id: Optional[str] = None


class AccountInfo(NamedTuple):
    """Information about an account for load balancing"""
    account_id: str
    client: TelegramClient
    status: AccountStatus
    last_used: float
    flood_wait_until: float
    consecutive_errors: int
    total_requests: int
    success_rate: float


class RequestInfo(NamedTuple):
    """Information about a queued request"""
    timestamp: float
    priority: int
    request_id: str
    operation_type: OperationType
    account_id: Optional[str] = None


class APIRateLimiter:
    """
    Enhanced Telegram API rate limiting and retry logic with media-specific features.
    
    This component handles FloodWaitError, HTTP 429 responses, implements
    exponential backoff, request queuing, proactive throttling, and multi-account
    load balancing for media downloads.
    """
    
    def __init__(self, 
                 requests_per_second: int = 20,
                 burst_limit: int = 30,
                 window_duration: int = 60,
                 max_queue_size: int = 1000):
        """
        Initialize the enhanced API rate limiter.
        
        Args:
            requests_per_second: Base rate limit for requests
            burst_limit: Maximum burst requests allowed
            window_duration: Time window for rate limiting (seconds)
            max_queue_size: Maximum number of queued requests
        """
        self.requests_per_second = requests_per_second
        self.burst_limit = burst_limit
        self.window_duration = window_duration
        self.max_queue_size = max_queue_size
        
        # Operation-specific rate limits
        self._operation_limits = {
            OperationType.GENERAL: {
                'requests_per_second': requests_per_second,
                'burst_limit': burst_limit,
                'concurrent_limit': 10
            },
            OperationType.MEDIA_DOWNLOAD: {
                'requests_per_second': 10,  # More conservative for media
                'burst_limit': 15,
                'concurrent_limit': 5
            },
            OperationType.PROFILE_PHOTO: {
                'requests_per_second': 15,
                'burst_limit': 20,
                'concurrent_limit': 3
            },
            OperationType.STORY_DOWNLOAD: {
                'requests_per_second': 8,  # Stories are more rate-limited
                'burst_limit': 12,
                'concurrent_limit': 3
            },
            OperationType.USER_INFO: {
                'requests_per_second': 25,
                'burst_limit': 35,
                'concurrent_limit': 8
            },
            OperationType.MESSAGE_FETCH: {
                'requests_per_second': 30,
                'burst_limit': 40,
                'concurrent_limit': 10
            }
        }
        
        # Request tracking per operation type
        self._request_times = defaultdict(deque)
        self._window_start = time.time()
        
        # Multi-account management
        self._accounts: Dict[str, AccountInfo] = {}
        self._account_rotation_enabled = False
        self._load_balancing_enabled = False
        
        # Queue management per operation type
        self._request_queues = {
            op_type: asyncio.Queue(maxsize=max_queue_size // len(OperationType))
            for op_type in OperationType
        }
        self._queue_processor_tasks = {}
        self._is_processing = defaultdict(bool)
        
        # Concurrent request tracking
        self._concurrent_requests = defaultdict(int)
        self._concurrent_semaphores = {
            op_type: asyncio.Semaphore(limits['concurrent_limit'])
            for op_type, limits in self._operation_limits.items()
        }
        
        # Rate limit state per operation
        self._flood_wait_until = defaultdict(float)
        self._last_429_time = defaultdict(float)
        self._consecutive_429s = defaultdict(int)
        self._quota_remaining = defaultdict(lambda: None)
        self._quota_reset_time = defaultdict(lambda: None)
        
        # Enhanced statistics
        self._total_requests = defaultdict(int)
        self._total_flood_waits = defaultdict(int)
        self._total_429s = defaultdict(int)
        self._total_queued = defaultdict(int)
        self._total_successful = defaultdict(int)
        self._total_failed = defaultdict(int)
        
        # Media-specific settings
        self._media_retry_delays = [1, 2, 5, 10, 30]  # Progressive delays for media
        self._media_timeout_multiplier = 2.0  # Longer timeouts for media operations
        
        self.logger = logger
        
    async def execute_with_rate_limit(self, api_call: Callable, *args, 
                                     operation_type: OperationType = OperationType.GENERAL,
                                     account_id: Optional[str] = None,
                                     **kwargs) -> Any:
        """
        Executes API call with automatic rate limiting and operation-specific handling.
        
        This method handles all rate limiting logic including FloodWaitError,
        HTTP 429 responses, proactive throttling, and multi-account load balancing.
        
        Args:
            api_call: The API function to call
            operation_type: Type of operation for specialized rate limiting
            account_id: Specific account ID to use (optional)
            *args: Arguments for the API call
            **kwargs: Keyword arguments for the API call
            
        Returns:
            Result of the API call
        """
        request_id = f"req_{operation_type.value}_{int(time.time() * 1000)}_{id(api_call)}"
        
        # Use concurrent semaphore for operation type
        async with self._concurrent_semaphores[operation_type]:
            try:
                self._concurrent_requests[operation_type] += 1
                
                # Select best account if multi-account is enabled
                selected_account = None
                if self._load_balancing_enabled and account_id is None:
                    selected_account = await self._select_best_account(operation_type)
                    if selected_account:
                        account_id = selected_account.account_id
                
                # Check if we need to wait due to previous flood wait
                await self._wait_for_flood_wait(operation_type, account_id)
                
                # Check if we need to throttle proactively
                await self._proactive_throttle(operation_type)
                
                # Execute the API call with retry logic
                result = await self._execute_with_retry(
                    api_call, request_id, operation_type, account_id, *args, **kwargs
                )
                
                # Update account success metrics
                if selected_account:
                    await self._update_account_success(selected_account.account_id)
                
                self._total_successful[operation_type] += 1
                return result
                
            except Exception as e:
                self._total_failed[operation_type] += 1
                
                # Update account error metrics
                if account_id:
                    await self._update_account_error(account_id)
                
                self.logger.error(f"API call {request_id} failed: {e}")
                raise
            finally:
                self._concurrent_requests[operation_type] -= 1
    
    async def _execute_with_retry(self, api_call: Callable, request_id: str, 
                                operation_type: OperationType, account_id: Optional[str],
                                *args, max_retries: int = None, **kwargs) -> Any:
        """Execute API call with retry logic for different error types"""
        # Use operation-specific retry count
        if max_retries is None:
            if operation_type == OperationType.MEDIA_DOWNLOAD:
                max_retries = 5  # More retries for media
            elif operation_type == OperationType.STORY_DOWNLOAD:
                max_retries = 3  # Fewer retries for stories (they expire)
            else:
                max_retries = 3
        
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                # Record request timing
                self._record_request(operation_type)
                
                # Apply operation-specific delay
                if attempt > 0:
                    delay = self._get_retry_delay(operation_type, attempt)
                    await asyncio.sleep(delay)
                
                # Execute the API call
                result = await api_call(*args, **kwargs)
                
                # Reset consecutive 429 counter on success
                self._consecutive_429s[operation_type] = 0
                
                self.logger.debug(f"API call {request_id} succeeded on attempt {attempt + 1}")
                return result
                
            except FloodWaitError as e:
                last_error = e
                # Enhanced API error logging for FloodWaitError in retry context
                self.logger.warning(
                    f"FloodWaitError in retry loop. "
                    f"Request: {request_id}, "
                    f"Operation: {operation_type.value}, "
                    f"Account: {account_id}, "
                    f"Attempt: {attempt + 1}/{max_retries + 1}, "
                    f"Wait time: {e.seconds}s, "
                    f"Action: Handling flood wait"
                )
                await self.handle_flood_wait(e, operation_type, account_id)
                
                # Don't retry immediately after flood wait - the wait is the retry
                if attempt < max_retries:
                    continue
                else:
                    break
                    
            except Exception as e:
                last_error = e
                
                # Check if this is an HTTP 429 error
                if self._is_http_429_error(e):
                    # Enhanced API error logging for HTTP 429 in retry context
                    self.logger.warning(
                        f"HTTP 429 error in retry loop. "
                        f"Request: {request_id}, "
                        f"Operation: {operation_type.value}, "
                        f"Account: {account_id}, "
                        f"Attempt: {attempt + 1}/{max_retries + 1}, "
                        f"Error: {e}, "
                        f"Action: Handling HTTP 429 with backoff"
                    )
                    await self.handle_http_429(e, operation_type, account_id)
                    
                    if attempt < max_retries:
                        continue
                else:
                    # Enhanced API error logging for non-recoverable errors
                    self.logger.error(
                        f"Non-recoverable API error. "
                        f"Request: {request_id}, "
                        f"Operation: {operation_type.value}, "
                        f"Account: {account_id}, "
                        f"Attempt: {attempt + 1}/{max_retries + 1}, "
                        f"Error type: {type(e).__name__}, "
                        f"Error: {e}, "
                        f"Action: Not retrying"
                    )
                    # Non-recoverable error, don't retry
                    break
        
        # All retries exhausted - enhanced final error logging
        self.logger.error(
            f"API call completely failed after all retries. "
            f"Request: {request_id}, "
            f"Operation: {operation_type.value}, "
            f"Account: {account_id}, "
            f"Total attempts: {max_retries + 1}, "
            f"Final error type: {type(last_error).__name__}, "
            f"Final error: {last_error}, "
            f"Total requests made: {self._total_requests[operation_type]}, "
            f"Total flood waits: {self._total_flood_waits[operation_type]}, "
            f"Total 429s: {self._total_429s[operation_type]}"
        )
        raise last_error
    
    async def handle_flood_wait(self, error: FloodWaitError, 
                              operation_type: OperationType = OperationType.GENERAL,
                              account_id: Optional[str] = None) -> None:
        """
        Handles FloodWaitError with proper wait time and account management.
        
        Args:
            error: The FloodWaitError containing wait time
            operation_type: Type of operation that caused the flood wait
            account_id: Account ID that received the flood wait
        """
        wait_seconds = error.seconds
        self._total_flood_waits[operation_type] += 1
        
        # Set flood wait end time for operation type
        self._flood_wait_until[operation_type] = time.time() + wait_seconds
        
        # Update account status if account_id provided
        if account_id and account_id in self._accounts:
            account = self._accounts[account_id]
            updated_account = account._replace(
                status=AccountStatus.FLOOD_WAIT,
                flood_wait_until=time.time() + wait_seconds,
                consecutive_errors=account.consecutive_errors + 1
            )
            self._accounts[account_id] = updated_account
        
        # Enhanced API error logging for FloodWaitError
        self.logger.error(
            f"FloodWaitError encountered. "
            f"Operation: {operation_type.value}, "
            f"Account: {account_id}, "
            f"Wait time: {wait_seconds}s, "
            f"Wait until: {datetime.fromtimestamp(self._flood_wait_until[operation_type])}, "
            f"Total flood waits for {operation_type.value}: {self._total_flood_waits[operation_type]}, "
            f"Error details: {error}, "
            f"Action: Waiting for specified duration"
        )
        
        # Wait for the specified duration
        await asyncio.sleep(wait_seconds)
        
        # Update account status back to active
        if account_id and account_id in self._accounts:
            account = self._accounts[account_id]
            updated_account = account._replace(
                status=AccountStatus.ACTIVE,
                flood_wait_until=0.0
            )
            self._accounts[account_id] = updated_account
        
        self.logger.info(
            f"FloodWait period completed. "
            f"Operation: {operation_type.value}, "
            f"Account: {account_id}, "
            f"Waited: {wait_seconds}s, "
            f"Resuming API calls at: {datetime.now()}"
        )
    
    async def handle_http_429(self, error: Exception, 
                             operation_type: OperationType = OperationType.GENERAL,
                             account_id: Optional[str] = None) -> None:
        """
        Handles HTTP 429 with exponential backoff and account management.
        
        Args:
            error: The HTTP 429 error (could be various exception types)
            operation_type: Type of operation that caused the 429
            account_id: Account ID that received the 429
        """
        self._total_429s[operation_type] += 1
        self._consecutive_429s[operation_type] += 1
        self._last_429_time[operation_type] = time.time()
        
        # Update account status if account_id provided
        if account_id and account_id in self._accounts:
            account = self._accounts[account_id]
            updated_account = account._replace(
                status=AccountStatus.RATE_LIMITED,
                consecutive_errors=account.consecutive_errors + 1
            )
            self._accounts[account_id] = updated_account
        
        # Exponential backoff: 2^attempt seconds, capped at 300 seconds (5 minutes)
        wait_time = min(2 ** self._consecutive_429s[operation_type], 300)
        
        # Enhanced API error logging for HTTP 429
        self.logger.error(
            f"HTTP 429 Rate Limit Error. "
            f"Operation: {operation_type.value}, "
            f"Account: {account_id}, "
            f"Error type: {type(error).__name__}, "
            f"Error message: {error}, "
            f"Consecutive 429s: {self._consecutive_429s[operation_type]}, "
            f"Total 429s for {operation_type.value}: {self._total_429s[operation_type]}, "
            f"Backoff time: {wait_time}s, "
            f"Retry at: {datetime.fromtimestamp(time.time() + wait_time)}, "
            f"Action: Exponential backoff wait"
        )
        
        await asyncio.sleep(wait_time)
        
        # Update account status back to active
        if account_id and account_id in self._accounts:
            account = self._accounts[account_id]
            updated_account = account._replace(status=AccountStatus.ACTIVE)
            self._accounts[account_id] = updated_account
        
        self.logger.info(
            f"HTTP 429 backoff completed. "
            f"Operation: {operation_type.value}, "
            f"Account: {account_id}, "
            f"Waited: {wait_time}s, "
            f"Consecutive 429s: {self._consecutive_429s[operation_type]}"
        )
    
    async def wait_if_needed(self, operation_type: OperationType = OperationType.GENERAL) -> None:
        """
        Wait if rate limiting is needed for the specified operation type.
        
        Args:
            operation_type: Type of operation to check rate limits for
        """
        await self._wait_for_flood_wait(operation_type)
        await self._proactive_throttle(operation_type)
    
    def add_account(self, account_id: str, client: TelegramClient) -> None:
        """
        Add an account for multi-account load balancing.
        
        Args:
            account_id: Unique identifier for the account
            client: Telegram client for the account
        """
        account_info = AccountInfo(
            account_id=account_id,
            client=client,
            status=AccountStatus.ACTIVE,
            last_used=0.0,
            flood_wait_until=0.0,
            consecutive_errors=0,
            total_requests=0,
            success_rate=1.0
        )
        
        self._accounts[account_id] = account_info
        self.logger.info(f"Added account {account_id} for load balancing")
    
    def remove_account(self, account_id: str) -> None:
        """
        Remove an account from load balancing.
        
        Args:
            account_id: Account ID to remove
        """
        if account_id in self._accounts:
            del self._accounts[account_id]
            self.logger.info(f"Removed account {account_id} from load balancing")
    
    def enable_load_balancing(self, enable: bool = True) -> None:
        """
        Enable or disable multi-account load balancing.
        
        Args:
            enable: Whether to enable load balancing
        """
        self._load_balancing_enabled = enable
        self.logger.info(f"Load balancing {'enabled' if enable else 'disabled'}")
    
    async def _select_best_account(self, operation_type: OperationType) -> Optional[AccountInfo]:
        """
        Select the best account for the given operation type.
        
        Args:
            operation_type: Type of operation
            
        Returns:
            Best available account or None
        """
        if not self._accounts:
            return None
        
        current_time = time.time()
        available_accounts = []
        
        for account in self._accounts.values():
            # Skip accounts in flood wait or error state
            if account.status == AccountStatus.FLOOD_WAIT:
                if current_time < account.flood_wait_until:
                    continue
                else:
                    # Update status if flood wait is over
                    updated_account = account._replace(
                        status=AccountStatus.ACTIVE,
                        flood_wait_until=0.0
                    )
                    self._accounts[account.account_id] = updated_account
                    account = updated_account
            
            if account.status in [AccountStatus.ACTIVE, AccountStatus.RATE_LIMITED]:
                available_accounts.append(account)
        
        if not available_accounts:
            return None
        
        # Select account based on success rate and last used time
        best_account = min(available_accounts, key=lambda a: (
            -a.success_rate,  # Higher success rate is better (negative for min)
            -a.last_used,     # Less recently used is better (negative for min)
            a.consecutive_errors  # Fewer errors is better
        ))
        
        return best_account
    
    async def _update_account_success(self, account_id: str) -> None:
        """Update account metrics on successful request."""
        if account_id in self._accounts:
            account = self._accounts[account_id]
            total_requests = account.total_requests + 1
            success_rate = (account.success_rate * account.total_requests + 1) / total_requests
            
            updated_account = account._replace(
                last_used=time.time(),
                total_requests=total_requests,
                success_rate=success_rate,
                consecutive_errors=0
            )
            self._accounts[account_id] = updated_account
    
    async def _update_account_error(self, account_id: str) -> None:
        """Update account metrics on failed request."""
        if account_id in self._accounts:
            account = self._accounts[account_id]
            total_requests = account.total_requests + 1
            success_rate = (account.success_rate * account.total_requests) / total_requests
            
            updated_account = account._replace(
                last_used=time.time(),
                total_requests=total_requests,
                success_rate=success_rate,
                consecutive_errors=account.consecutive_errors + 1
            )
            self._accounts[account_id] = updated_account
    
    def _get_retry_delay(self, operation_type: OperationType, attempt: int) -> float:
        """
        Get retry delay for specific operation type and attempt.
        
        Args:
            operation_type: Type of operation
            attempt: Retry attempt number (0-based)
            
        Returns:
            Delay in seconds
        """
        if operation_type == OperationType.MEDIA_DOWNLOAD:
            # Use progressive delays for media downloads
            if attempt < len(self._media_retry_delays):
                base_delay = self._media_retry_delays[attempt]
            else:
                base_delay = self._media_retry_delays[-1]
            
            # Add jitter
            jitter = random.uniform(0.5, 1.5)
            return base_delay * jitter
        
        elif operation_type == OperationType.STORY_DOWNLOAD:
            # Shorter delays for stories (they expire quickly)
            return min(2 ** attempt, 10) + random.uniform(0, 2)
        
        else:
            # Standard exponential backoff
            return min(2 ** attempt, 30) + random.uniform(0, 1)
    
    async def queue_request(self, api_call: Callable, priority: int = 1, 
                          operation_type: OperationType = OperationType.GENERAL,
                          account_id: Optional[str] = None,
                          *args, **kwargs) -> Any:
        """
        Queues a request for later execution when rate limits allow.
        
        Args:
            api_call: The API function to call
            priority: Request priority (higher = more important)
            operation_type: Type of operation for specialized handling
            account_id: Specific account ID to use (optional)
            *args: Arguments for the API call
            **kwargs: Keyword arguments for the API call
            
        Returns:
            Result of the API call when executed
        """
        queue = self._request_queues[operation_type]
        
        if queue.full():
            raise Exception(f"Request queue is full for {operation_type.value}")
        
        request_id = f"queued_{operation_type.value}_{int(time.time() * 1000)}_{id(api_call)}"
        future = asyncio.Future()
        
        # Put request in queue
        await queue.put({
            'api_call': api_call,
            'args': args,
            'kwargs': kwargs,
            'priority': priority,
            'request_id': request_id,
            'future': future,
            'timestamp': time.time(),
            'operation_type': operation_type,
            'account_id': account_id
        })
        
        self._total_queued[operation_type] += 1
        self.logger.debug(f"Queued {operation_type.value} request {request_id} with priority {priority}")
        
        # Start queue processor if not running
        if not self._is_processing[operation_type]:
            await self._start_queue_processor(operation_type)
        
        # Wait for result
        return await future
    
    async def _start_queue_processor(self, operation_type: OperationType):
        """Start the background queue processor for specific operation type"""
        if (operation_type in self._queue_processor_tasks and 
            self._queue_processor_tasks[operation_type] and 
            not self._queue_processor_tasks[operation_type].done()):
            return
        
        self._is_processing[operation_type] = True
        self._queue_processor_tasks[operation_type] = asyncio.create_task(
            self._process_queue(operation_type)
        )
        self.logger.info(f"Started request queue processor for {operation_type.value}")
    
    async def _process_queue(self, operation_type: OperationType):
        """Background task to process queued requests for specific operation type"""
        try:
            queue = self._request_queues[operation_type]
            
            while self._is_processing[operation_type]:
                try:
                    # Get next request from queue (with timeout)
                    request = await asyncio.wait_for(
                        queue.get(), 
                        timeout=10.0
                    )
                    
                    # Execute the request with rate limiting
                    try:
                        result = await self.execute_with_rate_limit(
                            request['api_call'],
                            operation_type=operation_type,
                            account_id=request.get('account_id'),
                            *request['args'],
                            **request['kwargs']
                        )
                        request['future'].set_result(result)
                        
                    except Exception as e:
                        request['future'].set_exception(e)
                    
                    # Mark task as done
                    queue.task_done()
                    
                except asyncio.TimeoutError:
                    # No requests in queue, continue waiting
                    continue
                    
        except Exception as e:
            self.logger.error(f"Queue processor error for {operation_type.value}: {e}")
        finally:
            self._is_processing[operation_type] = False
            self.logger.info(f"Request queue processor stopped for {operation_type.value}")
    
    async def _wait_for_flood_wait(self, operation_type: OperationType = OperationType.GENERAL, account_id: Optional[str] = None):
        """Wait if we're currently in a flood wait period for the operation type"""
        current_time = time.time()
        
        # Check operation-specific flood wait
        if self._flood_wait_until[operation_type] > current_time:
            wait_time = self._flood_wait_until[operation_type] - current_time
            self.logger.info(f"Waiting {wait_time:.1f}s for {operation_type.value} flood wait to end")
            await asyncio.sleep(wait_time)
        
        # Check account-specific flood wait
        if account_id and account_id in self._accounts:
            account = self._accounts[account_id]
            if account.flood_wait_until > current_time:
                wait_time = account.flood_wait_until - current_time
                self.logger.info(f"Waiting {wait_time:.1f}s for account {account_id} flood wait to end")
                await asyncio.sleep(wait_time)
    
    async def _proactive_throttle(self, operation_type: OperationType = OperationType.GENERAL):
        """Proactively throttle requests to prevent quota violations for operation type"""
        current_time = time.time()
        
        # Get operation-specific limits
        limits = self._operation_limits[operation_type]
        requests_per_second = limits['requests_per_second']
        burst_limit = limits['burst_limit']
        
        # Clean old request times outside the window for this operation type
        request_times = self._request_times[operation_type]
        while (request_times and 
               current_time - request_times[0] > self.window_duration):
            request_times.popleft()
        
        # Check if we're approaching rate limits
        requests_in_window = len(request_times)
        
        if requests_in_window >= burst_limit:
            # We've hit the burst limit, wait until we can make another request
            oldest_request = request_times[0]
            wait_time = self.window_duration - (current_time - oldest_request)
            
            if wait_time > 0:
                self.logger.info(f"Proactive throttling for {operation_type.value}: waiting {wait_time:.1f}s")
                await asyncio.sleep(wait_time)
        
        elif requests_in_window >= requests_per_second:
            # Approaching rate limit, add small delay
            delay = 1.0 / requests_per_second
            await asyncio.sleep(delay)
    
    def _record_request(self, operation_type: OperationType = OperationType.GENERAL):
        """Record a request timestamp for rate limiting by operation type"""
        current_time = time.time()
        self._request_times[operation_type].append(current_time)
        self._total_requests[operation_type] += 1
    
    def _is_http_429_error(self, error: Exception) -> bool:
        """Check if an error is an HTTP 429 error"""
        error_str = str(error).lower()
        return any(indicator in error_str for indicator in [
            '429', 'too many requests', 'rate limit', 'quota exceeded'
        ])
    
    def get_rate_limit_status(self, operation_type: OperationType = OperationType.GENERAL, account_id: Optional[str] = None) -> RateLimitStatus:
        """
        Returns current rate limit status and quotas for operation type.
        
        Args:
            operation_type: Type of operation to check status for
            account_id: Specific account ID to check (optional)
            
        Returns:
            RateLimitStatus with current status information
        """
        current_time = time.time()
        
        # Clean old request times for this operation type
        request_times = self._request_times[operation_type]
        while (request_times and 
               current_time - request_times[0] > self.window_duration):
            request_times.popleft()
        
        # Get operation-specific limits
        limits = self._operation_limits[operation_type]
        burst_limit = limits['burst_limit']
        
        requests_in_window = len(request_times)
        is_throttled = (self._flood_wait_until[operation_type] > current_time or 
                       requests_in_window >= burst_limit)
        
        next_available = None
        if self._flood_wait_until[operation_type] > current_time:
            next_available = datetime.fromtimestamp(self._flood_wait_until[operation_type])
        elif requests_in_window >= burst_limit and request_times:
            oldest_request = request_times[0]
            next_available_time = oldest_request + self.window_duration
            next_available = datetime.fromtimestamp(next_available_time)
        
        return RateLimitStatus(
            requests_in_window=requests_in_window,
            window_start=self._window_start,
            window_duration=self.window_duration,
            quota_remaining=self._quota_remaining[operation_type],
            quota_reset_time=self._quota_reset_time[operation_type],
            is_throttled=is_throttled,
            next_available_time=next_available,
            operation_type=operation_type.value,
            account_id=account_id
        )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive rate limiter statistics for all operation types"""
        stats = {
            'operation_stats': {},
            'account_stats': {},
            'queue_stats': {},
            'global_stats': {}
        }
        
        # Operation-specific statistics
        for op_type in OperationType:
            stats['operation_stats'][op_type.value] = {
                'total_requests': self._total_requests[op_type],
                'total_flood_waits': self._total_flood_waits[op_type],
                'total_429s': self._total_429s[op_type],
                'total_queued': self._total_queued[op_type],
                'total_successful': self._total_successful[op_type],
                'total_failed': self._total_failed[op_type],
                'consecutive_429s': self._consecutive_429s[op_type],
                'concurrent_requests': self._concurrent_requests[op_type],
                'current_status': self.get_rate_limit_status(op_type)._asdict()
            }
        
        # Account statistics
        for account_id, account in self._accounts.items():
            stats['account_stats'][account_id] = {
                'status': account.status.value,
                'last_used': account.last_used,
                'flood_wait_until': account.flood_wait_until,
                'consecutive_errors': account.consecutive_errors,
                'total_requests': account.total_requests,
                'success_rate': account.success_rate
            }
        
        # Queue statistics
        for op_type in OperationType:
            if op_type in self._request_queues:
                stats['queue_stats'][op_type.value] = {
                    'queue_size': self._request_queues[op_type].qsize(),
                    'is_processing': self._is_processing[op_type]
                }
        
        # Global statistics
        stats['global_stats'] = {
            'load_balancing_enabled': self._load_balancing_enabled,
            'total_accounts': len(self._accounts),
            'active_accounts': sum(1 for a in self._accounts.values() if a.status == AccountStatus.ACTIVE),
            'total_operation_types': len(OperationType)
        }
        
        return stats
    
    async def stop(self):
        """Stop the rate limiter and clean up resources"""
        # Stop all queue processors
        for op_type in OperationType:
            self._is_processing[op_type] = False
        
        # Cancel all queue processor tasks
        for task in self._queue_processor_tasks.values():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Clear any remaining queued requests
        for op_type, queue in self._request_queues.items():
            while not queue.empty():
                try:
                    request = queue.get_nowait()
                    if 'future' in request:
                        request['future'].cancel()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
        
        self.logger.info("API Rate Limiter stopped")


# Global instance
api_rate_limiter = APIRateLimiter()