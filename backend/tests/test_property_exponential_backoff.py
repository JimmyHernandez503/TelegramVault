"""
Property-based test for Exponential Backoff Calculation.

Feature: telegram-vault-enrichment-media-fixes
Property 13: Exponential Backoff Calculation

Tests that retry delays increase exponentially with each attempt.
"""

import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import MagicMock
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Mock dependencies
sys.modules['backend.app.services.telegram_service'] = MagicMock()

from app.services.media_retry_service import MediaRetryService


# Feature: telegram-vault-enrichment-media-fixes, Property 13: Exponential Backoff Calculation
@given(
    attempt_n=st.integers(min_value=1, max_value=10),
    base_delay=st.integers(min_value=1, max_value=10)
)
@settings(max_examples=100, deadline=None)
def test_property_exponential_backoff_calculation(attempt_n, base_delay):
    """
    **Validates: Requirements 5.2**
    
    Property: For any retry attempt N (where N > 1), the delay before retry 
    should be exponentially larger than the delay for attempt N-1.
    
    This test verifies that:
    1. Delay increases exponentially with attempt number
    2. The formula follows: delay = base * (2 ^ (attempt - 1))
    3. Delays are capped at a maximum value
    """
    # Arrange
    service = MediaRetryService()
    service._settings["retry_delay_base"] = base_delay
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False  # Disable jitter for predictable testing
    
    # Act
    delay_n = service._calculate_retry_delay(attempt_n)
    
    # Assert - Check exponential growth
    if attempt_n > 1:
        delay_n_minus_1 = service._calculate_retry_delay(attempt_n - 1)
        
        # Delay should be approximately double (allowing for cap)
        if delay_n < 300:  # Not capped
            assert delay_n >= delay_n_minus_1, \
                f"Delay for attempt {attempt_n} ({delay_n}s) should be >= delay for attempt {attempt_n-1} ({delay_n_minus_1}s)"
            
            # Should be approximately 2x (within rounding)
            expected_ratio = 2.0
            actual_ratio = delay_n / delay_n_minus_1 if delay_n_minus_1 > 0 else float('inf')
            
            # Allow some tolerance for rounding
            assert abs(actual_ratio - expected_ratio) < 0.1 or delay_n == 300, \
                f"Delay ratio should be ~2.0, got {actual_ratio:.2f} (delay_n={delay_n}, delay_n-1={delay_n_minus_1})"
    
    # Check that delay is capped at 300 seconds (5 minutes)
    assert delay_n <= 300, \
        f"Delay should be capped at 300 seconds, got {delay_n}"
    
    # Check that delay follows the formula (before cap)
    expected_delay = base_delay * (2 ** (attempt_n - 1))
    expected_delay_capped = min(expected_delay, 300)
    
    assert delay_n == expected_delay_capped, \
        f"Delay should be {expected_delay_capped}s (base={base_delay}, attempt={attempt_n}), got {delay_n}s"


# Feature: telegram-vault-enrichment-media-fixes, Property 13: Exponential Backoff Calculation
@given(
    attempt_sequence=st.lists(st.integers(min_value=1, max_value=10), min_size=2, max_size=10, unique=True).map(sorted)
)
@settings(max_examples=50, deadline=None)
def test_property_delays_increase_monotonically(attempt_sequence):
    """
    **Validates: Requirements 5.2**
    
    Property: For any sequence of retry attempts, delays should increase 
    monotonically (never decrease).
    
    This test verifies that:
    1. Each subsequent delay is >= the previous delay
    2. The sequence is non-decreasing
    3. This holds true even with the cap applied
    """
    # Arrange
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    # Act - Calculate delays for the sequence
    delays = [service._calculate_retry_delay(attempt) for attempt in attempt_sequence]
    
    # Assert - Delays should be non-decreasing
    for i in range(1, len(delays)):
        assert delays[i] >= delays[i-1], \
            f"Delay sequence should be non-decreasing: delays[{i}]={delays[i]} < delays[{i-1}]={delays[i-1]}"


# Feature: telegram-vault-enrichment-media-fixes, Property 13: Exponential Backoff Calculation
@given(
    attempt=st.integers(min_value=1, max_value=20),
    jitter_enabled=st.booleans()
)
@settings(max_examples=100, deadline=None)
def test_property_jitter_affects_delay(attempt, jitter_enabled):
    """
    **Validates: Requirements 5.2**
    
    Property: When jitter is enabled, the actual delay should vary around 
    the base exponential delay.
    
    This test verifies that:
    1. With jitter disabled, delays are deterministic
    2. With jitter enabled, delays vary but stay within reasonable bounds
    3. Jitter doesn't violate the exponential growth pattern
    """
    # Arrange
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = jitter_enabled
    
    # Act - Calculate delay multiple times
    delays = [service._calculate_retry_delay(attempt) for _ in range(10)]
    
    # Assert
    if jitter_enabled:
        # With jitter, delays should vary (unless capped)
        base_delay = 2 * (2 ** (attempt - 1))
        base_delay_capped = min(base_delay, 300)
        
        # All delays should be within jitter range (0.5x to 1.5x of base)
        min_expected = base_delay_capped * 0.5
        max_expected = min(base_delay_capped * 1.5, 300)
        
        for delay in delays:
            assert min_expected <= delay <= max_expected, \
                f"Delay with jitter should be in range [{min_expected}, {max_expected}], got {delay}"
    else:
        # Without jitter, all delays should be identical
        assert all(d == delays[0] for d in delays), \
            f"Without jitter, all delays should be identical, got {delays}"


# Unit tests for specific scenarios
def test_attempt_1_has_base_delay():
    """
    Unit test: First attempt should have base delay.
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    delay = service._calculate_retry_delay(1)
    
    # First attempt: 2 * (2^0) = 2 * 1 = 2
    assert delay == 2, f"First attempt should have base delay of 2s, got {delay}s"


def test_attempt_2_doubles_delay():
    """
    Unit test: Second attempt should double the delay.
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    delay = service._calculate_retry_delay(2)
    
    # Second attempt: 2 * (2^1) = 2 * 2 = 4
    assert delay == 4, f"Second attempt should have delay of 4s, got {delay}s"


def test_attempt_3_quadruples_delay():
    """
    Unit test: Third attempt should quadruple the base delay.
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    delay = service._calculate_retry_delay(3)
    
    # Third attempt: 2 * (2^2) = 2 * 4 = 8
    assert delay == 8, f"Third attempt should have delay of 8s, got {delay}s"


def test_delay_capped_at_300_seconds():
    """
    Unit test: Delay should be capped at 300 seconds (5 minutes).
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    # Attempt 10: 2 * (2^9) = 2 * 512 = 1024 seconds
    # Should be capped at 300
    delay = service._calculate_retry_delay(10)
    
    assert delay == 300, f"Delay should be capped at 300s, got {delay}s"


def test_large_attempt_number_still_capped():
    """
    Unit test: Even very large attempt numbers should be capped.
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    delay = service._calculate_retry_delay(100)
    
    assert delay == 300, f"Delay should be capped at 300s even for attempt 100, got {delay}s"


def test_exponential_growth_sequence():
    """
    Unit test: Verify the exponential growth sequence for first few attempts.
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    expected_delays = [2, 4, 8, 16, 32, 64, 128, 256, 300, 300]  # Last two capped
    
    for attempt in range(1, 11):
        delay = service._calculate_retry_delay(attempt)
        expected = expected_delays[attempt - 1]
        assert delay == expected, \
            f"Attempt {attempt} should have delay {expected}s, got {delay}s"


def test_jitter_produces_variation():
    """
    Unit test: With jitter enabled, delays should vary.
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = True
    
    # Calculate delay multiple times
    delays = [service._calculate_retry_delay(3) for _ in range(20)]
    
    # Should have some variation (not all identical)
    unique_delays = set(delays)
    assert len(unique_delays) > 1, \
        "With jitter enabled, delays should vary"
    
    # All delays should be within reasonable range
    # Base delay for attempt 3: 2 * (2^2) = 8
    # With jitter (0.5 to 1.5): 4 to 12
    for delay in delays:
        assert 4 <= delay <= 12, \
            f"Delay with jitter should be in range [4, 12], got {delay}"


def test_jitter_disabled_produces_consistent_delays():
    """
    Unit test: With jitter disabled, delays should be consistent.
    """
    service = MediaRetryService()
    service._settings["retry_delay_base"] = 2
    service._settings["exponential_backoff"] = True
    service._settings["jitter_enabled"] = False
    
    # Calculate delay multiple times
    delays = [service._calculate_retry_delay(3) for _ in range(20)]
    
    # All delays should be identical
    assert all(d == delays[0] for d in delays), \
        f"With jitter disabled, all delays should be identical, got {set(delays)}"
    
    # Should be exactly 8 seconds
    assert delays[0] == 8, \
        f"Delay for attempt 3 should be 8s, got {delays[0]}s"


def test_different_base_delays():
    """
    Unit test: Different base delays should produce proportionally different sequences.
    """
    service1 = MediaRetryService()
    service1._settings["retry_delay_base"] = 1
    service1._settings["exponential_backoff"] = True
    service1._settings["jitter_enabled"] = False
    
    service2 = MediaRetryService()
    service2._settings["retry_delay_base"] = 4
    service2._settings["exponential_backoff"] = True
    service2._settings["jitter_enabled"] = False
    
    # For attempt 3:
    # Service 1: 1 * (2^2) = 4
    # Service 2: 4 * (2^2) = 16
    delay1 = service1._calculate_retry_delay(3)
    delay2 = service2._calculate_retry_delay(3)
    
    assert delay1 == 4, f"Service 1 delay should be 4s, got {delay1}s"
    assert delay2 == 16, f"Service 2 delay should be 16s, got {delay2}s"
    assert delay2 == delay1 * 4, \
        "Delay should scale proportionally with base delay"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
