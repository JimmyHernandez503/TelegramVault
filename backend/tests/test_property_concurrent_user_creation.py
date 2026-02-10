"""
Property-Based Test for Concurrent User Creation Uniqueness

Feature: telegram-vault-enrichment-media-fixes
Property 26: Concurrent User Creation Uniqueness

For any concurrent attempts to create the same user (by telegram_id),
only one user record should exist in the database after all operations complete.

Validates: Requirements 8.3
"""

import pytest
import asyncio
from sqlalchemy import text
from backend.app.db.database import async_session_maker
from backend.app.services.user_management_service import (
    user_management_service,
    TelegramUserData
)


class TestPropertyConcurrentUserCreationUniqueness:
    """
    Property-Based Test: Concurrent User Creation Uniqueness
    
    This test validates that for ANY concurrent attempts to create the same user,
    only one record exists in the database after all operations complete.
    """
    
    # Test cases with different concurrency levels
    test_cases = [
        # (telegram_id, num_concurrent_operations, user_data_variants)
        (
            3000000001,
            2,
            [
                {"username": "user1_v1", "first_name": "First1_v1"},
                {"username": "user1_v2", "first_name": "First1_v2"}
            ]
        ),
        (
            3000000002,
            3,
            [
                {"username": "user2_v1", "first_name": "First2_v1"},
                {"username": "user2_v2", "first_name": "First2_v2"},
                {"username": "user2_v3", "first_name": "First2_v3"}
            ]
        ),
        (
            3000000003,
            5,
            [
                {"username": "user3_v1", "first_name": "First3_v1"},
                {"username": "user3_v2", "first_name": "First3_v2"},
                {"username": "user3_v3", "first_name": "First3_v3"},
                {"username": "user3_v4", "first_name": "First3_v4"},
                {"username": "user3_v5", "first_name": "First3_v5"}
            ]
        ),
        (
            3000000004,
            10,
            [
                {"username": f"user4_v{i}", "first_name": f"First4_v{i}"}
                for i in range(1, 11)
            ]
        ),
        (
            3000000005,
            2,
            [
                {"username": None, "first_name": "First5_v1"},
                {"username": "user5_v2", "first_name": None}
            ]
        ),
        (
            3000000006,
            3,
            [
                {"username": "user6", "first_name": "First6", "last_name": None},
                {"username": "user6", "first_name": "First6", "last_name": "Last6_v2"},
                {"username": "user6", "first_name": "First6", "last_name": "Last6_v3"}
            ]
        ),
        (
            3000000007,
            4,
            [
                {"username": "user7", "first_name": "First7", "phone": None},
                {"username": "user7", "first_name": "First7", "phone": "+1111111111"},
                {"username": "user7", "first_name": "First7", "phone": "+2222222222"},
                {"username": "user7", "first_name": "First7", "phone": "+3333333333"}
            ]
        ),
        (
            3000000008,
            5,
            [
                {"username": "user8", "first_name": "First8", "is_bot": False},
                {"username": "user8", "first_name": "First8", "is_bot": True},
                {"username": "user8", "first_name": "First8", "is_bot": False},
                {"username": "user8", "first_name": "First8", "is_bot": True},
                {"username": "user8", "first_name": "First8", "is_bot": False}
            ]
        ),
    ]
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("telegram_id,num_concurrent,user_data_variants", test_cases)
    async def test_property_concurrent_user_creation_uniqueness(
        self,
        telegram_id,
        num_concurrent,
        user_data_variants
    ):
        """
        Property Test: For any concurrent attempts to create the same user,
        only one user record should exist after all operations complete.
        
        **Validates: Requirements 8.3**
        """
        # Arrange: Create multiple user data objects with same telegram_id
        user_data_list = []
        for i in range(num_concurrent):
            variant = user_data_variants[i]
            user_data = TelegramUserData(
                telegram_id=telegram_id,  # Same telegram_id for all
                username=variant.get("username"),
                first_name=variant.get("first_name"),
                last_name=variant.get("last_name"),
                phone=variant.get("phone"),
                is_bot=variant.get("is_bot", False),
                is_premium=variant.get("is_premium", False)
            )
            user_data_list.append(user_data)
        
        # Act: Execute all upsert operations concurrently
        tasks = [
            user_management_service.upsert_user(user_data)
            for user_data in user_data_list
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Assert 1: All operations should complete (no unhandled exceptions)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                pytest.fail(
                    f"Concurrent operation {i+1}/{num_concurrent} raised exception: {result}. "
                    f"UPSERT should handle conflicts gracefully without raising exceptions."
                )
        
        # Assert 2: All operations should return a user object (not None)
        successful_results = [r for r in results if r is not None]
        assert len(successful_results) == num_concurrent, (
            f"Expected all {num_concurrent} operations to succeed, "
            f"but only {len(successful_results)} returned a user object."
        )
        
        # Assert 3: CRITICAL - Only ONE record should exist in database
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            count = result.scalar()
            
            assert count == 1, (
                f"CRITICAL FAILURE: Expected exactly 1 user record for telegram_id={telegram_id} "
                f"after {num_concurrent} concurrent operations, but found {count} records. "
                f"This indicates that UPSERT is not properly handling concurrent conflicts."
            )
        
        # Assert 4: Verify the user has valid data
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT * FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            db_user = result.fetchone()
            
            assert db_user is not None, f"User with telegram_id={telegram_id} not found"
            assert db_user.telegram_id == telegram_id
            
            # Verify default values are set
            assert db_user.has_stories is False
            assert db_user.messages_count == 0
            assert db_user.groups_count == 0
            
            # Verify at least one of the concurrent operations' data is present
            # (We can't predict which one wins, but it should be one of them)
            usernames = [v.get("username") for v in user_data_variants if v.get("username") is not None]
            if usernames:
                assert db_user.username in usernames or db_user.username is None, (
                    f"User has unexpected username: {db_user.username}. "
                    f"Expected one of: {usernames} or None"
                )
    
    @pytest.mark.asyncio
    async def test_property_high_concurrency(self):
        """
        Test with very high concurrency (20 simultaneous operations).
        
        This is a stress test to ensure UPSERT handles extreme concurrent load.
        """
        telegram_id = 3000000100
        num_concurrent = 20
        
        # Create 20 concurrent upsert operations
        user_data_list = [
            TelegramUserData(
                telegram_id=telegram_id,
                username=f"user_concurrent_{i}",
                first_name=f"First_{i}"
            )
            for i in range(num_concurrent)
        ]
        
        # Execute all concurrently
        tasks = [
            user_management_service.upsert_user(user_data)
            for user_data in user_data_list
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify no exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, (
            f"High concurrency test had {len(exceptions)} exceptions: {exceptions}"
        )
        
        # Verify only one record exists
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            count = result.scalar()
            
            assert count == 1, (
                f"High concurrency test FAILED: Expected 1 record after {num_concurrent} "
                f"concurrent operations, but found {count} records."
            )
    
    @pytest.mark.asyncio
    async def test_property_concurrent_with_delays(self):
        """
        Test concurrent operations with small random delays.
        
        This simulates real-world timing variations in concurrent requests.
        """
        telegram_id = 3000000101
        num_concurrent = 10
        
        async def upsert_with_delay(user_data, delay_ms):
            """Helper to add small delay before upsert"""
            await asyncio.sleep(delay_ms / 1000.0)
            return await user_management_service.upsert_user(user_data)
        
        # Create operations with varying delays (0-50ms)
        tasks = []
        for i in range(num_concurrent):
            user_data = TelegramUserData(
                telegram_id=telegram_id,
                username=f"user_delayed_{i}",
                first_name=f"First_{i}"
            )
            delay = (i * 5) % 50  # 0, 5, 10, ..., 45ms
            tasks.append(upsert_with_delay(user_data, delay))
        
        # Execute all concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify no exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Delayed concurrent test had {len(exceptions)} exceptions"
        
        # Verify only one record exists
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            count = result.scalar()
            
            assert count == 1, (
                f"Delayed concurrent test FAILED: Expected 1 record, found {count}"
            )
    
    @pytest.mark.asyncio
    async def test_property_summary(self):
        """
        Summary test that validates all concurrent tests succeeded.
        """
        async with async_session_maker() as session:
            # Count all test users from concurrent tests
            result = await session.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM telegram_users 
                    WHERE telegram_id >= 3000000001 AND telegram_id <= 3000000101
                """)
            )
            count = result.scalar()
            
            # Should have 8 parametrized tests + 2 additional tests = 10 users
            assert count == 10, (
                f"Expected 10 test users from concurrent creation tests, but found {count}. "
                f"Each concurrent test should result in exactly 1 user record."
            )
