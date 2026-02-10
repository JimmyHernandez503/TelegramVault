"""
Property-Based Test for UPSERT Conflict Handling

Feature: telegram-vault-enrichment-media-fixes
Property 25: UPSERT Conflict Handling

For any attempt to insert a user record with a duplicate telegram_id,
the system should update the existing record instead of failing.

Validates: Requirements 8.2
"""

import pytest
import asyncio
from sqlalchemy import text
from backend.app.db.database import async_session_maker
from backend.app.services.user_management_service import (
    user_management_service,
    TelegramUserData
)


class TestPropertyUpsertConflictHandling:
    """
    Property-Based Test: UPSERT Conflict Handling
    
    This test validates that for ANY attempt to insert a duplicate user,
    the system updates the existing record instead of raising an error.
    """
    
    # Generate test cases with various user data combinations
    test_cases = [
        # (telegram_id, initial_data, update_data)
        (
            2000000001,
            {"username": "user1", "first_name": "First1", "last_name": "Last1"},
            {"username": "user1_updated", "first_name": "First1_Updated", "last_name": "Last1_Updated"}
        ),
        (
            2000000002,
            {"username": "user2", "first_name": "First2", "last_name": None},
            {"username": "user2", "first_name": "First2", "last_name": "Last2_Added"}
        ),
        (
            2000000003,
            {"username": None, "first_name": "First3", "last_name": "Last3"},
            {"username": "user3_added", "first_name": "First3", "last_name": "Last3"}
        ),
        (
            2000000004,
            {"username": "user4", "first_name": None, "last_name": None},
            {"username": "user4", "first_name": "First4_Added", "last_name": "Last4_Added"}
        ),
        (
            2000000005,
            {"username": "user5", "first_name": "First5", "last_name": "Last5", "phone": None},
            {"username": "user5", "first_name": "First5", "last_name": "Last5", "phone": "+1234567890"}
        ),
        (
            2000000006,
            {"username": "user6", "first_name": "First6", "last_name": "Last6", "bio": None},
            {"username": "user6", "first_name": "First6", "last_name": "Last6", "bio": "Updated bio"}
        ),
        (
            2000000007,
            {"username": "user7", "first_name": "First7", "last_name": "Last7", "access_hash": None},
            {"username": "user7", "first_name": "First7", "last_name": "Last7", "access_hash": 123456789}
        ),
        (
            2000000008,
            {"username": "user8", "first_name": "First8", "last_name": "Last8", "is_bot": False},
            {"username": "user8", "first_name": "First8", "last_name": "Last8", "is_bot": True}
        ),
        (
            2000000009,
            {"username": "user9", "first_name": "First9", "last_name": "Last9", "is_premium": False},
            {"username": "user9", "first_name": "First9", "last_name": "Last9", "is_premium": True}
        ),
        (
            2000000010,
            {"username": "user10", "first_name": "First10", "last_name": "Last10"},
            {"username": "user10_v2", "first_name": "First10_v2", "last_name": "Last10_v2"}
        ),
    ]
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize("telegram_id,initial_data,update_data", test_cases)
    async def test_property_upsert_conflict_handling(
        self,
        telegram_id,
        initial_data,
        update_data
    ):
        """
        Property Test: For any attempt to insert a user with duplicate telegram_id,
        the system should update the existing record instead of failing.
        
        **Validates: Requirements 8.2**
        """
        # Arrange: Create initial user
        initial_user_data = TelegramUserData(
            telegram_id=telegram_id,
            username=initial_data.get("username"),
            first_name=initial_data.get("first_name"),
            last_name=initial_data.get("last_name"),
            phone=initial_data.get("phone"),
            access_hash=initial_data.get("access_hash"),
            bio=initial_data.get("bio"),
            is_bot=initial_data.get("is_bot", False),
            is_premium=initial_data.get("is_premium", False)
        )
        
        # Act 1: Create initial user
        initial_user = await user_management_service.upsert_user(initial_user_data)
        
        # Assert 1: Initial user should be created
        assert initial_user is not None, f"Initial user creation failed for telegram_id={telegram_id}"
        assert initial_user.telegram_id == telegram_id
        
        # Arrange: Create update data with same telegram_id (conflict)
        update_user_data = TelegramUserData(
            telegram_id=telegram_id,  # Same telegram_id - will cause conflict
            username=update_data.get("username"),
            first_name=update_data.get("first_name"),
            last_name=update_data.get("last_name"),
            phone=update_data.get("phone"),
            access_hash=update_data.get("access_hash"),
            bio=update_data.get("bio"),
            is_bot=update_data.get("is_bot", False),
            is_premium=update_data.get("is_premium", False)
        )
        
        # Act 2: Attempt to insert duplicate (should update instead)
        updated_user = await user_management_service.upsert_user(update_user_data)
        
        # Assert 2: Update should succeed (not raise error)
        assert updated_user is not None, (
            f"UPSERT failed for duplicate telegram_id={telegram_id}. "
            f"Expected update to succeed, but got None."
        )
        
        # Assert 3: Should still be the same user (same telegram_id)
        assert updated_user.telegram_id == telegram_id, (
            f"telegram_id changed after update: expected {telegram_id}, got {updated_user.telegram_id}"
        )
        
        # Assert 4: Verify only one record exists in database
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            count = result.scalar()
            assert count == 1, (
                f"Expected exactly 1 user with telegram_id={telegram_id}, but found {count}. "
                f"UPSERT should update existing record, not create duplicate."
            )
        
        # Assert 5: Verify updated values are present
        # Re-fetch the user to ensure database state is correct
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT * FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            db_user = result.fetchone()
            
            assert db_user is not None, f"User with telegram_id={telegram_id} not found in database"
            
            # Verify updated fields (COALESCE logic: new value if not None, else keep old)
            if update_data.get("username") is not None:
                assert db_user.username == update_data["username"], (
                    f"username not updated: expected {update_data['username']}, got {db_user.username}"
                )
            
            if update_data.get("first_name") is not None:
                assert db_user.first_name == update_data["first_name"], (
                    f"first_name not updated: expected {update_data['first_name']}, got {db_user.first_name}"
                )
            
            if update_data.get("last_name") is not None:
                assert db_user.last_name == update_data["last_name"], (
                    f"last_name not updated: expected {update_data['last_name']}, got {db_user.last_name}"
                )
    
    @pytest.mark.asyncio
    async def test_property_multiple_updates(self):
        """
        Test that multiple updates to the same user work correctly.
        
        This validates that UPSERT can be called multiple times on the same user
        without errors.
        """
        telegram_id = 2000000100
        
        # Create initial user
        user_data = TelegramUserData(
            telegram_id=telegram_id,
            username="initial",
            first_name="Initial"
        )
        user1 = await user_management_service.upsert_user(user_data)
        assert user1 is not None
        
        # Update 1
        user_data = TelegramUserData(
            telegram_id=telegram_id,
            username="update1",
            first_name="Update1"
        )
        user2 = await user_management_service.upsert_user(user_data)
        assert user2 is not None
        
        # Update 2
        user_data = TelegramUserData(
            telegram_id=telegram_id,
            username="update2",
            first_name="Update2"
        )
        user3 = await user_management_service.upsert_user(user_data)
        assert user3 is not None
        
        # Update 3
        user_data = TelegramUserData(
            telegram_id=telegram_id,
            username="update3",
            first_name="Update3"
        )
        user4 = await user_management_service.upsert_user(user_data)
        assert user4 is not None
        
        # Verify only one record exists
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            count = result.scalar()
            assert count == 1, f"Expected 1 user, found {count} after multiple updates"
            
            # Verify final values
            result = await session.execute(
                text("SELECT username, first_name FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            row = result.fetchone()
            assert row.username == "update3", f"Expected username 'update3', got {row.username}"
            assert row.first_name == "Update3", f"Expected first_name 'Update3', got {row.first_name}"
    
    @pytest.mark.asyncio
    async def test_property_summary(self):
        """
        Summary test that validates all UPSERT operations succeeded.
        """
        async with async_session_maker() as session:
            # Count test users (including the multiple updates test user)
            result = await session.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM telegram_users 
                    WHERE telegram_id >= 2000000001 AND telegram_id <= 2000000100
                """)
            )
            count = result.scalar()
            
            # Should have 10 test cases + 1 multiple updates test = 11 users
            assert count == 11, (
                f"Expected 11 test users from UPSERT conflict tests, but found {count}"
            )
