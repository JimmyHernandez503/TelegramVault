"""
Property-Based Test for Incomplete User Data Handling

Feature: telegram-vault-enrichment-media-fixes
Property 2: Incomplete User Data Handling

For any incomplete user data (missing optional fields like last_name, phone, bio),
the system should successfully create a user record with the available data.

Validates: Requirements 1.2, 1.4
"""

import pytest
from sqlalchemy import text
from backend.app.db.database import async_session_maker
from backend.app.services.user_management_service import (
    user_management_service,
    TelegramUserData
)


class TestPropertyIncompleteUserDataHandling:
    """
    Property-Based Test: Incomplete User Data Handling
    
    This test validates that for ANY combination of optional fields (None or provided),
    the system successfully creates a user record with the available data.
    """
    
    # Generate test cases covering various combinations of optional fields
    # This simulates property-based testing by testing many different input combinations
    test_cases = [
        # (telegram_id, username, first_name, last_name, phone, access_hash, bio, is_bot, is_premium)
        (1000000001, None, None, None, None, None, None, False, False),
        (1000000002, "user1", None, None, None, None, None, False, False),
        (1000000003, None, "First", None, None, None, None, False, False),
        (1000000004, None, None, "Last", None, None, None, False, False),
        (1000000005, None, None, None, "+1234567890", None, None, False, False),
        (1000000006, None, None, None, None, 123456789, None, False, False),
        (1000000007, None, None, None, None, None, "Bio text", False, False),
        (1000000008, "user2", "First", None, None, None, None, False, False),
        (1000000009, "user3", None, "Last", None, None, None, False, False),
        (1000000010, "user4", None, None, "+9876543210", None, None, False, False),
        (1000000011, None, "First", "Last", None, None, None, False, False),
        (1000000012, None, "First", None, "+1111111111", None, None, False, False),
        (1000000013, None, None, "Last", "+2222222222", None, None, False, False),
        (1000000014, "user5", "First", "Last", None, None, None, False, False),
        (1000000015, "user6", "First", None, "+3333333333", None, None, False, False),
        (1000000016, "user7", None, "Last", "+4444444444", None, None, False, False),
        (1000000017, "user8", "First", "Last", "+5555555555", None, None, False, False),
        (1000000018, "user9", "First", "Last", None, 987654321, None, False, False),
        (1000000019, "user10", "First", "Last", "+6666666666", 111222333, None, False, False),
        (1000000020, "user11", "First", "Last", "+7777777777", 444555666, "Full bio", False, False),
        # Test with bots
        (1000000021, "bot1", "Bot", "Name", None, None, None, True, False),
        (1000000022, None, None, None, None, None, None, True, False),
        # Test with premium users
        (1000000023, "premium1", "Premium", "User", None, None, None, False, True),
        (1000000024, None, "Premium", None, None, None, None, False, True),
        # Test with empty strings (should be treated as None)
        (1000000025, "", "", "", "", None, "", False, False),
        # Test with only telegram_id (absolute minimum)
        (1000000026, None, None, None, None, None, None, False, False),
        (1000000027, None, None, None, None, None, None, False, False),
        (1000000028, None, None, None, None, None, None, False, False),
        (1000000029, None, None, None, None, None, None, False, False),
        (1000000030, None, None, None, None, None, None, False, False),
    ]
    
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "telegram_id,username,first_name,last_name,phone,access_hash,bio,is_bot,is_premium",
        test_cases
    )
    async def test_property_incomplete_user_data_handling(
        self,
        telegram_id,
        username,
        first_name,
        last_name,
        phone,
        access_hash,
        bio,
        is_bot,
        is_premium
    ):
        """
        Property Test: For any incomplete user data (missing optional fields),
        the system should successfully create a user record with available data.
        
        **Validates: Requirements 1.2, 1.4**
        
        This test validates the property across 30 different combinations of
        optional fields, ensuring the system handles all cases correctly.
        """
        # Arrange: Create user data with the given combination of fields
        # Convert empty strings to None
        username = None if username == "" else username
        first_name = None if first_name == "" else first_name
        last_name = None if last_name == "" else last_name
        phone = None if phone == "" else phone
        bio = None if bio == "" else bio
        
        user_data = TelegramUserData(
            telegram_id=telegram_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            phone=phone,
            access_hash=access_hash,
            bio=bio,
            is_bot=is_bot,
            is_premium=is_premium
        )
        
        # Act: Attempt to create the user
        user = await user_management_service.upsert_user(user_data)
        
        # Assert: User should be created successfully regardless of which fields are None
        assert user is not None, (
            f"User creation failed for telegram_id={telegram_id} with "
            f"username={username}, first_name={first_name}, last_name={last_name}, "
            f"phone={phone}, access_hash={access_hash}, bio={bio}"
        )
        
        # Verify the user has the correct telegram_id
        assert user.telegram_id == telegram_id, (
            f"telegram_id mismatch: expected {telegram_id}, got {user.telegram_id}"
        )
        
        # Verify default values are applied for NOT NULL columns
        assert user.has_stories is False, "has_stories should default to False"
        assert user.is_watchlist is False, "is_watchlist should default to False"
        assert user.is_favorite is False, "is_favorite should default to False"
        assert user.messages_count == 0, "messages_count should default to 0"
        assert user.groups_count == 0, "groups_count should default to 0"
        assert user.media_count == 0, "media_count should default to 0"
        assert user.attachments_count == 0, "attachments_count should default to 0"
        
        # Verify provided values are stored correctly
        if username is not None:
            assert user.username == username, f"username mismatch: expected {username}, got {user.username}"
        if first_name is not None:
            assert user.first_name == first_name, f"first_name mismatch: expected {first_name}, got {user.first_name}"
        if last_name is not None:
            assert user.last_name == last_name, f"last_name mismatch: expected {last_name}, got {user.last_name}"
        if phone is not None:
            assert user.phone == phone, f"phone mismatch: expected {phone}, got {user.phone}"
        if access_hash is not None:
            assert user.access_hash == access_hash, f"access_hash mismatch: expected {access_hash}, got {user.access_hash}"
        if bio is not None:
            assert user.bio == bio, f"bio mismatch: expected {bio}, got {user.bio}"
        
        assert user.is_bot == is_bot, f"is_bot mismatch: expected {is_bot}, got {user.is_bot}"
        assert user.is_premium == is_premium, f"is_premium mismatch: expected {is_premium}, got {user.is_premium}"
        
        # Verify the user exists in the database
        async with async_session_maker() as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM telegram_users WHERE telegram_id = :tid"),
                {"tid": telegram_id}
            )
            count = result.scalar()
            assert count == 1, f"User with telegram_id={telegram_id} should exist in database"
    
    @pytest.mark.asyncio
    async def test_property_summary(self):
        """
        Summary test that validates the property holds across all test cases.
        
        This test runs after all parametrized tests and verifies that all
        test users were created successfully.
        """
        async with async_session_maker() as session:
            # Count how many test users were created
            result = await session.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM telegram_users 
                    WHERE telegram_id >= 1000000001 AND telegram_id <= 1000000030
                """)
            )
            count = result.scalar()
            
            # All 30 test cases should have created users
            assert count == 30, (
                f"Expected 30 test users to be created, but found {count}. "
                f"This indicates that some user creations failed."
            )
            
            # Verify all have correct default values
            result = await session.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM telegram_users 
                    WHERE telegram_id >= 1000000001 AND telegram_id <= 1000000030
                    AND has_stories = FALSE
                    AND is_watchlist = FALSE
                    AND is_favorite = FALSE
                    AND messages_count = 0
                    AND groups_count = 0
                    AND media_count = 0
                    AND attachments_count = 0
                """)
            )
            count_with_defaults = result.scalar()
            
            assert count_with_defaults == 30, (
                f"Expected all 30 users to have correct default values, "
                f"but only {count_with_defaults} do."
            )
