"""
Tests for Database Schema Fixes

This module tests the fixes for database schema constraints, specifically
the has_stories column default value and user insertion with missing fields.

Validates: Requirements 1.1, 1.2, 1.4
"""

import pytest
from sqlalchemy import text
from backend.app.db.database import async_session_maker
from backend.app.services.user_management_service import (
    user_management_service,
    TelegramUserData
)


class TestDatabaseSchemaFixes:
    """Tests for database schema constraint fixes"""
    
    @pytest.mark.asyncio
    async def test_has_stories_has_default_value(self):
        """
        Test that has_stories column has a default value in the database schema.
        Validates: Requirements 1.1
        """
        async with async_session_maker() as session:
            # Query the database schema to check if has_stories has a default value
            result = await session.execute(text("""
                SELECT column_name, column_default, is_nullable, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'telegram_users' 
                AND column_name = 'has_stories'
            """))
            
            column_info = result.fetchone()
            
            # Assert column exists
            assert column_info is not None, "has_stories column should exist"
            
            col_name, col_default, is_nullable, data_type = column_info
            
            # Assert column has correct properties
            assert col_name == 'has_stories'
            assert data_type == 'boolean'
            assert is_nullable == 'NO', "has_stories should be NOT NULL"
            assert col_default is not None, "has_stories should have a default value"
            assert 'false' in col_default.lower(), "has_stories default should be FALSE"
    
    @pytest.mark.asyncio
    async def test_user_insertion_without_has_stories(self):
        """
        Test that inserting a user without has_stories field succeeds.
        The default value should be applied automatically.
        Validates: Requirements 1.1, 1.2
        """
        # Create user data without has_stories
        user_data = TelegramUserData(
            telegram_id=999888777,  # Use a unique ID for testing
            username='test_user_no_stories',
            first_name='Test',
            last_name='User'
        )
        
        # Insert user
        user = await user_management_service.upsert_user(user_data)
        
        # Assert user was created successfully
        assert user is not None, "User should be created successfully"
        assert user.telegram_id == 999888777
        assert user.username == 'test_user_no_stories'
        assert user.has_stories is False, "has_stories should default to False"
    
    @pytest.mark.asyncio
    async def test_user_insertion_with_minimal_data(self):
        """
        Test that inserting a user with only telegram_id succeeds.
        All optional fields should be handled gracefully.
        Validates: Requirements 1.2, 1.4
        """
        # Create user data with only telegram_id
        user_data = TelegramUserData(
            telegram_id=888777666
        )
        
        # Insert user
        user = await user_management_service.upsert_user(user_data)
        
        # Assert user was created successfully
        assert user is not None, "User should be created with minimal data"
        assert user.telegram_id == 888777666
        assert user.has_stories is False, "has_stories should default to False"
        assert user.username is None, "username should be None when not provided"
        assert user.first_name is None, "first_name should be None when not provided"
        assert user.last_name is None, "last_name should be None when not provided"
    
    @pytest.mark.asyncio
    async def test_user_insertion_with_all_optional_fields_none(self):
        """
        Test that inserting a user with all optional fields explicitly set to None succeeds.
        Validates: Requirements 1.2, 1.4
        """
        # Create user data with all optional fields as None
        user_data = TelegramUserData(
            telegram_id=777666555,
            username=None,
            first_name=None,
            last_name=None,
            phone=None,
            access_hash=None,
            bio=None
        )
        
        # Insert user
        user = await user_management_service.upsert_user(user_data)
        
        # Assert user was created successfully
        assert user is not None, "User should be created with None values"
        assert user.telegram_id == 777666555
        assert user.has_stories is False, "has_stories should default to False"
        assert user.username is None
        assert user.first_name is None
        assert user.last_name is None
        assert user.phone is None
        assert user.bio is None
    
    @pytest.mark.asyncio
    async def test_no_null_values_in_has_stories_column(self):
        """
        Test that there are no NULL values in the has_stories column.
        Validates: Requirements 1.1
        """
        async with async_session_maker() as session:
            # Check for NULL values in has_stories
            result = await session.execute(text("""
                SELECT COUNT(*) 
                FROM telegram_users 
                WHERE has_stories IS NULL
            """))
            
            null_count = result.scalar()
            
            # Assert no NULL values exist
            assert null_count == 0, f"Found {null_count} NULL values in has_stories column"
    
    @pytest.mark.asyncio
    async def test_user_update_preserves_has_stories(self):
        """
        Test that updating a user preserves the has_stories value.
        Validates: Requirements 1.1, 1.2
        """
        # Create initial user
        user_data = TelegramUserData(
            telegram_id=666555444,
            username='test_update_user',
            first_name='Initial'
        )
        
        user = await user_management_service.upsert_user(user_data)
        assert user is not None
        assert user.has_stories is False
        
        # Update user with new data
        updated_data = TelegramUserData(
            telegram_id=666555444,  # Same telegram_id
            username='test_update_user',
            first_name='Updated',
            last_name='Name'
        )
        
        updated_user = await user_management_service.upsert_user(updated_data)
        
        # Assert user was updated and has_stories is still False
        assert updated_user is not None
        assert updated_user.telegram_id == 666555444
        assert updated_user.first_name == 'Updated'
        assert updated_user.last_name == 'Name'
        assert updated_user.has_stories is False, "has_stories should be preserved"
    
    @pytest.mark.asyncio
    async def test_batch_user_insertion_with_missing_fields(self):
        """
        Test that batch inserting users with missing optional fields succeeds.
        Validates: Requirements 1.2, 1.4
        """
        # Create batch of users with varying levels of data completeness
        users = [
            TelegramUserData(telegram_id=555444333, username='user1'),
            TelegramUserData(telegram_id=444333222, first_name='User2'),
            TelegramUserData(telegram_id=333222111),  # Only telegram_id
            TelegramUserData(
                telegram_id=222111000,
                username='user4',
                first_name='User',
                last_name='Four'
            )
        ]
        
        # Batch insert users
        result = await user_management_service.batch_upsert_users(users)
        
        # Assert all users were created successfully
        assert result.success_count == 4, f"Expected 4 successful, got {result.success_count}"
        assert result.failure_count == 0, f"Expected 0 failures, got {result.failure_count}"
        assert len(result.successful_users) == 4
        
        # Verify all users have has_stories set to False
        for user in result.successful_users:
            assert user.has_stories is False, f"User {user.telegram_id} should have has_stories=False"
