"""
User Management Service with UPSERT Implementation

This service provides robust user creation and update operations using proper
UPSERT patterns with ON CONFLICT DO UPDATE clauses to prevent UniqueViolationError
and handle concurrent user creation scenarios.

Requirements: 2.1, 2.2, 2.3
"""

import logging
from typing import List, Optional, Dict, Any, NamedTuple
from datetime import datetime
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from telethon.tl.types import User as TelegramUserEntity

from backend.app.models.telegram_user import TelegramUser
from backend.app.models.media import MediaFile
from backend.app.core.session_manager import session_manager
from backend.app.core.constraint_validator import constraint_validator
from backend.app.core.logging_config import get_logger

logger = get_logger("user_management_service")


class TelegramUserData(NamedTuple):
    """Data structure for Telegram user information"""
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    access_hash: Optional[int] = None
    is_bot: bool = False
    is_premium: bool = False
    is_verified: bool = False
    is_scam: bool = False
    is_fake: bool = False
    is_restricted: bool = False
    is_deleted: bool = False
    bio: Optional[str] = None


class MediaFileData(NamedTuple):
    """Data structure for media file information"""
    message_id: int
    file_type: str
    telegram_id: Optional[int] = None
    file_path: Optional[str] = None
    file_name: Optional[str] = None
    file_size: Optional[int] = None
    mime_type: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[int] = None
    file_hash: Optional[str] = None
    unique_id: Optional[str] = None
    perceptual_hash: Optional[str] = None
    validation_status: str = "pending"
    processing_status: str = "pending"
    processing_priority: int = 0
    download_attempts: int = 0
    download_error: Optional[str] = None
    download_error_category: Optional[str] = None
    is_duplicate: bool = False
    original_media_id: Optional[int] = None


class ConflictData(NamedTuple):
    """Information about a user conflict during UPSERT"""
    telegram_id: int
    existing_user: TelegramUser
    new_data: TelegramUserData
    conflict_type: str


class BatchUpsertResult(NamedTuple):
    """Result of batch user upsert operation"""
    successful_users: List[TelegramUser]
    failed_operations: List[Dict[str, Any]]
    total_processed: int
    success_count: int
    failure_count: int


class UserManagementService:
    """
    Enhanced user management service with proper UPSERT operations.
    
    Provides robust user creation and update functionality using ON CONFLICT DO UPDATE
    clauses to prevent constraint violations and handle concurrent operations gracefully.
    """
    
    def __init__(self):
        self.logger = logger
    
    async def upsert_user(self, user_data: TelegramUserData) -> Optional[TelegramUser]:
        """
        Creates or updates user using ON CONFLICT DO UPDATE.
        
        This method uses proper UPSERT operations to handle concurrent user creation
        without raising UniqueViolationError exceptions.
        
        Args:
            user_data: TelegramUserData with user information
            
        Returns:
            TelegramUser: The created or updated user, or None if operation failed
        """
        async def _upsert_operation(session: AsyncSession) -> TelegramUser:
            # Use raw SQL for proper UPSERT with ON CONFLICT DO UPDATE
            # CRITICAL FIX: Added messages_count, groups_count, media_count, attachments_count with default 0
            # CRITICAL FIX: Added has_stories with default FALSE (Task 1.2)
            # CRITICAL FIX: Added is_watchlist and is_favorite with default FALSE
            upsert_query = text("""
                INSERT INTO telegram_users (
                    telegram_id, username, first_name, last_name, phone, access_hash,
                    is_bot, is_premium, is_verified, is_scam, is_fake, is_restricted, 
                    is_deleted, has_stories, bio, messages_count, groups_count, media_count, attachments_count,
                    is_watchlist, is_favorite, created_at, updated_at
                ) VALUES (
                    :telegram_id, :username, :first_name, :last_name, :phone, :access_hash,
                    :is_bot, :is_premium, :is_verified, :is_scam, :is_fake, :is_restricted,
                    :is_deleted, FALSE, :bio, 0, 0, 0, 0, FALSE, FALSE, NOW(), NOW()
                )
                ON CONFLICT (telegram_id) DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, telegram_users.username),
                    first_name = COALESCE(EXCLUDED.first_name, telegram_users.first_name),
                    last_name = COALESCE(EXCLUDED.last_name, telegram_users.last_name),
                    phone = COALESCE(EXCLUDED.phone, telegram_users.phone),
                    access_hash = COALESCE(EXCLUDED.access_hash, telegram_users.access_hash),
                    is_bot = EXCLUDED.is_bot,
                    is_premium = EXCLUDED.is_premium,
                    is_verified = EXCLUDED.is_verified,
                    is_scam = EXCLUDED.is_scam,
                    is_fake = EXCLUDED.is_fake,
                    is_restricted = EXCLUDED.is_restricted,
                    is_deleted = EXCLUDED.is_deleted,
                    has_stories = EXCLUDED.has_stories,
                    bio = COALESCE(EXCLUDED.bio, telegram_users.bio),
                    updated_at = NOW()
                RETURNING *
            """)
            
            result = await session.execute(upsert_query, {
                'telegram_id': user_data.telegram_id,
                'username': user_data.username,
                'first_name': user_data.first_name,
                'last_name': user_data.last_name,
                'phone': user_data.phone,
                'access_hash': user_data.access_hash,
                'is_bot': user_data.is_bot,
                'is_premium': user_data.is_premium,
                'is_verified': user_data.is_verified,
                'is_scam': user_data.is_scam,
                'is_fake': user_data.is_fake,
                'is_restricted': user_data.is_restricted,
                'is_deleted': user_data.is_deleted,
                'bio': user_data.bio
            })
            
            row = result.fetchone()
            if not row:
                raise Exception("UPSERT operation did not return a user")
            
            # Convert row to TelegramUser object
            user = TelegramUser()
            for column, value in row._mapping.items():
                setattr(user, column, value)
            
            self.logger.debug(f"Successfully upserted user {user_data.telegram_id}")
            return user
        
        try:
            result = await session_manager.execute_with_retry(_upsert_operation)
            self.logger.info(f"User {user_data.telegram_id} upserted successfully")
            return result
                
        except IntegrityError as e:
            # Enhanced constraint violation logging for user operations
            self.logger.error(
                f"Database constraint violation during user upsert. "
                f"Telegram ID: {user_data.telegram_id}, "
                f"Username: {user_data.username}, "
                f"Constraint error: {e}, "
                f"Error details: {e.orig if hasattr(e, 'orig') else 'N/A'}, "
                f"User data: telegram_id={user_data.telegram_id}, username={user_data.username}, "
                f"first_name={user_data.first_name}, is_bot={user_data.is_bot}, "
                f"Action: Returning None"
            )
            return None
        except Exception as e:
            # Enhanced general error logging for user operations
            self.logger.error(
                f"Unexpected error during user upsert. "
                f"Telegram ID: {user_data.telegram_id}, "
                f"Username: {user_data.username}, "
                f"Error type: {type(e).__name__}, "
                f"Error: {e}, "
                f"User data summary: telegram_id={user_data.telegram_id}, "
                f"username={user_data.username}, is_bot={user_data.is_bot}"
            )
            return None
    
    async def batch_upsert_users(self, users: List[TelegramUserData]) -> BatchUpsertResult:
        """
        Batch upserts users with partial failure handling.
        
        Each user is processed independently, so failures in one user operation
        don't affect others in the batch. Includes constraint validation.
        
        Args:
            users: List of TelegramUserData objects
            
        Returns:
            BatchUpsertResult with success/failure statistics and results
        """
        # Validate constraints before attempting upsert
        user_records = []
        for user_data in users:
            user_records.append({
                'telegram_id': user_data.telegram_id,
                'username': user_data.username,
                'first_name': user_data.first_name,
                'last_name': user_data.last_name,
                'phone': user_data.phone
            })
        
        # Validate batch consistency
        validation_result = await constraint_validator.validate_batch_operation(
            'telegram_users', user_records
        )
        
        if not validation_result.is_valid:
            self.logger.warning(
                f"User batch validation found {len(validation_result.violations)} violations "
                f"and {len(validation_result.referential_violations)} referential violations"
            )
            
            # Log detailed violation information
            for violation in validation_result.violations:
                self.logger.warning(f"User constraint violation: {violation}")
            
            for ref_violation in validation_result.referential_violations:
                self.logger.warning(f"User referential integrity violation: {ref_violation}")
        
        successful_users = []
        failed_operations = []
        
        # Process each user individually to handle partial failures
        for i, user_data in enumerate(users):
            try:
                user = await self.upsert_user(user_data)
                if user:
                    successful_users.append(user)
                else:
                    failed_operations.append({
                        'user_data': user_data,
                        'error': 'Upsert operation returned None',
                        'index': i
                    })
            except Exception as e:
                failed_operations.append({
                    'user_data': user_data,
                    'error': str(e),
                    'index': i
                })
        
        success_count = len(successful_users)
        failure_count = len(failed_operations)
        
        self.logger.info(
            f"User batch upsert completed: {success_count} successful, "
            f"{failure_count} failed out of {len(users)} total. "
            f"Validation time: {validation_result.validation_time_ms:.2f}ms"
        )
        
        return BatchUpsertResult(
            successful_users=successful_users,
            failed_operations=failed_operations,
            total_processed=len(users),
            success_count=success_count,
            failure_count=failure_count
        )
    
    async def handle_user_conflict(self, conflict_data: ConflictData) -> TelegramUser:
        """
        Handles user constraint violations gracefully.
        
        This method provides additional conflict resolution strategies
        beyond the standard UPSERT operation.
        
        Args:
            conflict_data: ConflictData with conflict information
            
        Returns:
            TelegramUser: The resolved user
        """
        try:
            self.logger.info(
                f"Handling user conflict for telegram_id {conflict_data.telegram_id}, "
                f"type: {conflict_data.conflict_type}"
            )
            
            # For most conflicts, we can resolve by updating the existing user
            # with new data using our standard upsert operation
            updated_user = await self.upsert_user(conflict_data.new_data)
            
            if updated_user:
                self.logger.info(f"Successfully resolved conflict for user {conflict_data.telegram_id}")
                return updated_user
            else:
                # Fallback: return the existing user if update fails
                self.logger.warning(
                    f"Could not update user {conflict_data.telegram_id}, "
                    f"returning existing user"
                )
                return conflict_data.existing_user
                
        except Exception as e:
            self.logger.error(f"Error handling user conflict: {e}")
            # Return existing user as last resort
            return conflict_data.existing_user
    
    async def get_or_create_user(self, telegram_id: int, 
                               user_entity: Optional[TelegramUserEntity] = None) -> Optional[TelegramUser]:
        """
        Gets an existing user or creates a new one from Telegram entity.
        
        This is a convenience method that combines user lookup with creation
        using data from a Telegram user entity.
        
        Args:
            telegram_id: Telegram user ID
            user_entity: Optional Telegram user entity with additional data
            
        Returns:
            TelegramUser: The existing or newly created user
        """
        # First try to get existing user
        async def _get_user_operation(session: AsyncSession) -> Optional[TelegramUser]:
            result = await session.execute(
                select(TelegramUser).where(TelegramUser.telegram_id == telegram_id)
            )
            return result.scalar_one_or_none()
        
        try:
            existing_user = await session_manager.execute_with_retry(_get_user_operation)
            
            if existing_user:
                return existing_user
            
            # User doesn't exist, create new one
            user_data = self._create_user_data_from_entity(telegram_id, user_entity)
            return await self.upsert_user(user_data)
            
        except Exception as e:
            self.logger.error(f"Error in get_or_create_user for {telegram_id}: {e}")
            return None
    
    def _create_user_data_from_entity(self, telegram_id: int, 
                                    user_entity: Optional[TelegramUserEntity] = None) -> TelegramUserData:
        """
        Creates TelegramUserData from a Telegram user entity.
        
        Args:
            telegram_id: Telegram user ID
            user_entity: Optional Telegram user entity
            
        Returns:
            TelegramUserData: User data structure
        """
        if not user_entity:
            return TelegramUserData(telegram_id=telegram_id)
        
        return TelegramUserData(
            telegram_id=telegram_id,
            username=getattr(user_entity, 'username', None),
            first_name=getattr(user_entity, 'first_name', None),
            last_name=getattr(user_entity, 'last_name', None),
            phone=getattr(user_entity, 'phone', None),
            access_hash=getattr(user_entity, 'access_hash', None),
            is_bot=getattr(user_entity, 'bot', False),
            is_premium=getattr(user_entity, 'premium', False),
            is_verified=getattr(user_entity, 'verified', False),
            is_scam=getattr(user_entity, 'scam', False),
            is_fake=getattr(user_entity, 'fake', False),
            is_restricted=getattr(user_entity, 'restricted', False),
            is_deleted=getattr(user_entity, 'deleted', False)
        )
    
    async def update_user_message_count(self, user_id: int, increment: int = 1) -> bool:
        """
        Updates user message count atomically.
        
        Args:
            user_id: Database user ID (not telegram_id)
            increment: Amount to increment message count by
            
        Returns:
            bool: True if update succeeded
        """
        async def _update_count_operation(session: AsyncSession) -> bool:
            update_query = text("""
                UPDATE telegram_users 
                SET messages_count = messages_count + :increment,
                    updated_at = NOW()
                WHERE id = :user_id
            """)
            
            result = await session.execute(update_query, {
                'increment': increment,
                'user_id': user_id
            })
            
            return result.rowcount > 0
        
        try:
            result = await session_manager.execute_with_retry(_update_count_operation)
            return result is not None
        except Exception as e:
            self.logger.error(f"Error updating message count for user {user_id}: {e}")
            return False
    
    async def upsert_media(self, media_data: MediaFileData) -> Optional[MediaFile]:
        """
        Creates or updates media file using ON CONFLICT DO UPDATE.
        
        This method uses proper UPSERT operations to handle concurrent media creation
        without raising UniqueViolationError exceptions. Uses message_id as the
        conflict resolution key.
        
        Args:
            media_data: MediaFileData with media file information
            
        Returns:
            MediaFile: The created or updated media file, or None if operation failed
        """
        async def _upsert_operation(session: AsyncSession) -> MediaFile:
            # Use raw SQL for proper UPSERT with ON CONFLICT DO UPDATE
            # Using message_id as the unique constraint for conflict resolution
            upsert_query = text("""
                INSERT INTO media_files (
                    message_id, file_type, telegram_id, file_path, file_name, 
                    file_size, mime_type, width, height, duration,
                    file_hash, unique_id, perceptual_hash,
                    validation_status, processing_status, processing_priority,
                    download_attempts, download_error, download_error_category,
                    is_duplicate, original_media_id,
                    last_download_attempt, created_at, updated_at
                ) VALUES (
                    :message_id, :file_type, :telegram_id, :file_path, :file_name,
                    :file_size, :mime_type, :width, :height, :duration,
                    :file_hash, :unique_id, :perceptual_hash,
                    :validation_status, :processing_status, :processing_priority,
                    :download_attempts, :download_error, :download_error_category,
                    :is_duplicate, :original_media_id,
                    NOW(), NOW(), NOW()
                )
                ON CONFLICT (message_id) DO UPDATE SET
                    file_path = COALESCE(EXCLUDED.file_path, media_files.file_path),
                    file_name = COALESCE(EXCLUDED.file_name, media_files.file_name),
                    file_size = COALESCE(EXCLUDED.file_size, media_files.file_size),
                    file_hash = COALESCE(EXCLUDED.file_hash, media_files.file_hash),
                    unique_id = COALESCE(EXCLUDED.unique_id, media_files.unique_id),
                    perceptual_hash = COALESCE(EXCLUDED.perceptual_hash, media_files.perceptual_hash),
                    validation_status = EXCLUDED.validation_status,
                    processing_status = EXCLUDED.processing_status,
                    download_attempts = media_files.download_attempts + 1,
                    last_download_attempt = NOW(),
                    download_error = EXCLUDED.download_error,
                    download_error_category = EXCLUDED.download_error_category,
                    is_duplicate = EXCLUDED.is_duplicate,
                    original_media_id = COALESCE(EXCLUDED.original_media_id, media_files.original_media_id),
                    updated_at = NOW()
                RETURNING *
            """)
            
            result = await session.execute(upsert_query, {
                'message_id': media_data.message_id,
                'file_type': media_data.file_type,
                'telegram_id': media_data.telegram_id,
                'file_path': media_data.file_path,
                'file_name': media_data.file_name,
                'file_size': media_data.file_size,
                'mime_type': media_data.mime_type,
                'width': media_data.width,
                'height': media_data.height,
                'duration': media_data.duration,
                'file_hash': media_data.file_hash,
                'unique_id': media_data.unique_id,
                'perceptual_hash': media_data.perceptual_hash,
                'validation_status': media_data.validation_status,
                'processing_status': media_data.processing_status,
                'processing_priority': media_data.processing_priority,
                'download_attempts': media_data.download_attempts,
                'download_error': media_data.download_error,
                'download_error_category': media_data.download_error_category,
                'is_duplicate': media_data.is_duplicate,
                'original_media_id': media_data.original_media_id
            })
            
            row = result.fetchone()
            if not row:
                raise Exception("UPSERT operation did not return a media file")
            
            # Convert row to MediaFile object
            media = MediaFile()
            for column, value in row._mapping.items():
                setattr(media, column, value)
            
            self.logger.debug(f"Successfully upserted media for message {media_data.message_id}")
            return media
        
        try:
            result = await session_manager.execute_with_retry(_upsert_operation)
            self.logger.info(f"Media for message {media_data.message_id} upserted successfully")
            return result
                
        except IntegrityError as e:
            # Enhanced constraint violation logging for media operations
            self.logger.error(
                f"Database constraint violation during media upsert. "
                f"Message ID: {media_data.message_id}, "
                f"File type: {media_data.file_type}, "
                f"Constraint error: {e}, "
                f"Error details: {e.orig if hasattr(e, 'orig') else 'N/A'}, "
                f"Media data: message_id={media_data.message_id}, file_type={media_data.file_type}, "
                f"file_path={media_data.file_path}, "
                f"Action: Returning None"
            )
            return None
        except Exception as e:
            # Enhanced general error logging for media operations
            self.logger.error(
                f"Unexpected error during media upsert. "
                f"Message ID: {media_data.message_id}, "
                f"File type: {media_data.file_type}, "
                f"Error type: {type(e).__name__}, "
                f"Error: {e}, "
                f"Media data summary: message_id={media_data.message_id}, "
                f"file_type={media_data.file_type}, file_path={media_data.file_path}"
            )
            return None


# Global instance
user_management_service = UserManagementService()