"""
Message Ingestion Service with UPSERT Implementation

This service provides robust message insertion and update operations using proper
UPSERT patterns with ON CONFLICT clauses to prevent constraint violations
and handle duplicate message processing. It also includes comprehensive entity
resolution error handling.

Requirements: 3.1, 3.3, 3.4, 6.1, 6.2, 6.3, 6.4
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any, NamedTuple, Set
from datetime import datetime, timedelta
from sqlalchemy import text, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from telethon.tl.types import Message as TelegramMessageEntity
from telethon.errors import PeerIdInvalidError, ChannelPrivateError, ChatAdminRequiredError

from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_group import TelegramGroup
from backend.app.core.session_manager import session_manager
from backend.app.core.constraint_validator import constraint_validator
from backend.app.core.api_rate_limiter import APIRateLimiter, OperationType
from backend.app.core.logging_config import get_logger

logger = get_logger("message_ingestion_service")


class TelegramMessageData(NamedTuple):
    """Data structure for Telegram message information"""
    telegram_id: int
    group_id: int
    sender_id: Optional[int] = None
    text: Optional[str] = None
    message_type: str = "text"
    date: Optional[datetime] = None
    edit_date: Optional[datetime] = None
    reply_to_msg_id: Optional[int] = None
    reply_preview: Optional[str] = None
    forward_from_id: Optional[int] = None
    forward_from_name: Optional[str] = None
    forward_date: Optional[datetime] = None
    views: Optional[int] = None
    forwards: Optional[int] = None
    mentions: Optional[dict] = None
    reactions: Optional[dict] = None
    is_pinned: bool = False
    is_deleted: bool = False
    grouped_id: Optional[int] = None
    content_hash: Optional[str] = None


class EntityResolutionStrategy(NamedTuple):
    """Strategy for entity resolution retry"""
    name: str
    max_attempts: int
    delay_seconds: float
    description: str


class ChannelUnavailabilityInfo(NamedTuple):
    """Information about channel unavailability"""
    channel_id: int
    telegram_id: int
    failure_count: int
    first_failure: datetime
    last_failure: datetime
    is_marked_unavailable: bool
    reason: str


class BatchInsertResult(NamedTuple):
    """Result of batch message insertion operation"""
    successful_messages: List[TelegramMessage]
    failed_operations: List[Dict[str, Any]]
    total_processed: int
    success_count: int
    failure_count: int
    duplicate_count: int


class EntityResolutionResult(NamedTuple):
    """Result of entity resolution attempt"""
    success: bool
    entity: Optional[Any]
    error: Optional[str]
    should_retry: bool
    should_mark_unavailable: bool


class MessageIngestionService:
    """
    Enhanced message ingestion service with proper UPSERT operations and entity resolution.
    
    Provides robust message insertion functionality using ON CONFLICT clauses
    to handle duplicate messages and constraint violations gracefully. Includes
    comprehensive entity resolution error handling.
    """
    
    # Entity resolution strategies
    RESOLUTION_STRATEGIES = [
        EntityResolutionStrategy("direct", 1, 0.0, "Direct entity resolution"),
        EntityResolutionStrategy("by_username", 2, 1.0, "Resolution by username"),
        EntityResolutionStrategy("by_phone", 2, 2.0, "Resolution by phone number"),
        EntityResolutionStrategy("cache_refresh", 1, 5.0, "Entity cache refresh")
    ]
    
    # Channel unavailability tracking
    UNAVAILABLE_THRESHOLD = 5  # Mark unavailable after 5 consecutive failures
    UNAVAILABLE_TIMEOUT = timedelta(hours=24)  # Check again after 24 hours
    
    def __init__(self):
        self.logger = logger
        self._unavailable_channels: Dict[int, ChannelUnavailabilityInfo] = {}
        self._entity_cache: Dict[int, Any] = {}
        self._cache_timestamps: Dict[int, datetime] = {}
        self._cache_ttl = timedelta(hours=1)  # Cache entities for 1 hour
        
        # Enhanced components
        self.rate_limiter = APIRateLimiter()
    
    async def upsert_message(self, message_data: TelegramMessageData) -> Optional[TelegramMessage]:
        """
        Inserts message using ON CONFLICT DO NOTHING for duplicate prevention.
        
        This method uses proper UPSERT operations to handle duplicate messages
        without raising constraint violation exceptions.
        
        Args:
            message_data: TelegramMessageData with message information
            
        Returns:
            TelegramMessage: The inserted message, or None if it was a duplicate
        """
        async def _upsert_operation(session: AsyncSession) -> Optional[TelegramMessage]:
            # Use raw SQL for proper UPSERT with ON CONFLICT DO NOTHING
            upsert_query = text("""
                INSERT INTO telegram_messages (
                    telegram_id, group_id, sender_id, text, message_type, date, edit_date,
                    reply_to_msg_id, reply_preview, forward_from_id, forward_from_name, 
                    forward_date, views, forwards, mentions, reactions, is_pinned, 
                    is_deleted, grouped_id, content_hash, created_at, updated_at
                ) VALUES (
                    :telegram_id, :group_id, :sender_id, :text, :message_type, :date, :edit_date,
                    :reply_to_msg_id, :reply_preview, :forward_from_id, :forward_from_name,
                    :forward_date, :views, :forwards, :mentions, :reactions, :is_pinned,
                    :is_deleted, :grouped_id, :content_hash, NOW(), NOW()
                )
                ON CONFLICT (telegram_id, group_id) DO NOTHING
                RETURNING *
            """)
            
            result = await session.execute(upsert_query, {
                'telegram_id': message_data.telegram_id,
                'group_id': message_data.group_id,
                'sender_id': message_data.sender_id,
                'text': message_data.text,
                'message_type': message_data.message_type,
                'date': message_data.date or datetime.utcnow(),
                'edit_date': message_data.edit_date,
                'reply_to_msg_id': message_data.reply_to_msg_id,
                'reply_preview': message_data.reply_preview,
                'forward_from_id': message_data.forward_from_id,
                'forward_from_name': message_data.forward_from_name,
                'forward_date': message_data.forward_date,
                'views': message_data.views,
                'forwards': message_data.forwards,
                'mentions': message_data.mentions,
                'reactions': message_data.reactions,
                'is_pinned': message_data.is_pinned,
                'is_deleted': message_data.is_deleted,
                'grouped_id': message_data.grouped_id,
                'content_hash': message_data.content_hash
            })
            
            row = result.fetchone()
            if not row:
                # Message was a duplicate (ON CONFLICT DO NOTHING triggered)
                self.logger.debug(f"Message {message_data.telegram_id} in group {message_data.group_id} already exists")
                return None
            
            # Convert row to TelegramMessage object
            message = TelegramMessage()
            for column, value in row._mapping.items():
                setattr(message, column, value)
            
            self.logger.debug(f"Successfully inserted message {message_data.telegram_id}")
            return message
        
        try:
            result = await session_manager.execute_with_retry(_upsert_operation)
            
            if result:
                self.logger.info(f"Message {message_data.telegram_id} in group {message_data.group_id} inserted successfully")
            
            return result
                
        except IntegrityError as e:
            # Enhanced constraint violation logging
            self.logger.error(
                f"Database constraint violation during message upsert. "
                f"Message ID: {message_data.telegram_id}, "
                f"Group ID: {message_data.group_id}, "
                f"Constraint error: {e}, "
                f"Error details: {e.orig if hasattr(e, 'orig') else 'N/A'}, "
                f"Message data: telegram_id={message_data.telegram_id}, group_id={message_data.group_id}, "
                f"sender_id={message_data.sender_id}, text_length={len(message_data.text or '')}, "
                f"Action: Returning None"
            )
            return None
        except Exception as e:
            # Enhanced general error logging
            self.logger.error(
                f"Unexpected error during message upsert. "
                f"Message ID: {message_data.telegram_id}, "
                f"Group ID: {message_data.group_id}, "
                f"Error type: {type(e).__name__}, "
                f"Error: {e}, "
                f"Message data summary: telegram_id={message_data.telegram_id}, "
                f"group_id={message_data.group_id}, sender_id={message_data.sender_id}"
            )
            return None
    
    async def upsert_message_with_update(self, message_data: TelegramMessageData, 
                                       update_fields: List[str] = None) -> Optional[TelegramMessage]:
        """
        Inserts message using ON CONFLICT DO UPDATE for message updates.
        
        This method supports updating existing messages when needed, such as
        for edit operations or view count updates.
        
        Args:
            message_data: TelegramMessageData with message information
            update_fields: List of fields to update on conflict (default: text, edit_date, views, forwards)
            
        Returns:
            TelegramMessage: The inserted or updated message
        """
        if update_fields is None:
            update_fields = ['text', 'edit_date', 'views', 'forwards', 'reactions']
        
        # Build the UPDATE SET clause dynamically
        update_clauses = []
        for field in update_fields:
            if field in ['text', 'edit_date', 'views', 'forwards', 'reactions']:
                update_clauses.append(f"{field} = EXCLUDED.{field}")
        
        update_clauses.append("updated_at = NOW()")
        update_set = ", ".join(update_clauses)
        
        async def _upsert_with_update_operation(session: AsyncSession) -> Optional[TelegramMessage]:
            upsert_query = text(f"""
                INSERT INTO telegram_messages (
                    telegram_id, group_id, sender_id, text, message_type, date, edit_date,
                    reply_to_msg_id, reply_preview, forward_from_id, forward_from_name, 
                    forward_date, views, forwards, mentions, reactions, is_pinned, 
                    is_deleted, grouped_id, content_hash, created_at, updated_at
                ) VALUES (
                    :telegram_id, :group_id, :sender_id, :text, :message_type, :date, :edit_date,
                    :reply_to_msg_id, :reply_preview, :forward_from_id, :forward_from_name,
                    :forward_date, :views, :forwards, :mentions, :reactions, :is_pinned,
                    :is_deleted, :grouped_id, :content_hash, NOW(), NOW()
                )
                ON CONFLICT (telegram_id, group_id) DO UPDATE SET
                    {update_set}
                RETURNING *
            """)
            
            result = await session.execute(upsert_query, {
                'telegram_id': message_data.telegram_id,
                'group_id': message_data.group_id,
                'sender_id': message_data.sender_id,
                'text': message_data.text,
                'message_type': message_data.message_type,
                'date': message_data.date or datetime.utcnow(),
                'edit_date': message_data.edit_date,
                'reply_to_msg_id': message_data.reply_to_msg_id,
                'reply_preview': message_data.reply_preview,
                'forward_from_id': message_data.forward_from_id,
                'forward_from_name': message_data.forward_from_name,
                'forward_date': message_data.forward_date,
                'views': message_data.views,
                'forwards': message_data.forwards,
                'mentions': message_data.mentions,
                'reactions': message_data.reactions,
                'is_pinned': message_data.is_pinned,
                'is_deleted': message_data.is_deleted,
                'grouped_id': message_data.grouped_id,
                'content_hash': message_data.content_hash
            })
            
            row = result.fetchone()
            if not row:
                raise Exception("UPSERT operation did not return a message")
            
            # Convert row to TelegramMessage object
            message = TelegramMessage()
            for column, value in row._mapping.items():
                setattr(message, column, value)
            
            self.logger.debug(f"Successfully upserted message {message_data.telegram_id} with updates")
            return message
        
        try:
            result = await session_manager.execute_with_retry(_upsert_with_update_operation)
            self.logger.info(f"Message {message_data.telegram_id} in group {message_data.group_id} upserted with updates")
            return result
                
        except IntegrityError as e:
            # Enhanced constraint violation logging for update operations
            self.logger.error(
                f"Database constraint violation during message upsert with update. "
                f"Message ID: {message_data.telegram_id}, "
                f"Group ID: {message_data.group_id}, "
                f"Update fields: {update_fields}, "
                f"Constraint error: {e}, "
                f"Error details: {e.orig if hasattr(e, 'orig') else 'N/A'}, "
                f"Action: Returning None"
            )
            return None
        except Exception as e:
            # Enhanced general error logging for update operations
            self.logger.error(
                f"Error during message upsert with update. "
                f"Message ID: {message_data.telegram_id}, "
                f"Group ID: {message_data.group_id}, "
                f"Update fields: {update_fields}, "
                f"Error type: {type(e).__name__}, "
                f"Error: {e}"
            )
            return None
    
    async def batch_insert_messages(self, messages: List[TelegramMessageData]) -> BatchInsertResult:
        """
        Batch inserts messages with partial failure handling.
        
        Each message is processed independently, so failures in one message
        don't affect others in the batch. Duplicates are handled gracefully.
        Includes constraint validation before insertion.
        
        Args:
            messages: List of TelegramMessageData objects
            
        Returns:
            BatchInsertResult with success/failure statistics and results
        """
        # Validate constraints before attempting insertion
        message_records = []
        for msg_data in messages:
            message_records.append({
                'telegram_id': msg_data.telegram_id,
                'group_id': msg_data.group_id,
                'sender_id': msg_data.sender_id,
                'text': msg_data.text,
                'message_type': msg_data.message_type,
                'date': msg_data.date
            })
        
        # Validate batch consistency
        validation_result = await constraint_validator.validate_batch_operation(
            'telegram_messages', message_records
        )
        
        if not validation_result.is_valid:
            self.logger.warning(
                f"Batch validation found {len(validation_result.violations)} violations "
                f"and {len(validation_result.referential_violations)} referential violations"
            )
            
            # Log detailed violation information
            for violation in validation_result.violations:
                self.logger.warning(f"Constraint violation: {violation}")
            
            for ref_violation in validation_result.referential_violations:
                self.logger.warning(f"Referential integrity violation: {ref_violation}")
        
        successful_messages = []
        failed_operations = []
        duplicate_count = 0
        
        # Process each message individually to handle partial failures
        for i, message_data in enumerate(messages):
            try:
                message = await self.upsert_message(message_data)
                if message:
                    successful_messages.append(message)
                else:
                    # Message was a duplicate
                    duplicate_count += 1
                    
            except Exception as e:
                failed_operations.append({
                    'message_data': message_data,
                    'error': str(e),
                    'index': i
                })
        
        success_count = len(successful_messages)
        failure_count = len(failed_operations)
        
        self.logger.info(
            f"Batch insert completed: {success_count} successful, "
            f"{failure_count} failed, {duplicate_count} duplicates "
            f"out of {len(messages)} total. "
            f"Validation time: {validation_result.validation_time_ms:.2f}ms"
        )
        
        return BatchInsertResult(
            successful_messages=successful_messages,
            failed_operations=failed_operations,
            total_processed=len(messages),
            success_count=success_count,
            failure_count=failure_count,
            duplicate_count=duplicate_count
        )
    
    async def validate_message_constraints(self, message_data: TelegramMessageData) -> bool:
        """
        Validates message data against database constraints.
        
        This method checks that the message data will not violate any database
        constraints before attempting insertion.
        
        Args:
            message_data: TelegramMessageData to validate
            
        Returns:
            bool: True if message data is valid
        """
        try:
            # Check required fields
            if not message_data.telegram_id:
                self.logger.warning("Message validation failed: missing telegram_id")
                return False
            
            if not message_data.group_id:
                self.logger.warning("Message validation failed: missing group_id")
                return False
            
            # Check if sender exists if sender_id is provided
            if message_data.sender_id:
                async def _check_sender(session: AsyncSession) -> bool:
                    result = await session.execute(
                        select(TelegramUser).where(TelegramUser.id == message_data.sender_id)
                    )
                    return result.scalar_one_or_none() is not None
                
                sender_exists = await session_manager.execute_with_retry(_check_sender)
                if not sender_exists:
                    self.logger.warning(f"Message validation failed: sender_id {message_data.sender_id} does not exist")
                    return False
            
            # Validate text length (assuming reasonable limit)
            if message_data.text and len(message_data.text) > 10000:
                self.logger.warning("Message validation failed: text too long")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating message constraints: {e}")
            return False
    
    async def get_message_by_telegram_id(self, telegram_id: int, group_id: int) -> Optional[TelegramMessage]:
        """
        Retrieves a message by its Telegram ID and group ID.
        
        Args:
            telegram_id: Telegram message ID
            group_id: Group database ID
            
        Returns:
            TelegramMessage: The message if found, None otherwise
        """
        async def _get_message_operation(session: AsyncSession) -> Optional[TelegramMessage]:
            result = await session.execute(
                select(TelegramMessage).where(
                    TelegramMessage.telegram_id == telegram_id,
                    TelegramMessage.group_id == group_id
                )
            )
            return result.scalar_one_or_none()
        
        try:
            return await session_manager.execute_with_retry(_get_message_operation)
        except Exception as e:
            self.logger.error(f"Error retrieving message {telegram_id} from group {group_id}: {e}")
            return None
    
    def create_message_data_from_entity(self, telegram_message: TelegramMessageEntity, 
                                      group_id: int, sender_id: Optional[int] = None) -> TelegramMessageData:
        """
        Creates TelegramMessageData from a Telegram message entity.
        
        Args:
            telegram_message: Telegram message entity
            group_id: Database group ID
            sender_id: Database sender ID (optional)
            
        Returns:
            TelegramMessageData: Message data structure
        """
        # Determine message type from media
        message_type = "text"
        if telegram_message.media:
            media_class = type(telegram_message.media).__name__
            if "Photo" in media_class:
                message_type = "photo"
            elif "Document" in media_class:
                message_type = "document"
            elif "Video" in media_class:
                message_type = "video"
            elif "Audio" in media_class:
                message_type = "audio"
            elif "Voice" in media_class:
                message_type = "voice"
            elif "Sticker" in media_class:
                message_type = "sticker"
        
        # Handle date timezone
        msg_date = telegram_message.date
        if msg_date and msg_date.tzinfo:
            msg_date = msg_date.replace(tzinfo=None)
        
        # Handle edit date
        edit_date = getattr(telegram_message, 'edit_date', None)
        if edit_date and edit_date.tzinfo:
            edit_date = edit_date.replace(tzinfo=None)
        
        # Handle forward date
        forward_date = getattr(telegram_message, 'forward_date', None)
        if forward_date and forward_date.tzinfo:
            forward_date = forward_date.replace(tzinfo=None)
        
        return TelegramMessageData(
            telegram_id=telegram_message.id,
            group_id=group_id,
            sender_id=sender_id,
            text=telegram_message.message or "",
            message_type=message_type,
            date=msg_date or datetime.utcnow(),
            edit_date=edit_date,
            reply_to_msg_id=getattr(telegram_message.reply_to, 'reply_to_msg_id', None) if hasattr(telegram_message, 'reply_to') and telegram_message.reply_to else None,
            forward_from_id=getattr(telegram_message, 'forward_from_id', None),
            forward_from_name=getattr(telegram_message, 'forward_from_name', None),
            forward_date=forward_date,
            views=getattr(telegram_message, 'views', None),
            forwards=getattr(telegram_message, 'forwards', None),
            is_pinned=getattr(telegram_message, 'pinned', False),
            grouped_id=getattr(telegram_message, 'grouped_id', None)
        )
    
    # Entity Resolution Methods
    
    async def resolve_entity_with_retry(self, client, entity_id: int, 
                                      strategies: Optional[List[EntityResolutionStrategy]] = None) -> EntityResolutionResult:
        """
        Attempts to resolve entity using multiple strategies with retry logic.
        
        Args:
            client: Telegram client instance
            entity_id: Entity ID to resolve
            strategies: Optional list of strategies to use (defaults to all strategies)
            
        Returns:
            EntityResolutionResult with resolution outcome
        """
        if strategies is None:
            strategies = self.RESOLUTION_STRATEGIES
        
        # Check if channel is marked as unavailable
        if self._is_channel_unavailable(entity_id):
            self.logger.debug(f"Skipping entity resolution for unavailable channel {entity_id}")
            return EntityResolutionResult(
                success=False,
                entity=None,
                error="Channel marked as unavailable",
                should_retry=False,
                should_mark_unavailable=False
            )
        
        # Check entity cache first
        cached_entity = self._get_cached_entity(entity_id)
        if cached_entity:
            self.logger.debug(f"Using cached entity for {entity_id}")
            return EntityResolutionResult(
                success=True,
                entity=cached_entity,
                error=None,
                should_retry=False,
                should_mark_unavailable=False
            )
        
        last_error = None
        
        for strategy in strategies:
            self.logger.debug(f"Trying entity resolution strategy '{strategy.name}' for {entity_id}")
            
            for attempt in range(strategy.max_attempts):
                try:
                    if strategy.delay_seconds > 0 and attempt > 0:
                        await asyncio.sleep(strategy.delay_seconds)
                    
                    entity = await self._resolve_entity_with_strategy(client, entity_id, strategy)
                    
                    if entity:
                        # Cache successful resolution
                        self._cache_entity(entity_id, entity)
                        self.logger.info(f"Successfully resolved entity {entity_id} using strategy '{strategy.name}'")
                        return EntityResolutionResult(
                            success=True,
                            entity=entity,
                            error=None,
                            should_retry=False,
                            should_mark_unavailable=False
                        )
                
                except (PeerIdInvalidError, ChannelPrivateError, ChatAdminRequiredError) as e:
                    last_error = str(e)
                    self.logger.warning(f"Entity resolution failed for {entity_id} with strategy '{strategy.name}', attempt {attempt + 1}: {e}")
                    
                    # These errors indicate the channel is likely unavailable
                    if isinstance(e, (ChannelPrivateError, ChatAdminRequiredError)):
                        return EntityResolutionResult(
                            success=False,
                            entity=None,
                            error=last_error,
                            should_retry=False,
                            should_mark_unavailable=True
                        )
                
                except Exception as e:
                    last_error = str(e)
                    self.logger.error(f"Unexpected error resolving entity {entity_id} with strategy '{strategy.name}': {e}")
        
        # All strategies failed
        self.logger.error(f"All entity resolution strategies failed for {entity_id}. Last error: {last_error}")
        
        # Determine if we should mark as unavailable based on error patterns
        should_mark_unavailable = self._should_mark_unavailable(last_error)
        
        return EntityResolutionResult(
            success=False,
            entity=None,
            error=last_error,
            should_retry=not should_mark_unavailable,
            should_mark_unavailable=should_mark_unavailable
        )
    
    async def _resolve_entity_with_strategy(self, client, entity_id: int, 
                                          strategy: EntityResolutionStrategy) -> Optional[Any]:
        """
        Resolves entity using a specific strategy with API rate limiting.
        
        Args:
            client: Telegram client instance
            entity_id: Entity ID to resolve
            strategy: Resolution strategy to use
            
        Returns:
            Resolved entity or None if resolution failed
        """
        try:
            if strategy.name == "direct":
                # Use API rate limiter for get_entity calls
                return await self.rate_limiter.execute_with_rate_limit(
                    client.get_entity, OperationType.USER_INFO, entity_id
                )
            
            elif strategy.name == "by_username":
                # Try to resolve by username if we have it cached
                # This is a simplified implementation - in practice you'd need username lookup
                return await self.rate_limiter.execute_with_rate_limit(
                    client.get_entity, OperationType.USER_INFO, entity_id
                )
            
            elif strategy.name == "by_phone":
                # Try to resolve by phone number if available
                # This is a simplified implementation - in practice you'd need phone lookup
                return await self.rate_limiter.execute_with_rate_limit(
                    client.get_entity, OperationType.USER_INFO, entity_id
                )
            
            elif strategy.name == "cache_refresh":
                # Force refresh entity cache and try again
                self._invalidate_entity_cache(entity_id)
                return await self.rate_limiter.execute_with_rate_limit(
                    client.get_entity, OperationType.USER_INFO, entity_id
                )
            
            else:
                self.logger.warning(f"Unknown entity resolution strategy: {strategy.name}")
                return None
                
        except Exception as e:
            self.logger.debug(f"Strategy '{strategy.name}' failed for entity {entity_id}: {e}")
            raise
    
    def mark_channel_unavailable(self, channel_id: int, telegram_id: int, reason: str) -> None:
        """
        Marks a channel as unavailable for future processing.
        
        Args:
            channel_id: Database channel ID
            telegram_id: Telegram channel ID
            reason: Reason for marking unavailable
        """
        now = datetime.utcnow()
        
        if channel_id in self._unavailable_channels:
            # Update existing entry
            info = self._unavailable_channels[channel_id]
            self._unavailable_channels[channel_id] = ChannelUnavailabilityInfo(
                channel_id=channel_id,
                telegram_id=telegram_id,
                failure_count=info.failure_count + 1,
                first_failure=info.first_failure,
                last_failure=now,
                is_marked_unavailable=info.failure_count + 1 >= self.UNAVAILABLE_THRESHOLD,
                reason=reason
            )
        else:
            # Create new entry
            self._unavailable_channels[channel_id] = ChannelUnavailabilityInfo(
                channel_id=channel_id,
                telegram_id=telegram_id,
                failure_count=1,
                first_failure=now,
                last_failure=now,
                is_marked_unavailable=False,
                reason=reason
            )
        
        info = self._unavailable_channels[channel_id]
        if info.is_marked_unavailable:
            self.logger.warning(f"Channel {channel_id} (telegram_id: {telegram_id}) marked as unavailable after {info.failure_count} failures. Reason: {reason}")
        else:
            self.logger.info(f"Channel {channel_id} failure count: {info.failure_count}/{self.UNAVAILABLE_THRESHOLD}")
    
    def _is_channel_unavailable(self, channel_id: int) -> bool:
        """
        Checks if a channel is marked as unavailable.
        
        Args:
            channel_id: Channel ID to check
            
        Returns:
            bool: True if channel is unavailable
        """
        if channel_id not in self._unavailable_channels:
            return False
        
        info = self._unavailable_channels[channel_id]
        
        # Check if enough time has passed to retry
        if info.is_marked_unavailable:
            time_since_last_failure = datetime.utcnow() - info.last_failure
            if time_since_last_failure > self.UNAVAILABLE_TIMEOUT:
                # Reset the unavailable status for retry
                self._unavailable_channels[channel_id] = ChannelUnavailabilityInfo(
                    channel_id=info.channel_id,
                    telegram_id=info.telegram_id,
                    failure_count=0,
                    first_failure=info.first_failure,
                    last_failure=info.last_failure,
                    is_marked_unavailable=False,
                    reason=info.reason
                )
                self.logger.info(f"Channel {channel_id} unavailable timeout expired, allowing retry")
                return False
        
        return info.is_marked_unavailable
    
    def _cache_entity(self, entity_id: int, entity: Any) -> None:
        """
        Caches an entity for future use.
        
        Args:
            entity_id: Entity ID
            entity: Entity object to cache
        """
        self._entity_cache[entity_id] = entity
        self._cache_timestamps[entity_id] = datetime.utcnow()
        self.logger.debug(f"Cached entity {entity_id}")
    
    def _get_cached_entity(self, entity_id: int) -> Optional[Any]:
        """
        Retrieves cached entity if still valid.
        
        Args:
            entity_id: Entity ID to retrieve
            
        Returns:
            Cached entity or None if not found/expired
        """
        if entity_id not in self._entity_cache:
            return None
        
        # Check if cache entry is still valid
        cache_time = self._cache_timestamps.get(entity_id)
        if not cache_time or datetime.utcnow() - cache_time > self._cache_ttl:
            # Cache expired, remove entry
            self._invalidate_entity_cache(entity_id)
            return None
        
        return self._entity_cache[entity_id]
    
    def _invalidate_entity_cache(self, entity_id: int) -> None:
        """
        Invalidates cached entity.
        
        Args:
            entity_id: Entity ID to invalidate
        """
        self._entity_cache.pop(entity_id, None)
        self._cache_timestamps.pop(entity_id, None)
        self.logger.debug(f"Invalidated cache for entity {entity_id}")
    
    def refresh_entity_cache(self) -> None:
        """
        Refreshes the entire entity cache by removing expired entries.
        """
        now = datetime.utcnow()
        expired_entities = []
        
        for entity_id, cache_time in self._cache_timestamps.items():
            if now - cache_time > self._cache_ttl:
                expired_entities.append(entity_id)
        
        for entity_id in expired_entities:
            self._invalidate_entity_cache(entity_id)
        
        if expired_entities:
            self.logger.info(f"Refreshed entity cache, removed {len(expired_entities)} expired entries")
    
    def _should_mark_unavailable(self, error_message: str) -> bool:
        """
        Determines if an error indicates the channel should be marked unavailable.
        
        Args:
            error_message: Error message to analyze
            
        Returns:
            bool: True if channel should be marked unavailable
        """
        if not error_message:
            return False
        
        error_lower = error_message.lower()
        unavailable_indicators = [
            'channel is private',
            'chat admin required',
            'peer id invalid',
            'channel not found',
            'access denied',
            'forbidden',
            'deleted'
        ]
        
        return any(indicator in error_lower for indicator in unavailable_indicators)
    
    def get_unavailable_channels_stats(self) -> Dict[str, Any]:
        """
        Returns statistics about unavailable channels.
        
        Returns:
            dict: Statistics about channel unavailability
        """
        total_tracked = len(self._unavailable_channels)
        marked_unavailable = sum(1 for info in self._unavailable_channels.values() if info.is_marked_unavailable)
        
        return {
            'total_tracked_channels': total_tracked,
            'marked_unavailable': marked_unavailable,
            'available_for_retry': total_tracked - marked_unavailable,
            'cache_size': len(self._entity_cache)
        }


# Global instance
message_ingestion_service = MessageIngestionService()