"""
Session Manager Component

This component handles database session lifecycle management, transaction handling,
and provides robust error recovery for database operations.

Requirements: 4.1, 4.2, 4.3
"""

import asyncio
import logging
from typing import Callable, Any, Optional, TypeVar, Generic
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError, PendingRollbackError, InvalidRequestError
from backend.app.db.database import async_session_maker
from backend.app.core.logging_config import get_logger

logger = get_logger("session_manager")

T = TypeVar('T')


class SessionError(Exception):
    """Base exception for session management errors"""
    pass


class SessionRecoveryError(SessionError):
    """Exception raised when session recovery fails"""
    pass


class SessionManager:
    """
    Manages database session lifecycle and transaction handling.
    
    This component provides robust error recovery for database operations,
    handles PendingRollbackError situations, and ensures proper session cleanup.
    """
    
    def __init__(self):
        self.logger = logger
        self._active_sessions = set()
        
    async def create_session(self) -> AsyncSession:
        """
        Creates a new database session with proper configuration.
        
        Returns:
            AsyncSession: Configured database session
        """
        try:
            session = async_session_maker()
            self._active_sessions.add(id(session))
            self.logger.debug(f"Created new session {id(session)}")
            return session
            
        except Exception as e:
            self.logger.error(f"Failed to create database session: {e}")
            raise SessionError(f"Session creation failed: {e}")
    
    async def execute_with_retry(
        self, 
        operation: Callable[[AsyncSession], Any], 
        max_retries: int = 3,
        session: Optional[AsyncSession] = None
    ) -> Any:
        """
        Executes database operation with automatic retry on rollback errors.
        
        Args:
            operation: Async function that takes a session and returns a result
            max_retries: Maximum number of retry attempts
            session: Optional existing session to use
            
        Returns:
            Result of the operation
            
        Raises:
            SessionRecoveryError: If all retry attempts fail
        """
        last_error = None
        current_session = session
        
        for attempt in range(max_retries + 1):
            try:
                # Create new session if none provided or if we need to recover
                if current_session is None or (attempt > 0):
                    if current_session:
                        await self._cleanup_session(current_session)
                    current_session = await self.create_session()
                
                # Execute the operation
                result = await operation(current_session)
                
                # If we created the session, commit and close it
                if session is None:
                    await self.safe_commit(current_session)
                    await self._cleanup_session(current_session)
                
                self.logger.debug(f"Operation succeeded on attempt {attempt + 1}")
                return result
                
            except PendingRollbackError as e:
                last_error = e
                self.logger.warning(f"PendingRollbackError on attempt {attempt + 1}: {e}")
                current_session = await self.handle_rollback_error(current_session, e)
                
            except (SQLAlchemyError, Exception) as e:
                last_error = e
                self.logger.error(f"Operation failed on attempt {attempt + 1}: {e}")
                
                if current_session:
                    try:
                        await current_session.rollback()
                    except Exception as rollback_error:
                        self.logger.error(f"Rollback failed: {rollback_error}")
                
                # Don't retry on non-recoverable errors
                if not self._is_recoverable_error(e):
                    break
                    
                if attempt < max_retries:
                    await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
        
        # Clean up session if we created it
        if session is None and current_session:
            await self._cleanup_session(current_session)
        
        raise SessionRecoveryError(f"Operation failed after {max_retries + 1} attempts. Last error: {last_error}")
    
    async def handle_rollback_error(self, session: AsyncSession, error: Exception) -> AsyncSession:
        """
        Handles PendingRollbackError by creating fresh session.
        
        Args:
            session: Session with pending rollback
            error: The rollback error that occurred
            
        Returns:
            New fresh session
        """
        # Enhanced session rollback logging
        session_id = id(session) if session else "unknown"
        self.logger.error(
            f"Session rollback error occurred. "
            f"Session ID: {session_id}, "
            f"Error type: {type(error).__name__}, "
            f"Error message: {error}, "
            f"Recovery action: Creating fresh session"
        )
        
        try:
            # Try to rollback the current session
            if session:
                try:
                    await session.rollback()
                    self.logger.info(f"Successfully rolled back problematic session {session_id}")
                except Exception as rollback_error:
                    self.logger.error(
                        f"Rollback failed for session {session_id}. "
                        f"Rollback error: {rollback_error}, "
                        f"Original error: {error}, "
                        f"Action: Creating new session anyway"
                    )
                
                # Clean up the old session
                await self._cleanup_session(session)
            
            # Create a fresh session
            new_session = await self.create_session()
            new_session_id = id(new_session)
            self.logger.info(
                f"Session recovery completed. "
                f"Old session: {session_id}, "
                f"New session: {new_session_id}, "
                f"Cause: {type(error).__name__}"
            )
            return new_session
            
        except Exception as e:
            self.logger.error(
                f"Session recovery failed completely. "
                f"Original session: {session_id}, "
                f"Original error: {error}, "
                f"Recovery error: {e}, "
                f"Action: Raising SessionRecoveryError"
            )
            raise SessionRecoveryError(f"Could not recover from rollback error: {e}")
    
    async def safe_commit(self, session: AsyncSession) -> bool:
        """
        Commits transaction with proper error handling.
        
        Args:
            session: Session to commit
            
        Returns:
            bool: True if commit succeeded, False otherwise
        """
        session_id = id(session)
        
        try:
            await session.commit()
            self.logger.debug(f"Successfully committed session {session_id}")
            return True
            
        except PendingRollbackError as e:
            self.logger.error(
                f"PendingRollbackError during commit. "
                f"Session: {session_id}, "
                f"Error: {e}, "
                f"Action: Rolling back session"
            )
            try:
                await session.rollback()
                self.logger.info(f"Rolled back session {session_id} after PendingRollbackError")
            except Exception as rollback_error:
                self.logger.error(
                    f"Failed to rollback session {session_id} after PendingRollbackError. "
                    f"Rollback error: {rollback_error}, "
                    f"Original error: {e}"
                )
            return False
            
        except SQLAlchemyError as e:
            self.logger.error(
                f"SQLAlchemy error during commit. "
                f"Session: {session_id}, "
                f"Error type: {type(e).__name__}, "
                f"Error: {e}, "
                f"Action: Rolling back session"
            )
            try:
                await session.rollback()
                self.logger.info(f"Rolled back session {session_id} after SQLAlchemy error")
            except Exception as rollback_error:
                self.logger.error(
                    f"Failed to rollback session {session_id} after SQLAlchemy error. "
                    f"Rollback error: {rollback_error}, "
                    f"Original error: {e}"
                )
            return False
            
        except Exception as e:
            self.logger.error(
                f"Unexpected error during commit. "
                f"Session: {session_id}, "
                f"Error type: {type(e).__name__}, "
                f"Error: {e}, "
                f"Action: Rolling back session"
            )
            try:
                await session.rollback()
                self.logger.info(f"Rolled back session {session_id} after unexpected error")
            except Exception as rollback_error:
                self.logger.error(
                    f"Failed to rollback session {session_id} after unexpected error. "
                    f"Rollback error: {rollback_error}, "
                    f"Original error: {e}"
                )
            return False
    
    @asynccontextmanager
    async def session_scope(self):
        """
        Context manager for automatic session lifecycle management.
        
        Usage:
            async with session_manager.session_scope() as session:
                # Use session for database operations
                pass
        """
        session = None
        try:
            session = await self.create_session()
            yield session
            await self.safe_commit(session)
            
        except Exception as e:
            self.logger.error(f"Error in session scope: {e}")
            if session:
                try:
                    await session.rollback()
                except Exception as rollback_error:
                    self.logger.error(f"Failed to rollback in session scope: {rollback_error}")
            raise
            
        finally:
            if session:
                await self._cleanup_session(session)
    
    @asynccontextmanager
    async def transaction_scope(self, session: AsyncSession):
        """
        Context manager for transaction handling with automatic rollback on errors.
        
        Args:
            session: Existing session to use for the transaction
            
        Usage:
            async with session_manager.transaction_scope(session):
                # Database operations within transaction
                pass
        """
        try:
            yield session
            # Note: Commit is handled by the caller or session_scope
            
        except Exception as e:
            self.logger.error(f"Error in transaction scope: {e}")
            try:
                await session.rollback()
                self.logger.debug("Rolled back transaction after error")
            except Exception as rollback_error:
                self.logger.error(f"Failed to rollback transaction: {rollback_error}")
            raise
    
    async def _cleanup_session(self, session: AsyncSession) -> None:
        """Clean up and close a database session"""
        try:
            session_id = id(session)
            
            # Remove from active sessions tracking
            self._active_sessions.discard(session_id)
            
            # Close the session
            await session.close()
            self.logger.debug(f"Cleaned up session {session_id}")
            
        except Exception as e:
            self.logger.warning(f"Error during session cleanup: {e}")
    
    def _is_recoverable_error(self, error: Exception) -> bool:
        """
        Determines if an error is recoverable and worth retrying.
        
        Args:
            error: The exception to check
            
        Returns:
            bool: True if the error is recoverable
        """
        # PendingRollbackError is always recoverable
        if isinstance(error, PendingRollbackError):
            return True
        
        # Connection errors are usually recoverable
        if isinstance(error, SQLAlchemyError):
            error_str = str(error).lower()
            recoverable_patterns = [
                'connection',
                'timeout',
                'deadlock',
                'lock',
                'temporary',
                'retry'
            ]
            return any(pattern in error_str for pattern in recoverable_patterns)
        
        return False
    
    async def get_session_stats(self) -> dict:
        """
        Returns statistics about active sessions.
        
        Returns:
            dict: Session statistics
        """
        return {
            'active_sessions': len(self._active_sessions),
            'session_ids': list(self._active_sessions)
        }
    
    async def cleanup_all_sessions(self) -> None:
        """Emergency cleanup of all tracked sessions"""
        self.logger.warning(f"Emergency cleanup of {len(self._active_sessions)} active sessions")
        self._active_sessions.clear()


# Global instance
session_manager = SessionManager()