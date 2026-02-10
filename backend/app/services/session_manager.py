"""
Session Manager Component

This component handles database session lifecycle management, transaction handling,
and provides robust error recovery for database operations including PendingRollbackError
handling and automatic session recreation.

Requirements: 4.1, 4.2, 4.3
"""

import logging
import asyncio
from typing import Callable, Any, Optional, TypeVar, Generic
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import (
    PendingRollbackError, 
    SQLAlchemyError, 
    IntegrityError,
    OperationalError
)
from backend.app.db.database import async_session_maker

logger = logging.getLogger(__name__)

T = TypeVar('T')


class SessionOperationResult(Generic[T]):
    """Result of a session operation with success status and optional result"""
    def __init__(self, success: bool, result: Optional[T] = None, error: Optional[Exception] = None):
        self.success = success
        self.result = result
        self.error = error


class SessionManager:
    """
    Manages database session lifecycle and transaction management.
    
    Provides robust error handling for database operations including:
    - Automatic session recreation on PendingRollbackError
    - Retry logic for transient database errors
    - Proper session cleanup and resource management
    - Transaction state monitoring and recovery
    """
    
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.logger = logging.getLogger(__name__)
    
    async def create_session(self) -> AsyncSession:
        """
        Creates a new database session with proper configuration.
        
        Returns:
            AsyncSession: A new database session
        """
        try:
            session = async_session_maker()
            self.logger.debug("Created new database session")
            return session
        except Exception as e:
            self.logger.error(f"Failed to create database session: {e}")
            raise
    
    async def execute_with_retry(self, operation: Callable[[AsyncSession], Any], 
                               max_retries: Optional[int] = None) -> SessionOperationResult[Any]:
        """
        Executes database operation with automatic retry on rollback errors.
        
        Args:
            operation: Async function that takes a session and returns a result
            max_retries: Maximum number of retry attempts (defaults to instance max_retries)
            
        Returns:
            SessionOperationResult with success status and result/error
        """
        max_retries = max_retries or self.max_retries
        last_error = None
        
        for attempt in range(max_retries + 1):
            session = None
            try:
                session = await self.create_session()
                
                # Execute the operation
                result = await operation(session)
                
                # Commit if the session is still active
                if session.in_transaction():
                    await session.commit()
                
                self.logger.debug(f"Operation succeeded on attempt {attempt + 1}")
                return SessionOperationResult(success=True, result=result)
                
            except PendingRollbackError as e:
                self.logger.warning(
                    f"PendingRollbackError on attempt {attempt + 1}: {e}"
                )
                last_error = e
                
                if session:
                    session = await self.handle_rollback_error(session, e)
                
                if attempt < max_retries:
                    self.logger.info(f"Retrying operation (attempt {attempt + 2})")
                    await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                    
            except (IntegrityError, OperationalError) as e:
                self.logger.warning(
                    f"Database error on attempt {attempt + 1}: {e}"
                )
                last_error = e
                
                if session:
                    await self._safe_rollback(session)
                    await self._safe_close(session)
                
                if attempt < max_retries and self._is_retryable_error(e):
                    self.logger.info(f"Retrying operation (attempt {attempt + 2})")
                    await asyncio.sleep(0.1 * (attempt + 1))
                    continue
                else:
                    break
                    
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                last_error = e
                
                if session:
                    await self._safe_rollback(session)
                    await self._safe_close(session)
                break
                
            finally:
                if session:
                    await self._safe_close(session)
        
        self.logger.error(f"Operation failed after {max_retries + 1} attempts")
        return SessionOperationResult(success=False, error=last_error)
    
    async def handle_rollback_error(self, session: AsyncSession, 
                                  error: Exception) -> AsyncSession:
        """
        Handles PendingRollbackError by creating fresh session.
        
        Args:
            session: The session with pending rollback
            error: The original error that caused the rollback
            
        Returns:
            AsyncSession: A new fresh session
        """
        try:
            self.logger.info("Handling PendingRollbackError - rolling back and creating new session")
            
            # Attempt to rollback the current session
            await self._safe_rollback(session)
            
            # Close the problematic session
            await self._safe_close(session)
            
            # Create a new fresh session
            new_session = await self.create_session()
            
            self.logger.info("Successfully created new session after rollback error")
            return new_session
            
        except Exception as e:
            self.logger.error(f"Error handling rollback: {e}")
            # If we can't handle the rollback, create a completely new session
            try:
                await self._safe_close(session)
                return await self.create_session()
            except Exception as create_error:
                self.logger.error(f"Failed to create new session: {create_error}")
                raise
    
    async def safe_commit(self, session: AsyncSession) -> bool:
        """
        Commits transaction with proper error handling.
        
        Args:
            session: The session to commit
            
        Returns:
            bool: True if commit succeeded, False otherwise
        """
        try:
            if session.in_transaction():
                await session.commit()
                self.logger.debug("Transaction committed successfully")
                return True
            else:
                self.logger.debug("No active transaction to commit")
                return True
                
        except PendingRollbackError as e:
            self.logger.warning(f"PendingRollbackError during commit: {e}")
            await self._safe_rollback(session)
            return False
            
        except Exception as e:
            self.logger.error(f"Error during commit: {e}")
            await self._safe_rollback(session)
            return False
    
    @asynccontextmanager
    async def session_scope(self):
        """
        Context manager for database sessions with automatic cleanup.
        
        Usage:
            async with session_manager.session_scope() as session:
                # Use session here
                pass
        """
        session = None
        try:
            session = await self.create_session()
            yield session
            
            if session.in_transaction():
                await session.commit()
                
        except PendingRollbackError as e:
            self.logger.warning(f"PendingRollbackError in session scope: {e}")
            if session:
                await self._safe_rollback(session)
            raise
            
        except Exception as e:
            self.logger.error(f"Error in session scope: {e}")
            if session:
                await self._safe_rollback(session)
            raise
            
        finally:
            if session:
                await self._safe_close(session)
    
    async def _safe_rollback(self, session: AsyncSession) -> None:
        """Safely rollback a session without raising exceptions"""
        try:
            if session.in_transaction():
                await session.rollback()
                self.logger.debug("Session rolled back successfully")
        except Exception as e:
            self.logger.warning(f"Error during rollback (ignored): {e}")
    
    async def _safe_close(self, session: AsyncSession) -> None:
        """Safely close a session without raising exceptions"""
        try:
            await session.close()
            self.logger.debug("Session closed successfully")
        except Exception as e:
            self.logger.warning(f"Error during session close (ignored): {e}")
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Determines if an error is retryable.
        
        Args:
            error: The exception to check
            
        Returns:
            bool: True if the error is retryable
        """
        # Connection errors are usually retryable
        if isinstance(error, OperationalError):
            error_msg = str(error).lower()
            retryable_patterns = [
                'connection',
                'timeout',
                'network',
                'temporary',
                'deadlock'
            ]
            return any(pattern in error_msg for pattern in retryable_patterns)
        
        # PendingRollbackError is always retryable
        if isinstance(error, PendingRollbackError):
            return True
        
        # Most integrity errors are not retryable (they indicate data issues)
        if isinstance(error, IntegrityError):
            return False
        
        return False
    
    async def execute_batch_with_partial_failure_handling(
        self, 
        operations: list[Callable[[AsyncSession], Any]]
    ) -> list[SessionOperationResult[Any]]:
        """
        Executes a batch of operations with individual error handling.
        
        Each operation is executed independently, so failures in one operation
        don't affect others.
        
        Args:
            operations: List of async functions that take a session
            
        Returns:
            List of SessionOperationResult objects, one per operation
        """
        results = []
        
        for i, operation in enumerate(operations):
            self.logger.debug(f"Executing batch operation {i + 1}/{len(operations)}")
            result = await self.execute_with_retry(operation)
            results.append(result)
            
            if not result.success:
                self.logger.warning(
                    f"Batch operation {i + 1} failed: {result.error}"
                )
        
        successful_count = sum(1 for r in results if r.success)
        self.logger.info(
            f"Batch execution completed: {successful_count}/{len(operations)} succeeded"
        )
        
        return results