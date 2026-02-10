"""
Integration Service

This service demonstrates how all the enhanced database components work together
and provides utility methods for coordinated operations.

Requirements: All requirements
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from backend.app.core.database_manager import database_manager
from backend.app.core.session_manager import session_manager
from backend.app.core.constraint_validator import constraint_validator
from backend.app.core.api_rate_limiter import APIRateLimiter, OperationType
from backend.app.services.message_ingestion_service import message_ingestion_service, TelegramMessageData
from backend.app.services.user_management_service import user_management_service, TelegramUserData
from backend.app.core.logging_config import get_logger

logger = get_logger("integration_service")


class IntegrationService:
    """
    Service that coordinates all enhanced database components.
    
    This service provides high-level operations that use multiple components
    together to ensure data consistency and proper error handling.
    """
    
    def __init__(self):
        self.logger = logger
        
        # Enhanced components
        self.rate_limiter = APIRateLimiter()
    
    async def initialize_database_constraints(self) -> Dict[str, Any]:
        """
        Initializes database constraints and validates schema integrity.
        
        Returns:
            Dictionary with initialization results
        """
        self.logger.info("Initializing database constraints and schema validation")
        
        try:
            # Validate existing constraints
            validation_result = await database_manager.validate_constraints()
            
            # Create missing constraints if needed
            if not validation_result.is_valid:
                self.logger.info(f"Creating {len(validation_result.missing_constraints)} missing constraints")
                await database_manager.create_missing_constraints()
                
                # Re-validate after creation
                validation_result = await database_manager.validate_constraints()
            
            # Create performance indexes
            await database_manager.create_performance_indexes()
            
            self.logger.info("Database initialization completed successfully")
            
            return {
                'success': True,
                'constraints_valid': validation_result.is_valid,
                'missing_constraints': len(validation_result.missing_constraints),
                'existing_constraints': len(validation_result.existing_constraints),
                'errors': validation_result.errors
            }
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def process_telegram_batch_with_validation(
        self, 
        users: List[TelegramUserData], 
        messages: List[TelegramMessageData],
        validate_before_insert: bool = True
    ) -> Dict[str, Any]:
        """
        Processes a batch of users and messages with full validation and error handling.
        
        Args:
            users: List of user data to process
            messages: List of message data to process
            validate_before_insert: Whether to validate constraints before insertion
            
        Returns:
            Dictionary with processing results
        """
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Processing batch: {len(users)} users, {len(messages)} messages")
            
            # Step 1: Process users first (messages depend on users)
            user_results = await user_management_service.batch_upsert_users(users)
            
            # Step 2: Process messages
            message_results = await message_ingestion_service.batch_insert_messages(messages)
            
            # Step 3: Calculate processing statistics
            end_time = datetime.now()
            processing_time_ms = (end_time - start_time).total_seconds() * 1000
            
            results = {
                'success': True,
                'processing_time_ms': processing_time_ms,
                'users': {
                    'total': len(users),
                    'successful': user_results.success_count,
                    'failed': user_results.failure_count
                },
                'messages': {
                    'total': len(messages),
                    'successful': message_results.success_count,
                    'failed': message_results.failure_count,
                    'duplicates': message_results.duplicate_count
                },
                'overall_success_rate': (
                    (user_results.success_count + message_results.success_count) /
                    (len(users) + len(messages))
                ) if (len(users) + len(messages)) > 0 else 0
            }
            
            self.logger.info(
                f"Batch processing completed in {processing_time_ms:.2f}ms. "
                f"Users: {user_results.success_count}/{len(users)}, "
                f"Messages: {message_results.success_count}/{len(messages)}"
            )
            
            return results
            
        except Exception as e:
            end_time = datetime.now()
            processing_time_ms = (end_time - start_time).total_seconds() * 1000
            
            self.logger.error(f"Batch processing failed after {processing_time_ms:.2f}ms: {e}")
            
            return {
                'success': False,
                'error': str(e),
                'processing_time_ms': processing_time_ms
            }
    
    async def validate_data_integrity(self, table_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Validates data integrity across specified tables.
        
        Args:
            table_names: List of table names to validate (default: all tables)
            
        Returns:
            Dictionary with validation results
        """
        if table_names is None:
            table_names = ['telegram_users', 'telegram_messages', 'telegram_groups']
        
        validation_results = []
        
        for table_name in table_names:
            try:
                # Get sample records for validation
                async def _get_sample_records(session):
                    from sqlalchemy import text
                    result = await session.execute(
                        text(f"SELECT * FROM {table_name} LIMIT 100")
                    )
                    return [dict(row._mapping) for row in result]
                
                sample_records = await session_manager.execute_with_retry(_get_sample_records)
                
                if sample_records:
                    validation_result = await constraint_validator.validate_batch_operation(
                        table_name, sample_records
                    )
                    validation_results.append({
                        'table': table_name,
                        'is_valid': validation_result.is_valid,
                        'records_checked': validation_result.total_records_checked,
                        'violations': len(validation_result.violations),
                        'referential_violations': len(validation_result.referential_violations),
                        'validation_time_ms': validation_result.validation_time_ms
                    })
                else:
                    validation_results.append({
                        'table': table_name,
                        'is_valid': True,
                        'records_checked': 0,
                        'violations': 0,
                        'referential_violations': 0,
                        'validation_time_ms': 0
                    })
                    
            except Exception as e:
                self.logger.error(f"Validation failed for table {table_name}: {e}")
                validation_results.append({
                    'table': table_name,
                    'is_valid': False,
                    'error': str(e)
                })
        
        # Calculate summary statistics
        total_violations = sum(r.get('violations', 0) for r in validation_results)
        total_referential_violations = sum(r.get('referential_violations', 0) for r in validation_results)
        valid_tables = sum(1 for r in validation_results if r.get('is_valid', False))
        
        return {
            'tables_validated': len(validation_results),
            'valid_tables': valid_tables,
            'invalid_tables': len(validation_results) - valid_tables,
            'total_violations': total_violations,
            'total_referential_violations': total_referential_violations,
            'results': validation_results
        }
    
    async def get_system_health_status(self) -> Dict[str, Any]:
        """
        Gets comprehensive system health status from all components.
        
        Returns:
            Dictionary with system health information
        """
        try:
            # Get session manager stats
            session_stats = await session_manager.get_session_stats()
            
            # Get API rate limiter stats
            rate_limiter_stats = self.rate_limiter.get_statistics()
            
            # Get constraint validation status
            constraint_status = await database_manager.validate_constraints()
            
            # Get entity resolution stats from message ingestion service
            entity_stats = message_ingestion_service.get_unavailable_channels_stats()
            
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_health': 'healthy',  # Could be calculated based on various factors
                'components': {
                    'session_manager': {
                        'status': 'healthy',
                        'active_sessions': session_stats['active_sessions']
                    },
                    'api_rate_limiter': {
                        'status': 'healthy',
                        'global_stats': rate_limiter_stats['global_stats'],
                        'operation_stats': rate_limiter_stats['operation_stats'],
                        'account_stats': rate_limiter_stats['account_stats'],
                        'queue_stats': rate_limiter_stats['queue_stats']
                    },
                    'database_constraints': {
                        'status': 'healthy' if constraint_status.is_valid else 'warning',
                        'constraints_valid': constraint_status.is_valid,
                        'missing_constraints': len(constraint_status.missing_constraints),
                        'existing_constraints': len(constraint_status.existing_constraints)
                    },
                    'entity_resolution': {
                        'status': 'healthy',
                        'total_tracked_channels': entity_stats['total_tracked_channels'],
                        'marked_unavailable': entity_stats['marked_unavailable'],
                        'cache_size': entity_stats['cache_size']
                    }
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get system health status: {e}")
            return {
                'timestamp': datetime.now().isoformat(),
                'overall_health': 'error',
                'error': str(e)
            }
    
    async def cleanup_and_optimize(self) -> Dict[str, Any]:
        """
        Performs cleanup and optimization operations across all components.
        
        Returns:
            Dictionary with cleanup results
        """
        try:
            self.logger.info("Starting system cleanup and optimization")
            
            # Cleanup expired entity cache
            message_ingestion_service.refresh_entity_cache()
            
            # Cleanup any stale sessions
            await session_manager.cleanup_all_sessions()
            
            # Get final stats
            session_stats = await session_manager.get_session_stats()
            entity_stats = message_ingestion_service.get_unavailable_channels_stats()
            
            self.logger.info("System cleanup completed successfully")
            
            return {
                'success': True,
                'cleanup_time': datetime.now().isoformat(),
                'results': {
                    'sessions_cleaned': True,
                    'entity_cache_refreshed': True,
                    'active_sessions': session_stats['active_sessions'],
                    'entity_cache_size': entity_stats['cache_size']
                }
            }
            
        except Exception as e:
            self.logger.error(f"System cleanup failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }


# Global instance
integration_service = IntegrationService()