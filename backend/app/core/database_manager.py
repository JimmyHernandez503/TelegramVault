"""
Database Manager Component

This component handles database schema validation, constraint management,
and ensures proper UPSERT operations work correctly.

Requirements: 1.1, 1.2, 1.3
"""

import logging
from typing import List, Dict, Any, Optional, NamedTuple
from sqlalchemy import text, inspect, MetaData
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from backend.app.db.database import engine, async_session_maker
from backend.app.core.logging_config import get_logger

logger = get_logger("database_manager")


class ConstraintInfo(NamedTuple):
    """Information about a database constraint"""
    name: str
    type: str
    columns: List[str]
    table_name: str


class ConstraintValidationResult(NamedTuple):
    """Result of constraint validation"""
    is_valid: bool
    missing_constraints: List[Dict[str, Any]]
    existing_constraints: List[ConstraintInfo]
    errors: List[str]


class DatabaseManager:
    """
    Manages database schema integrity and constraint validation.
    
    This component ensures that all ON CONFLICT clauses in UPSERT operations
    have corresponding unique constraints in the database schema.
    """
    
    def __init__(self):
        self.logger = logger
        
    async def validate_constraints(self) -> ConstraintValidationResult:
        """
        Validates all database constraints match UPSERT operations.
        
        Returns:
            ConstraintValidationResult with validation status and details
        """
        try:
            async with async_session_maker() as session:
                existing_constraints = await self._get_all_constraints(session)
                missing_constraints = await self._check_required_constraints(session, existing_constraints)
                
                is_valid = len(missing_constraints) == 0
                
                self.logger.info(f"Constraint validation completed. Valid: {is_valid}, "
                               f"Missing: {len(missing_constraints)}, "
                               f"Existing: {len(existing_constraints)}")
                
                return ConstraintValidationResult(
                    is_valid=is_valid,
                    missing_constraints=missing_constraints,
                    existing_constraints=existing_constraints,
                    errors=[]
                )
                
        except Exception as e:
            self.logger.error(f"Constraint validation failed: {e}")
            return ConstraintValidationResult(
                is_valid=False,
                missing_constraints=[],
                existing_constraints=[],
                errors=[str(e)]
            )
    
    async def create_missing_constraints(self) -> None:
        """
        Creates missing unique constraints and indexes for proper UPSERT operations.
        
        This method is idempotent and safe to run multiple times.
        """
        try:
            validation_result = await self.validate_constraints()
            
            if validation_result.is_valid:
                self.logger.info("All required constraints already exist")
                return
                
            async with async_session_maker() as session:
                for constraint_def in validation_result.missing_constraints:
                    await self._create_constraint(session, constraint_def)
                    
                await session.commit()
                self.logger.info(f"Created {len(validation_result.missing_constraints)} missing constraints")
                
        except Exception as e:
            self.logger.error(f"Failed to create missing constraints: {e}")
            raise
    
    async def get_constraint_info(self, table_name: str) -> List[ConstraintInfo]:
        """
        Returns constraint information for a specific table.
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            List of ConstraintInfo objects for the table
        """
        try:
            async with async_session_maker() as session:
                constraints = await self._get_table_constraints(session, table_name)
                self.logger.debug(f"Found {len(constraints)} constraints for table {table_name}")
                return constraints
                
        except Exception as e:
            self.logger.error(f"Failed to get constraint info for table {table_name}: {e}")
            return []
    
    async def _get_all_constraints(self, session: AsyncSession) -> List[ConstraintInfo]:
        """Get all constraints from the database"""
        constraints = []
        
        # Get unique constraints
        query = text("""
            SELECT 
                tc.constraint_name,
                tc.table_name,
                string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
                AND tc.table_schema = 'public'
            GROUP BY tc.constraint_name, tc.table_name
        """)
        
        result = await session.execute(query)
        for row in result:
            constraints.append(ConstraintInfo(
                name=row.constraint_name,
                type='UNIQUE',
                columns=row.columns.split(','),
                table_name=row.table_name
            ))
        
        return constraints
    
    async def _get_table_constraints(self, session: AsyncSession, table_name: str) -> List[ConstraintInfo]:
        """Get constraints for a specific table"""
        query = text("""
            SELECT 
                tc.constraint_name,
                tc.constraint_type,
                string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = :table_name
                AND tc.table_schema = 'public'
                AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
            GROUP BY tc.constraint_name, tc.constraint_type
        """)
        
        result = await session.execute(query, {"table_name": table_name})
        constraints = []
        
        for row in result:
            constraints.append(ConstraintInfo(
                name=row.constraint_name,
                type=row.constraint_type,
                columns=row.columns.split(','),
                table_name=table_name
            ))
        
        return constraints
    
    async def _check_required_constraints(self, session: AsyncSession, existing_constraints: List[ConstraintInfo]) -> List[Dict[str, Any]]:
        """Check for required constraints that are missing"""
        missing = []
        
        # Required constraint: telegram_messages (telegram_id, group_id)
        telegram_messages_constraint = self._find_constraint(
            existing_constraints, 
            'telegram_messages', 
            ['telegram_id', 'group_id']
        )
        
        if not telegram_messages_constraint:
            missing.append({
                'table': 'telegram_messages',
                'columns': ['telegram_id', 'group_id'],
                'name': 'uq_telegram_messages_telegram_id_group_id',
                'type': 'UNIQUE'
            })
        
        # Check if telegram_users has unique constraint on telegram_id (should already exist)
        telegram_users_constraint = self._find_constraint(
            existing_constraints,
            'telegram_users',
            ['telegram_id']
        )
        
        if not telegram_users_constraint:
            self.logger.warning("telegram_users.telegram_id unique constraint not found - this should exist")
        
        return missing
    
    def _find_constraint(self, constraints: List[ConstraintInfo], table_name: str, columns: List[str]) -> Optional[ConstraintInfo]:
        """Find a constraint by table and columns"""
        for constraint in constraints:
            if (constraint.table_name == table_name and 
                set(constraint.columns) == set(columns)):
                return constraint
        return None
    
    async def _create_constraint(self, session: AsyncSession, constraint_def: Dict[str, Any]) -> None:
        """Create a single constraint"""
        table = constraint_def['table']
        columns = constraint_def['columns']
        name = constraint_def['name']
        constraint_type = constraint_def['type']
        
        if constraint_type == 'UNIQUE':
            columns_str = ', '.join(columns)
            sql = f"ALTER TABLE {table} ADD CONSTRAINT {name} UNIQUE ({columns_str})"
            
            try:
                await session.execute(text(sql))
                self.logger.info(f"Created unique constraint {name} on {table}({columns_str})")
            except SQLAlchemyError as e:
                if "already exists" in str(e).lower():
                    self.logger.info(f"Constraint {name} already exists, skipping")
                else:
                    # Enhanced constraint violation logging
                    self.logger.error(
                        f"Failed to create constraint {name} on {table}({columns_str}). "
                        f"Error: {e}. SQL: {sql}. "
                        f"Constraint definition: {constraint_def}"
                    )
                    raise
    
    async def create_performance_indexes(self) -> None:
        """Create performance indexes for common query patterns"""
        indexes = [
            {
                'name': 'idx_telegram_messages_timestamp',
                'table': 'telegram_messages',
                'columns': ['date'],
                'type': 'btree'
            },
            {
                'name': 'idx_telegram_messages_sender_id',
                'table': 'telegram_messages', 
                'columns': ['sender_id'],
                'type': 'btree'
            },
            {
                'name': 'idx_telegram_messages_group_id',
                'table': 'telegram_messages',
                'columns': ['group_id'],
                'type': 'btree'
            }
        ]
        
        try:
            async with async_session_maker() as session:
                for index_def in indexes:
                    await self._create_index_if_not_exists(session, index_def)
                await session.commit()
                self.logger.info(f"Created {len(indexes)} performance indexes")
                
        except Exception as e:
            self.logger.error(f"Failed to create performance indexes: {e}")
            raise
    
    async def _create_index_if_not_exists(self, session: AsyncSession, index_def: Dict[str, Any]) -> None:
        """Create an index if it doesn't exist"""
        name = index_def['name']
        table = index_def['table']
        columns = index_def['columns']
        
        columns_str = ', '.join(columns)
        sql = f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({columns_str})"
        
        try:
            await session.execute(text(sql))
            self.logger.debug(f"Created index {name} on {table}({columns_str})")
        except SQLAlchemyError as e:
            self.logger.warning(f"Failed to create index {name}: {e}")


# Global instance
database_manager = DatabaseManager()