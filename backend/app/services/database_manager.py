"""
Database Manager Component

This component handles database schema validation, constraint management,
and ensures proper UPSERT operations work correctly by validating that
all ON CONFLICT clauses have corresponding unique constraints.

Requirements: 1.1, 1.2, 1.3
"""

import logging
from typing import List, Dict, Any, Optional, NamedTuple
from sqlalchemy import text, inspect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.engine import Inspector
from backend.app.db.database import engine

logger = logging.getLogger(__name__)


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
    Manages database schema validation and constraint enforcement.
    
    This component ensures that all ON CONFLICT clauses used in UPSERT operations
    have corresponding unique constraints or unique indexes in the database.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def validate_constraints(self) -> ConstraintValidationResult:
        """
        Validates all database constraints match UPSERT operations.
        
        Checks that required unique constraints exist for:
        - telegram_messages (telegram_id, group_id)
        - telegram_users (telegram_id)
        
        Returns:
            ConstraintValidationResult with validation status and details
        """
        try:
            async with engine.begin() as conn:
                # Get constraint information
                existing_constraints = await self._get_all_constraints(conn)
                
                # Define required constraints for UPSERT operations
                required_constraints = [
                    {
                        'table': 'telegram_messages',
                        'columns': ['telegram_id', 'group_id'],
                        'name': 'uq_telegram_messages_telegram_id_group_id'
                    },
                    {
                        'table': 'telegram_users', 
                        'columns': ['telegram_id'],
                        'name': 'telegram_users_telegram_id_key'
                    }
                ]
                
                # Check for missing constraints
                missing_constraints = []
                errors = []
                
                for required in required_constraints:
                    if not self._constraint_exists(existing_constraints, required):
                        missing_constraints.append(required)
                        self.logger.warning(
                            f"Missing constraint on {required['table']} "
                            f"for columns {required['columns']}"
                        )
                
                is_valid = len(missing_constraints) == 0
                
                return ConstraintValidationResult(
                    is_valid=is_valid,
                    missing_constraints=missing_constraints,
                    existing_constraints=existing_constraints,
                    errors=errors
                )
                
        except Exception as e:
            self.logger.error(f"Error validating constraints: {e}")
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
            
            async with engine.begin() as conn:
                for constraint in validation_result.missing_constraints:
                    await self._create_constraint(conn, constraint)
                    
            self.logger.info("Successfully created missing constraints")
            
        except Exception as e:
            self.logger.error(f"Error creating missing constraints: {e}")
            raise
    
    async def get_constraint_info(self, table_name: str) -> List[ConstraintInfo]:
        """
        Returns constraint information for a specific table.
        
        Args:
            table_name: Name of the table to get constraints for
            
        Returns:
            List of ConstraintInfo objects for the table
        """
        try:
            async with engine.begin() as conn:
                constraints = await self._get_table_constraints(conn, table_name)
                return constraints
                
        except Exception as e:
            self.logger.error(f"Error getting constraint info for {table_name}: {e}")
            return []
    
    async def _get_all_constraints(self, conn) -> List[ConstraintInfo]:
        """Get all constraints from the database"""
        constraints = []
        
        # Get unique constraints
        unique_query = text("""
            SELECT 
                tc.constraint_name,
                tc.table_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
                AND tc.table_schema = 'public'
            GROUP BY tc.constraint_name, tc.table_name
        """)
        
        result = await conn.execute(unique_query)
        for row in result:
            constraints.append(ConstraintInfo(
                name=row.constraint_name,
                type='UNIQUE',
                columns=row.columns,
                table_name=row.table_name
            ))
        
        # Get unique indexes (which also serve as unique constraints)
        index_query = text("""
            SELECT 
                i.indexname as constraint_name,
                i.tablename as table_name,
                array_agg(a.attname ORDER BY a.attnum) as columns
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.tablename
            JOIN pg_index idx ON idx.indexrelid = (
                SELECT oid FROM pg_class WHERE relname = i.indexname
            )
            JOIN pg_attribute a ON a.attrelid = c.oid 
                AND a.attnum = ANY(idx.indkey)
            WHERE i.schemaname = 'public'
                AND idx.indisunique = true
            GROUP BY i.indexname, i.tablename
        """)
        
        result = await conn.execute(index_query)
        for row in result:
            constraints.append(ConstraintInfo(
                name=row.constraint_name,
                type='UNIQUE_INDEX',
                columns=row.columns,
                table_name=row.table_name
            ))
        
        return constraints
    
    async def _get_table_constraints(self, conn, table_name: str) -> List[ConstraintInfo]:
        """Get constraints for a specific table"""
        all_constraints = await self._get_all_constraints(conn)
        return [c for c in all_constraints if c.table_name == table_name]
    
    def _constraint_exists(self, existing_constraints: List[ConstraintInfo], 
                          required: Dict[str, Any]) -> bool:
        """Check if a required constraint exists in the list of existing constraints"""
        for constraint in existing_constraints:
            if (constraint.table_name == required['table'] and 
                set(constraint.columns) == set(required['columns'])):
                return True
        return False
    
    async def _create_constraint(self, conn, constraint_info: Dict[str, Any]) -> None:
        """Create a missing constraint"""
        table = constraint_info['table']
        columns = constraint_info['columns']
        name = constraint_info['name']
        
        # Create unique constraint using ALTER TABLE
        columns_str = ', '.join(columns)
        
        # Use IF NOT EXISTS for idempotent operation
        create_query = text(f"""
            ALTER TABLE {table} 
            ADD CONSTRAINT {name} 
            UNIQUE ({columns_str})
        """)
        
        try:
            await conn.execute(create_query)
            self.logger.info(f"Created constraint {name} on {table}({columns_str})")
        except Exception as e:
            # Check if constraint already exists (race condition)
            if "already exists" in str(e).lower():
                self.logger.info(f"Constraint {name} already exists")
            else:
                self.logger.error(f"Failed to create constraint {name}: {e}")
                raise
    
    async def validate_upsert_compatibility(self, table_name: str, 
                                          conflict_columns: List[str]) -> bool:
        """
        Validates that a table has the necessary constraints for UPSERT operations.
        
        Args:
            table_name: Name of the table
            conflict_columns: Columns used in ON CONFLICT clause
            
        Returns:
            True if compatible constraints exist, False otherwise
        """
        try:
            constraints = await self.get_constraint_info(table_name)
            
            for constraint in constraints:
                if set(constraint.columns) == set(conflict_columns):
                    return True
            
            self.logger.warning(
                f"No unique constraint found for {table_name} "
                f"on columns {conflict_columns}"
            )
            return False
            
        except Exception as e:
            self.logger.error(
                f"Error validating UPSERT compatibility for {table_name}: {e}"
            )
            return False