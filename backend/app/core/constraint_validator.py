"""
Constraint Validator Component

This component provides data consistency validation and referential integrity
checking before transaction commits to prevent constraint violations.

Requirements: 8.1, 8.2, 8.3, 8.4
"""

import logging
from typing import List, Dict, Any, Optional, NamedTuple, Set
from datetime import datetime
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from backend.app.core.session_manager import session_manager
from backend.app.core.logging_config import get_logger

logger = get_logger("constraint_validator")


class ValidationViolation(NamedTuple):
    """Information about a validation violation"""
    violation_type: str
    table_name: str
    column_name: str
    violating_value: Any
    expected_constraint: str
    description: str
    severity: str  # 'error', 'warning', 'info'


class ReferentialIntegrityViolation(NamedTuple):
    """Information about referential integrity violations"""
    child_table: str
    child_column: str
    child_value: Any
    parent_table: str
    parent_column: str
    violation_count: int
    sample_records: List[Dict[str, Any]]


class BatchValidationResult(NamedTuple):
    """Result of batch operation validation"""
    is_valid: bool
    violations: List[ValidationViolation]
    referential_violations: List[ReferentialIntegrityViolation]
    total_records_checked: int
    validation_time_ms: float


class ConstraintValidator:
    """
    Validates data consistency and referential integrity before transaction commits.
    
    This component checks for constraint violations, referential integrity issues,
    and data consistency problems before they cause database errors.
    """
    
    def __init__(self):
        self.logger = logger
        
        # Define referential integrity rules
        self._referential_rules = {
            'telegram_messages': {
                'sender_id': ('telegram_users', 'id'),
                'group_id': ('telegram_groups', 'id'),
                'reply_to_msg_id': ('telegram_messages', 'telegram_id'),
                'forward_from_id': ('telegram_users', 'telegram_id')
            },
            'telegram_users': {
                # Users table is typically a root table with no foreign keys
            },
            'telegram_groups': {
                # Groups table is typically a root table with no foreign keys
            }
        }
        
        # Define constraint rules
        self._constraint_rules = {
            'telegram_messages': {
                'telegram_id': {'not_null': True, 'min_value': 1},
                'group_id': {'not_null': True, 'min_value': 1},
                'text': {'max_length': 10000},
                'date': {'not_null': True}
            },
            'telegram_users': {
                'telegram_id': {'not_null': True, 'min_value': 1},
                'username': {'max_length': 255},
                'first_name': {'max_length': 255},
                'last_name': {'max_length': 255}
            },
            'telegram_groups': {
                'telegram_id': {'not_null': True},
                'title': {'max_length': 255}
            }
        }
    
    async def validate_referential_integrity(self, table_name: str, 
                                           records: List[Dict[str, Any]]) -> List[ReferentialIntegrityViolation]:
        """
        Validates referential integrity for a batch of records.
        
        Args:
            table_name: Name of the table being validated
            records: List of record dictionaries to validate
            
        Returns:
            List of ReferentialIntegrityViolation objects
        """
        violations = []
        
        if table_name not in self._referential_rules:
            self.logger.debug(f"No referential rules defined for table {table_name}")
            return violations
        
        rules = self._referential_rules[table_name]
        
        for column, (parent_table, parent_column) in rules.items():
            # Get all foreign key values from the records
            fk_values = []
            for record in records:
                if column in record and record[column] is not None:
                    fk_values.append(record[column])
            
            if not fk_values:
                continue
            
            # Check if all foreign key values exist in parent table
            missing_values = await self._check_foreign_key_existence(
                parent_table, parent_column, fk_values
            )
            
            if missing_values:
                # Find sample records with violations
                sample_records = []
                for record in records[:5]:  # Limit to 5 samples
                    if column in record and record[column] in missing_values:
                        sample_records.append({
                            'record_id': record.get('id', 'unknown'),
                            column: record[column],
                            'sample_data': {k: v for k, v in record.items() if k in ['id', 'telegram_id', 'title', 'username']}
                        })
                
                violations.append(ReferentialIntegrityViolation(
                    child_table=table_name,
                    child_column=column,
                    child_value=missing_values,
                    parent_table=parent_table,
                    parent_column=parent_column,
                    violation_count=len(missing_values),
                    sample_records=sample_records
                ))
                
                self.logger.warning(
                    f"Referential integrity violation: {table_name}.{column} -> {parent_table}.{parent_column}. "
                    f"Missing values: {missing_values[:10]}{'...' if len(missing_values) > 10 else ''}"
                )
        
        return violations
    
    async def validate_data_consistency(self, table_name: str, 
                                      records: List[Dict[str, Any]]) -> List[ValidationViolation]:
        """
        Validates data consistency for a batch of records.
        
        Args:
            table_name: Name of the table being validated
            records: List of record dictionaries to validate
            
        Returns:
            List of ValidationViolation objects
        """
        violations = []
        
        if table_name not in self._constraint_rules:
            self.logger.debug(f"No constraint rules defined for table {table_name}")
            return violations
        
        rules = self._constraint_rules[table_name]
        
        for record in records:
            for column, constraints in rules.items():
                value = record.get(column)
                
                # Check not_null constraint
                if constraints.get('not_null', False) and value is None:
                    violations.append(ValidationViolation(
                        violation_type='not_null',
                        table_name=table_name,
                        column_name=column,
                        violating_value=value,
                        expected_constraint='NOT NULL',
                        description=f"Column {column} cannot be null",
                        severity='error'
                    ))
                
                # Check min_value constraint
                if 'min_value' in constraints and value is not None:
                    min_val = constraints['min_value']
                    if isinstance(value, (int, float)) and value < min_val:
                        violations.append(ValidationViolation(
                            violation_type='min_value',
                            table_name=table_name,
                            column_name=column,
                            violating_value=value,
                            expected_constraint=f'>= {min_val}',
                            description=f"Column {column} value {value} is below minimum {min_val}",
                            severity='error'
                        ))
                
                # Check max_length constraint
                if 'max_length' in constraints and value is not None:
                    max_len = constraints['max_length']
                    if isinstance(value, str) and len(value) > max_len:
                        violations.append(ValidationViolation(
                            violation_type='max_length',
                            table_name=table_name,
                            column_name=column,
                            violating_value=f"{value[:50]}..." if len(value) > 50 else value,
                            expected_constraint=f'<= {max_len} characters',
                            description=f"Column {column} length {len(value)} exceeds maximum {max_len}",
                            severity='error'
                        ))
        
        return violations
    
    async def validate_batch_operation(self, table_name: str, 
                                     records: List[Dict[str, Any]]) -> BatchValidationResult:
        """
        Validates a complete batch operation for consistency and integrity.
        
        Args:
            table_name: Name of the table for the batch operation
            records: List of record dictionaries to validate
            
        Returns:
            BatchValidationResult with validation outcome
        """
        start_time = datetime.now()
        
        try:
            # Validate data consistency
            consistency_violations = await self.validate_data_consistency(table_name, records)
            
            # Validate referential integrity
            referential_violations = await self.validate_referential_integrity(table_name, records)
            
            # Calculate validation time
            end_time = datetime.now()
            validation_time_ms = (end_time - start_time).total_seconds() * 1000
            
            is_valid = len(consistency_violations) == 0 and len(referential_violations) == 0
            
            self.logger.info(
                f"Batch validation completed for {table_name}. "
                f"Records: {len(records)}, "
                f"Valid: {is_valid}, "
                f"Consistency violations: {len(consistency_violations)}, "
                f"Referential violations: {len(referential_violations)}, "
                f"Time: {validation_time_ms:.2f}ms"
            )
            
            return BatchValidationResult(
                is_valid=is_valid,
                violations=consistency_violations,
                referential_violations=referential_violations,
                total_records_checked=len(records),
                validation_time_ms=validation_time_ms
            )
            
        except Exception as e:
            end_time = datetime.now()
            validation_time_ms = (end_time - start_time).total_seconds() * 1000
            
            self.logger.error(f"Batch validation failed for {table_name}: {e}")
            
            return BatchValidationResult(
                is_valid=False,
                violations=[ValidationViolation(
                    violation_type='validation_error',
                    table_name=table_name,
                    column_name='unknown',
                    violating_value=str(e),
                    expected_constraint='validation_success',
                    description=f"Validation process failed: {e}",
                    severity='error'
                )],
                referential_violations=[],
                total_records_checked=len(records),
                validation_time_ms=validation_time_ms
            )
    
    async def _check_foreign_key_existence(self, parent_table: str, parent_column: str, 
                                         fk_values: List[Any]) -> List[Any]:
        """
        Checks which foreign key values don't exist in the parent table.
        
        Args:
            parent_table: Name of the parent table
            parent_column: Name of the parent column
            fk_values: List of foreign key values to check
            
        Returns:
            List of missing foreign key values
        """
        if not fk_values:
            return []
        
        async def _check_existence_operation(session: AsyncSession) -> List[Any]:
            # Create a query to check which values exist
            placeholders = ', '.join([f':val_{i}' for i in range(len(fk_values))])
            query = text(f"""
                SELECT {parent_column} 
                FROM {parent_table} 
                WHERE {parent_column} IN ({placeholders})
            """)
            
            # Create parameters dictionary
            params = {f'val_{i}': val for i, val in enumerate(fk_values)}
            
            result = await session.execute(query, params)
            existing_values = {row[0] for row in result}
            
            # Return values that don't exist
            missing_values = [val for val in fk_values if val not in existing_values]
            return missing_values
        
        try:
            return await session_manager.execute_with_retry(_check_existence_operation)
        except Exception as e:
            self.logger.error(f"Error checking foreign key existence: {e}")
            return fk_values  # Assume all are missing on error
    
    async def validate_unique_constraints(self, table_name: str, 
                                        records: List[Dict[str, Any]]) -> List[ValidationViolation]:
        """
        Validates unique constraints for a batch of records.
        
        Args:
            table_name: Name of the table being validated
            records: List of record dictionaries to validate
            
        Returns:
            List of ValidationViolation objects for unique constraint violations
        """
        violations = []
        
        # Define unique constraints for each table
        unique_constraints = {
            'telegram_messages': [
                ['telegram_id', 'group_id']  # Composite unique constraint
            ],
            'telegram_users': [
                ['telegram_id']  # Single column unique constraint
            ],
            'telegram_groups': [
                ['telegram_id']  # Single column unique constraint
            ]
        }
        
        if table_name not in unique_constraints:
            return violations
        
        constraints = unique_constraints[table_name]
        
        for constraint_columns in constraints:
            # Check for duplicates within the batch
            seen_values = set()
            for record in records:
                # Create a tuple of values for the constraint columns
                constraint_values = tuple(record.get(col) for col in constraint_columns)
                
                # Skip if any value is None (handled by not_null validation)
                if any(val is None for val in constraint_values):
                    continue
                
                if constraint_values in seen_values:
                    violations.append(ValidationViolation(
                        violation_type='unique_constraint',
                        table_name=table_name,
                        column_name=', '.join(constraint_columns),
                        violating_value=constraint_values,
                        expected_constraint=f'UNIQUE({", ".join(constraint_columns)})',
                        description=f"Duplicate values found in batch for unique constraint",
                        severity='error'
                    ))
                else:
                    seen_values.add(constraint_values)
            
            # Check for duplicates against existing database records
            if seen_values:
                existing_duplicates = await self._check_unique_constraint_violations(
                    table_name, constraint_columns, list(seen_values)
                )
                
                for duplicate_value in existing_duplicates:
                    violations.append(ValidationViolation(
                        violation_type='unique_constraint',
                        table_name=table_name,
                        column_name=', '.join(constraint_columns),
                        violating_value=duplicate_value,
                        expected_constraint=f'UNIQUE({", ".join(constraint_columns)})',
                        description=f"Value already exists in database",
                        severity='error'
                    ))
        
        return violations
    
    async def _check_unique_constraint_violations(self, table_name: str, 
                                                constraint_columns: List[str], 
                                                values_to_check: List[tuple]) -> List[tuple]:
        """
        Checks which values violate unique constraints in the database.
        
        Args:
            table_name: Name of the table
            constraint_columns: List of column names in the constraint
            values_to_check: List of value tuples to check
            
        Returns:
            List of value tuples that already exist in the database
        """
        if not values_to_check:
            return []
        
        async def _check_violations_operation(session: AsyncSession) -> List[tuple]:
            violations = []
            
            for value_tuple in values_to_check:
                # Build WHERE clause for the constraint columns
                where_conditions = []
                params = {}
                
                for i, (column, value) in enumerate(zip(constraint_columns, value_tuple)):
                    param_name = f'val_{i}'
                    where_conditions.append(f"{column} = :{param_name}")
                    params[param_name] = value
                
                where_clause = ' AND '.join(where_conditions)
                query = text(f"SELECT 1 FROM {table_name} WHERE {where_clause} LIMIT 1")
                
                result = await session.execute(query, params)
                if result.fetchone():
                    violations.append(value_tuple)
            
            return violations
        
        try:
            return await session_manager.execute_with_retry(_check_violations_operation)
        except Exception as e:
            self.logger.error(f"Error checking unique constraint violations: {e}")
            return []  # Assume no violations on error
    
    def get_validation_summary(self, results: List[BatchValidationResult]) -> Dict[str, Any]:
        """
        Creates a summary of validation results across multiple batches.
        
        Args:
            results: List of BatchValidationResult objects
            
        Returns:
            Dictionary with validation summary statistics
        """
        total_records = sum(r.total_records_checked for r in results)
        total_violations = sum(len(r.violations) for r in results)
        total_referential_violations = sum(len(r.referential_violations) for r in results)
        total_time_ms = sum(r.validation_time_ms for r in results)
        
        valid_batches = sum(1 for r in results if r.is_valid)
        invalid_batches = len(results) - valid_batches
        
        # Group violations by type
        violation_types = {}
        for result in results:
            for violation in result.violations:
                violation_types[violation.violation_type] = violation_types.get(violation.violation_type, 0) + 1
        
        return {
            'total_batches': len(results),
            'valid_batches': valid_batches,
            'invalid_batches': invalid_batches,
            'total_records_checked': total_records,
            'total_violations': total_violations,
            'total_referential_violations': total_referential_violations,
            'total_validation_time_ms': total_time_ms,
            'average_time_per_batch_ms': total_time_ms / len(results) if results else 0,
            'violation_types': violation_types,
            'validation_success_rate': valid_batches / len(results) if results else 0
        }


# Global instance
constraint_validator = ConstraintValidator()