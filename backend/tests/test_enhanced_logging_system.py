"""
Tests for EnhancedLoggingSystem improvements.

This test file validates the new methods added to EnhancedLoggingSystem:
- log_with_context()
- log_operation_start()
- log_operation_end()
- log_metrics()
"""

import pytest
import asyncio
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem, LogLevel


@pytest.fixture
async def logging_system():
    """Create a temporary logging system for testing."""
    # Create temporary directory for logs
    temp_dir = tempfile.mkdtemp()
    
    # Create logging system
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    yield logger
    
    # Cleanup
    await logger.shutdown()
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_log_with_context_basic():
    """Test basic log_with_context functionality."""
    temp_dir = tempfile.mkdtemp()
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    try:
        # Log with context
        await logger.log_with_context(
            "INFO",
            "Test message",
            "TestService",
            context={"key1": "value1", "key2": 123}
        )
        
        # Verify log was created
        log_file = Path(temp_dir) / "info.log"
        assert log_file.exists()
        
        # Read and verify log content
        with open(log_file, 'r') as f:
            lines = f.readlines()
            # Skip the header line
            log_lines = [line for line in lines if line.strip() and not line.startswith('#')]
            assert len(log_lines) >= 1
            
            # Parse the last log entry
            last_log = json.loads(log_lines[-1])
            assert last_log['level'] == 'INFO'
            assert last_log['component'] == 'TestService'
            assert last_log['message'] == 'Test message'
            assert 'details' in last_log
            assert last_log['details']['key1'] == 'value1'
            assert last_log['details']['key2'] == 123
            assert 'timestamp' in last_log
            
    finally:
        await logger.shutdown()
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_log_with_context_with_error():
    """Test log_with_context with error and stack trace."""
    temp_dir = tempfile.mkdtemp()
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    try:
        # Create an exception
        try:
            raise ValueError("Test error")
        except ValueError as e:
            # Log with error
            await logger.log_with_context(
                "ERROR",
                "Operation failed",
                "TestService",
                context={"operation": "test_op"},
                error=e
            )
        
        # Verify log was created with stack trace
        log_file = Path(temp_dir) / "error.log"
        assert log_file.exists()
        
        # Read and verify log content
        with open(log_file, 'r') as f:
            lines = f.readlines()
            log_lines = [line for line in lines if line.strip() and not line.startswith('#')]
            assert len(log_lines) >= 1
            
            # Parse the last log entry
            last_log = json.loads(log_lines[-1])
            assert last_log['level'] == 'ERROR'
            assert last_log['component'] == 'TestService'
            assert 'details' in last_log
            assert last_log['details']['error_type'] == 'ValueError'
            assert last_log['details']['error_message'] == 'Test error'
            assert 'stack_trace' in last_log
            assert 'ValueError: Test error' in last_log['stack_trace']
            
    finally:
        await logger.shutdown()
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_log_operation_start_and_end():
    """Test operation tracking with start and end."""
    temp_dir = tempfile.mkdtemp()
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    try:
        # Start operation
        op_id = await logger.log_operation_start(
            "test_operation",
            "TestService",
            context={"param1": "value1"}
        )
        
        # Verify operation ID is returned
        assert op_id is not None
        assert len(op_id) > 0
        
        # Simulate some work
        await asyncio.sleep(0.1)
        
        # End operation successfully
        await logger.log_operation_end(
            op_id,
            success=True,
            context={"result": "success", "items_processed": 10}
        )
        
        # Verify logs were created
        log_file = Path(temp_dir) / "info.log"
        assert log_file.exists()
        
        # Read and verify log content
        with open(log_file, 'r') as f:
            lines = f.readlines()
            log_lines = [line for line in lines if line.strip() and not line.startswith('#')]
            
            # Should have at least 2 logs (start and end)
            assert len(log_lines) >= 2
            
            # Parse logs
            logs = [json.loads(line) for line in log_lines]
            
            # Find operation start log
            start_logs = [log for log in logs if 'Operation started' in log.get('message', '')]
            assert len(start_logs) >= 1
            start_log = start_logs[-1]
            assert start_log['details']['operation_id'] == op_id
            assert start_log['details']['operation_status'] == 'started'
            
            # Find operation end log
            end_logs = [log for log in logs if 'completed successfully' in log.get('message', '')]
            assert len(end_logs) >= 1
            end_log = end_logs[-1]
            assert end_log['details']['operation_id'] == op_id
            assert end_log['details']['operation_status'] == 'success'
            assert end_log['details']['success'] is True
            
    finally:
        await logger.shutdown()
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_log_operation_end_with_failure():
    """Test operation tracking with failure."""
    temp_dir = tempfile.mkdtemp()
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    try:
        # Start operation
        op_id = await logger.log_operation_start(
            "failing_operation",
            "TestService"
        )
        
        # Create an exception
        try:
            raise RuntimeError("Operation failed")
        except RuntimeError as e:
            # End operation with failure
            await logger.log_operation_end(
                op_id,
                success=False,
                context={"error_details": "Something went wrong"},
                error=e
            )
        
        # Verify error log was created
        log_file = Path(temp_dir) / "error.log"
        assert log_file.exists()
        
        # Read and verify log content
        with open(log_file, 'r') as f:
            lines = f.readlines()
            log_lines = [line for line in lines if line.strip() and not line.startswith('#')]
            assert len(log_lines) >= 1
            
            # Parse the last log entry
            last_log = json.loads(log_lines[-1])
            assert last_log['level'] == 'ERROR'
            assert 'failed' in last_log['message']
            assert last_log['details']['operation_id'] == op_id
            assert last_log['details']['operation_status'] == 'failed'
            assert last_log['details']['success'] is False
            assert last_log['details']['error_type'] == 'RuntimeError'
            assert 'stack_trace' in last_log
            
    finally:
        await logger.shutdown()
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_log_metrics():
    """Test metrics logging."""
    temp_dir = tempfile.mkdtemp()
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    try:
        # Log metrics
        await logger.log_metrics(
            "MediaRetryService",
            {
                "total_processed": 100,
                "successful": 95,
                "failed": 5,
                "success_rate": 0.95,
                "average_time_ms": 234.5
            }
        )
        
        # Verify log was created
        log_file = Path(temp_dir) / "info.log"
        assert log_file.exists()
        
        # Read and verify log content
        with open(log_file, 'r') as f:
            lines = f.readlines()
            log_lines = [line for line in lines if line.strip() and not line.startswith('#')]
            assert len(log_lines) >= 1
            
            # Parse the last log entry
            last_log = json.loads(log_lines[-1])
            assert last_log['level'] == 'INFO'
            assert last_log['component'] == 'MediaRetryService'
            assert last_log['operation'] == 'metrics'
            assert 'Metrics report' in last_log['message']
            assert 'details' in last_log
            assert last_log['details']['total_processed'] == 100
            assert last_log['details']['successful'] == 95
            assert last_log['details']['failed'] == 5
            assert last_log['details']['success_rate'] == 0.95
            assert last_log['details']['average_time_ms'] == 234.5
            assert 'metrics_timestamp' in last_log['details']
            
    finally:
        await logger.shutdown()
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_log_with_context_all_levels():
    """Test log_with_context with different log levels."""
    temp_dir = tempfile.mkdtemp()
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    try:
        # Test different log levels
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        
        for level in levels:
            await logger.log_with_context(
                level,
                f"Test {level} message",
                "TestService",
                context={"level": level}
            )
        
        # Verify logs were created for each level
        for level in levels:
            log_file = Path(temp_dir) / f"{level.lower()}.log"
            assert log_file.exists(), f"Log file for {level} not found"
            
    finally:
        await logger.shutdown()
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_structured_log_format():
    """Test that all logs include required fields: timestamp, level, service, context, message."""
    temp_dir = tempfile.mkdtemp()
    logger = EnhancedLoggingSystem(log_dir=temp_dir)
    await logger.initialize()
    
    try:
        # Log with context
        await logger.log_with_context(
            "INFO",
            "Structured log test",
            "TestService",
            context={"test_key": "test_value"}
        )
        
        # Read log file
        log_file = Path(temp_dir) / "info.log"
        with open(log_file, 'r') as f:
            lines = f.readlines()
            log_lines = [line for line in lines if line.strip() and not line.startswith('#')]
            
            # Parse the last log entry
            last_log = json.loads(log_lines[-1])
            
            # Verify all required fields are present
            assert 'timestamp' in last_log, "timestamp field missing"
            assert 'level' in last_log, "level field missing"
            assert 'component' in last_log, "component (service) field missing"
            assert 'message' in last_log, "message field missing"
            assert 'details' in last_log, "details (context) field missing"
            
            # Verify timestamp is valid ISO format
            datetime.fromisoformat(last_log['timestamp'])
            
    finally:
        await logger.shutdown()
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
