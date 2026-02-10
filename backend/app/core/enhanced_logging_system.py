import asyncio
import logging
import json
import time
import traceback
import psutil
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Callable
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque
import aiofiles
import aiofiles.os
from concurrent.futures import ThreadPoolExecutor


class LogLevel(Enum):
    """Enhanced log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    PERFORMANCE = "PERFORMANCE"
    SECURITY = "SECURITY"
    AUDIT = "AUDIT"


class AlertSeverity(Enum):
    """Alert severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class LogEntry:
    """Enhanced log entry structure"""
    timestamp: datetime
    level: LogLevel
    component: str
    operation: str
    message: str
    details: Optional[Dict[str, Any]] = None
    user_id: Optional[int] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    duration_ms: Optional[float] = None
    error_code: Optional[str] = None
    stack_trace: Optional[str] = None
    performance_metrics: Optional[Dict[str, Any]] = None


@dataclass
class PerformanceMetrics:
    """Performance metrics structure"""
    operation: str
    duration_ms: float
    memory_usage_mb: float
    cpu_percent: float
    disk_io_read_mb: float
    disk_io_write_mb: float
    network_sent_mb: float
    network_recv_mb: float
    timestamp: datetime


@dataclass
class ErrorPattern:
    """Error pattern detection structure"""
    pattern_id: str
    error_type: str
    message_pattern: str
    component: str
    count: int
    first_occurrence: datetime
    last_occurrence: datetime
    severity: AlertSeverity
    suggested_action: str


@dataclass
class Alert:
    """Alert structure"""
    alert_id: str
    severity: AlertSeverity
    title: str
    description: str
    component: str
    timestamp: datetime
    details: Dict[str, Any]
    suggested_actions: List[str]
    is_resolved: bool = False
    resolved_at: Optional[datetime] = None


class EnhancedLoggingSystem:
    """
    Comprehensive logging and monitoring system for TelegramVault.
    
    Features:
    - Detailed operation logging with structured data
    - Error pattern detection and analysis
    - Performance metrics collection and monitoring
    - Diagnostic information generation
    - Real-time alerting system
    - Log aggregation and analysis
    """
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.executor = ThreadPoolExecutor(max_workers=2)
        
        # Logging configuration
        self._log_files = {
            LogLevel.DEBUG: "debug.log",
            LogLevel.INFO: "info.log",
            LogLevel.WARNING: "warning.log",
            LogLevel.ERROR: "error.log",
            LogLevel.CRITICAL: "critical.log",
            LogLevel.PERFORMANCE: "performance.log",
            LogLevel.SECURITY: "security.log",
            LogLevel.AUDIT: "audit.log"
        }
        
        # In-memory storage for real-time analysis
        self._recent_logs: deque = deque(maxlen=10000)
        self._error_patterns: Dict[str, ErrorPattern] = {}
        self._performance_metrics: deque = deque(maxlen=1000)
        self._active_alerts: Dict[str, Alert] = {}
        
        # Pattern detection settings
        self._pattern_detection_enabled = True
        self._pattern_threshold = 5  # Minimum occurrences to create pattern
        self._pattern_time_window = timedelta(minutes=30)
        
        # Performance monitoring settings
        self._performance_monitoring_enabled = True
        self._performance_threshold_ms = 5000  # Alert if operation takes > 5s
        self._memory_threshold_mb = 1000  # Alert if memory usage > 1GB
        
        # Alert settings
        self._alert_cooldown = timedelta(minutes=15)  # Minimum time between similar alerts
        self._alert_callbacks: List[Callable] = []
        
        # Statistics
        self._stats = {
            "total_logs": 0,
            "logs_by_level": defaultdict(int),
            "logs_by_component": defaultdict(int),
            "error_patterns_detected": 0,
            "alerts_generated": 0,
            "performance_violations": 0
        }
        
        self._initialized = False
        self._monitoring_task: Optional[asyncio.Task] = None
    
    async def initialize(self) -> bool:
        """
        Initialize the enhanced logging system.
        
        Returns:
            bool: True if initialization successful
        """
        try:
            # Create log directory structure
            await aiofiles.os.makedirs(self.log_dir, exist_ok=True)
            
            # Initialize log files
            for log_level, filename in self._log_files.items():
                log_file = self.log_dir / filename
                if not await aiofiles.os.path.exists(log_file):
                    async with aiofiles.open(log_file, 'w') as f:
                        await f.write(f"# {log_level.value} Log - Started at {datetime.now().isoformat()}\n")
            
            # Start monitoring task
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            self._initialized = True
            await self.log_info("EnhancedLoggingSystem", "initialize", "Logging system initialized successfully")
            
            return True
            
        except Exception as e:
            print(f"Failed to initialize EnhancedLoggingSystem: {e}")
            return False
    
    async def log_debug(self, component: str, operation: str, message: str, **kwargs) -> None:
        """Log debug message."""
        await self._log(LogLevel.DEBUG, component, operation, message, **kwargs)
    
    async def log_info(self, component: str, operation: str, message: str, **kwargs) -> None:
        """Log info message."""
        await self._log(LogLevel.INFO, component, operation, message, **kwargs)
    
    async def log_warning(self, component: str, operation: str, message: str, **kwargs) -> None:
        """Log warning message."""
        await self._log(LogLevel.WARNING, component, operation, message, **kwargs)
    
    async def log_error(self, component: str, operation: str, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log error message with optional exception details."""
        details = kwargs.get('details', {})
        
        if error:
            details.update({
                'error_type': type(error).__name__,
                'error_message': str(error)
            })
            kwargs['stack_trace'] = traceback.format_exc()
        
        kwargs['details'] = details
        await self._log(LogLevel.ERROR, component, operation, message, **kwargs)
    
    async def log_critical(self, component: str, operation: str, message: str, error: Optional[Exception] = None, **kwargs) -> None:
        """Log critical message."""
        details = kwargs.get('details', {})
        
        if error:
            details.update({
                'error_type': type(error).__name__,
                'error_message': str(error)
            })
            kwargs['stack_trace'] = traceback.format_exc()
        
        kwargs['details'] = details
        await self._log(LogLevel.CRITICAL, component, operation, message, **kwargs)
        
        # Generate critical alert
        await self._generate_alert(
            AlertSeverity.CRITICAL,
            f"Critical Error in {component}",
            f"Critical error during {operation}: {message}",
            component,
            details
        )
    
    async def log_performance(self, component: str, operation: str, duration_ms: float, **kwargs) -> None:
        """Log performance metrics."""
        # Collect system metrics
        performance_metrics = await self._collect_performance_metrics(operation, duration_ms)
        
        kwargs.update({
            'duration_ms': duration_ms,
            'performance_metrics': asdict(performance_metrics)
        })
        
        message = f"Operation completed in {duration_ms:.2f}ms"
        await self._log(LogLevel.PERFORMANCE, component, operation, message, **kwargs)
        
        # Store performance metrics
        self._performance_metrics.append(performance_metrics)
        
        # Check for performance violations
        if duration_ms > self._performance_threshold_ms:
            self._stats["performance_violations"] += 1
            await self._generate_alert(
                AlertSeverity.HIGH,
                f"Performance Issue in {component}",
                f"Operation {operation} took {duration_ms:.2f}ms (threshold: {self._performance_threshold_ms}ms)",
                component,
                {'duration_ms': duration_ms, 'threshold_ms': self._performance_threshold_ms}
            )
    
    async def log_security(self, component: str, operation: str, message: str, **kwargs) -> None:
        """Log security-related events."""
        await self._log(LogLevel.SECURITY, component, operation, message, **kwargs)
        
        # Generate security alert for high-severity events
        if kwargs.get('severity') == 'high':
            await self._generate_alert(
                AlertSeverity.HIGH,
                f"Security Event in {component}",
                f"Security event during {operation}: {message}",
                component,
                kwargs.get('details', {})
            )
    
    async def log_audit(self, component: str, operation: str, message: str, **kwargs) -> None:
        """Log audit trail events."""
        await self._log(LogLevel.AUDIT, component, operation, message, **kwargs)
    
    async def log_with_context(
        self,
        level: str,
        message: str,
        service: str,
        context: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None
    ) -> None:
        """
        Log a message with structured context in JSON format.
        
        This method ensures all logs include: timestamp, level, service, context, and message.
        Automatically captures stack traces for errors.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Message to log
            service: Name of the service generating the log
            context: Additional context dictionary
            error: Optional exception to include stack trace
            
        Example:
            await logger.log_with_context(
                "INFO",
                "User enrichment completed",
                "EnhancedUserEnricherService",
                context={"user_id": 123, "duration_ms": 450}
            )
        """
        try:
            # Convert string level to LogLevel enum
            log_level = LogLevel[level.upper()]
            
            # Prepare details with context
            details = context.copy() if context else {}
            
            # Add stack trace if error provided
            stack_trace = None
            if error:
                details.update({
                    'error_type': type(error).__name__,
                    'error_message': str(error)
                })
                stack_trace = traceback.format_exc()
            
            # Use internal _log method with all required fields
            await self._log(
                log_level,
                service,  # component
                "operation",  # operation (generic for this method)
                message,
                details=details,
                stack_trace=stack_trace
            )
            
        except KeyError:
            # Invalid log level, default to INFO
            await self._log(
                LogLevel.INFO,
                service,
                "operation",
                message,
                details=context or {}
            )
        except Exception as e:
            print(f"Error in log_with_context: {e}")
    
    async def log_operation_start(
        self,
        operation: str,
        service: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Log the start of an operation and return a unique operation ID for tracking.
        
        Args:
            operation: Name of the operation
            service: Name of the service
            context: Additional context dictionary
            
        Returns:
            str: Unique operation ID for tracking
            
        Example:
            op_id = await logger.log_operation_start(
                "enrich_user_batch",
                "EnhancedUserEnricherService",
                context={"batch_size": 20}
            )
        """
        try:
            # Generate unique operation ID
            operation_id = str(uuid.uuid4())
            
            # Prepare context with operation ID and start time
            op_context = context.copy() if context else {}
            op_context.update({
                'operation_id': operation_id,
                'operation_start': datetime.now().isoformat(),
                'operation_status': 'started'
            })
            
            # Log operation start
            await self._log(
                LogLevel.INFO,
                service,
                operation,
                f"Operation started: {operation}",
                details=op_context,
                request_id=operation_id
            )
            
            return operation_id
            
        except Exception as e:
            print(f"Error in log_operation_start: {e}")
            return str(uuid.uuid4())  # Return a valid ID even on error
    
    async def log_operation_end(
        self,
        operation_id: str,
        success: bool,
        context: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None
    ) -> None:
        """
        Log the end of an operation.
        
        Args:
            operation_id: Operation ID from log_operation_start
            success: Whether the operation was successful
            context: Additional context dictionary
            error: Optional exception if operation failed
            
        Example:
            await logger.log_operation_end(
                op_id,
                success=True,
                context={"processed": 18, "failed": 2}
            )
        """
        try:
            # Prepare context with operation end details
            op_context = context.copy() if context else {}
            op_context.update({
                'operation_id': operation_id,
                'operation_end': datetime.now().isoformat(),
                'operation_status': 'success' if success else 'failed',
                'success': success
            })
            
            # Determine log level based on success
            log_level = LogLevel.INFO if success else LogLevel.ERROR
            
            # Add error details if provided
            stack_trace = None
            if error:
                op_context.update({
                    'error_type': type(error).__name__,
                    'error_message': str(error)
                })
                stack_trace = traceback.format_exc()
            
            # Log operation end
            message = f"Operation {'completed successfully' if success else 'failed'}"
            await self._log(
                log_level,
                "OperationTracker",
                "operation_end",
                message,
                details=op_context,
                request_id=operation_id,
                stack_trace=stack_trace
            )
            
        except Exception as e:
            print(f"Error in log_operation_end: {e}")
    
    async def log_metrics(
        self,
        service: str,
        metrics: Dict[str, Any]
    ) -> None:
        """
        Log aggregated metrics for a service.
        
        Args:
            service: Name of the service
            metrics: Dictionary of metrics to log
            
        Example:
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
        """
        try:
            # Add timestamp to metrics
            metrics_with_timestamp = metrics.copy()
            metrics_with_timestamp['metrics_timestamp'] = datetime.now().isoformat()
            
            # Log metrics
            await self._log(
                LogLevel.INFO,
                service,
                "metrics",
                f"Metrics report for {service}",
                details=metrics_with_timestamp
            )
            
        except Exception as e:
            print(f"Error in log_metrics: {e}")
    
    async def _log(self, level: LogLevel, component: str, operation: str, message: str, **kwargs) -> None:
        """Internal logging method."""
        try:
            # Create log entry
            log_entry = LogEntry(
                timestamp=datetime.now(),
                level=level,
                component=component,
                operation=operation,
                message=message,
                details=kwargs.get('details'),
                user_id=kwargs.get('user_id'),
                session_id=kwargs.get('session_id'),
                request_id=kwargs.get('request_id'),
                duration_ms=kwargs.get('duration_ms'),
                error_code=kwargs.get('error_code'),
                stack_trace=kwargs.get('stack_trace'),
                performance_metrics=kwargs.get('performance_metrics')
            )
            
            # Add to recent logs for real-time analysis
            self._recent_logs.append(log_entry)
            
            # Update statistics
            self._stats["total_logs"] += 1
            self._stats["logs_by_level"][level.value] += 1
            self._stats["logs_by_component"][component] += 1
            
            # Write to file asynchronously
            await self._write_log_to_file(log_entry)
            
            # Perform pattern detection for errors
            if level in [LogLevel.ERROR, LogLevel.CRITICAL] and self._pattern_detection_enabled:
                await self._detect_error_pattern(log_entry)
            
        except Exception as e:
            print(f"Error in logging system: {e}")
    
    async def _write_log_to_file(self, log_entry: LogEntry) -> None:
        """Write log entry to appropriate file."""
        try:
            log_file = self.log_dir / self._log_files[log_entry.level]
            
            # Format log entry as JSON
            log_data = {
                'timestamp': log_entry.timestamp.isoformat(),
                'level': log_entry.level.value,
                'component': log_entry.component,
                'operation': log_entry.operation,
                'message': log_entry.message
            }
            
            # Add optional fields
            if log_entry.details:
                log_data['details'] = log_entry.details
            if log_entry.user_id:
                log_data['user_id'] = log_entry.user_id
            if log_entry.session_id:
                log_data['session_id'] = log_entry.session_id
            if log_entry.request_id:
                log_data['request_id'] = log_entry.request_id
            if log_entry.duration_ms:
                log_data['duration_ms'] = log_entry.duration_ms
            if log_entry.error_code:
                log_data['error_code'] = log_entry.error_code
            if log_entry.stack_trace:
                log_data['stack_trace'] = log_entry.stack_trace
            if log_entry.performance_metrics:
                log_data['performance_metrics'] = log_entry.performance_metrics
            
            # Write to file
            async with aiofiles.open(log_file, 'a') as f:
                await f.write(json.dumps(log_data) + '\n')
                
        except Exception as e:
            print(f"Error writing log to file: {e}")
    
    async def _collect_performance_metrics(self, operation: str, duration_ms: float) -> PerformanceMetrics:
        """Collect system performance metrics."""
        try:
            # Get system metrics using executor to avoid blocking
            metrics = await asyncio.get_event_loop().run_in_executor(
                self.executor, self._get_system_metrics_sync
            )
            
            return PerformanceMetrics(
                operation=operation,
                duration_ms=duration_ms,
                memory_usage_mb=metrics['memory_mb'],
                cpu_percent=metrics['cpu_percent'],
                disk_io_read_mb=metrics['disk_read_mb'],
                disk_io_write_mb=metrics['disk_write_mb'],
                network_sent_mb=metrics['network_sent_mb'],
                network_recv_mb=metrics['network_recv_mb'],
                timestamp=datetime.now()
            )
            
        except Exception as e:
            print(f"Error collecting performance metrics: {e}")
            return PerformanceMetrics(
                operation=operation,
                duration_ms=duration_ms,
                memory_usage_mb=0,
                cpu_percent=0,
                disk_io_read_mb=0,
                disk_io_write_mb=0,
                network_sent_mb=0,
                network_recv_mb=0,
                timestamp=datetime.now()
            )
    
    def _get_system_metrics_sync(self) -> Dict[str, float]:
        """Synchronous system metrics collection."""
        try:
            process = psutil.Process()
            
            # Memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            # CPU usage
            cpu_percent = process.cpu_percent()
            
            # Disk I/O
            io_counters = process.io_counters()
            disk_read_mb = io_counters.read_bytes / (1024 * 1024)
            disk_write_mb = io_counters.write_bytes / (1024 * 1024)
            
            # Network I/O (system-wide)
            net_io = psutil.net_io_counters()
            network_sent_mb = net_io.bytes_sent / (1024 * 1024)
            network_recv_mb = net_io.bytes_recv / (1024 * 1024)
            
            return {
                'memory_mb': memory_mb,
                'cpu_percent': cpu_percent,
                'disk_read_mb': disk_read_mb,
                'disk_write_mb': disk_write_mb,
                'network_sent_mb': network_sent_mb,
                'network_recv_mb': network_recv_mb
            }
            
        except Exception:
            return {
                'memory_mb': 0,
                'cpu_percent': 0,
                'disk_read_mb': 0,
                'disk_write_mb': 0,
                'network_sent_mb': 0,
                'network_recv_mb': 0
            }
    
    async def _detect_error_pattern(self, log_entry: LogEntry) -> None:
        """Detect error patterns for alerting."""
        try:
            # Create pattern key
            pattern_key = f"{log_entry.component}:{log_entry.operation}:{log_entry.level.value}"
            
            now = datetime.now()
            
            if pattern_key in self._error_patterns:
                pattern = self._error_patterns[pattern_key]
                
                # Check if within time window
                if now - pattern.first_occurrence <= self._pattern_time_window:
                    pattern.count += 1
                    pattern.last_occurrence = now
                    
                    # Generate alert if threshold reached
                    if pattern.count >= self._pattern_threshold:
                        await self._generate_pattern_alert(pattern)
                else:
                    # Reset pattern if outside time window
                    pattern.count = 1
                    pattern.first_occurrence = now
                    pattern.last_occurrence = now
            else:
                # Create new pattern
                error_type = log_entry.details.get('error_type', 'Unknown') if log_entry.details else 'Unknown'
                
                pattern = ErrorPattern(
                    pattern_id=pattern_key,
                    error_type=error_type,
                    message_pattern=log_entry.message[:100],  # First 100 chars
                    component=log_entry.component,
                    count=1,
                    first_occurrence=now,
                    last_occurrence=now,
                    severity=AlertSeverity.MEDIUM if log_entry.level == LogLevel.ERROR else AlertSeverity.HIGH,
                    suggested_action=self._get_suggested_action(log_entry)
                )
                
                self._error_patterns[pattern_key] = pattern
                self._stats["error_patterns_detected"] += 1
                
        except Exception as e:
            print(f"Error in pattern detection: {e}")
    
    def _get_suggested_action(self, log_entry: LogEntry) -> str:
        """Get suggested action based on error type."""
        component = log_entry.component.lower()
        operation = log_entry.operation.lower()
        
        if 'session' in component or 'session' in operation:
            return "Check session health and consider session recovery"
        elif 'download' in operation or 'media' in component:
            return "Check network connectivity and retry download"
        elif 'database' in component or 'db' in component:
            return "Check database connection and query performance"
        elif 'rate' in operation or 'flood' in log_entry.message.lower():
            return "Implement rate limiting and backoff strategies"
        else:
            return "Review error details and check system resources"
    
    async def _generate_pattern_alert(self, pattern: ErrorPattern) -> None:
        """Generate alert for detected error pattern."""
        alert_id = f"pattern_{pattern.pattern_id}_{int(time.time())}"
        
        alert = Alert(
            alert_id=alert_id,
            severity=pattern.severity,
            title=f"Error Pattern Detected: {pattern.component}",
            description=f"Pattern '{pattern.message_pattern}' occurred {pattern.count} times in {self._pattern_time_window}",
            component=pattern.component,
            timestamp=datetime.now(),
            details={
                'pattern_id': pattern.pattern_id,
                'error_type': pattern.error_type,
                'count': pattern.count,
                'time_window_minutes': self._pattern_time_window.total_seconds() / 60,
                'first_occurrence': pattern.first_occurrence.isoformat(),
                'last_occurrence': pattern.last_occurrence.isoformat()
            },
            suggested_actions=[pattern.suggested_action]
        )
        
        await self._generate_alert_from_object(alert)
    
    async def _generate_alert(self, severity: AlertSeverity, title: str, description: str, component: str, details: Dict[str, Any]) -> None:
        """Generate a new alert."""
        alert_id = f"alert_{component}_{int(time.time())}"
        
        alert = Alert(
            alert_id=alert_id,
            severity=severity,
            title=title,
            description=description,
            component=component,
            timestamp=datetime.now(),
            details=details,
            suggested_actions=self._get_suggested_actions_for_alert(component, details)
        )
        
        await self._generate_alert_from_object(alert)
    
    async def _generate_alert_from_object(self, alert: Alert) -> None:
        """Generate alert from alert object."""
        try:
            # Check cooldown
            similar_alerts = [
                a for a in self._active_alerts.values()
                if a.component == alert.component and a.severity == alert.severity
                and not a.is_resolved
                and alert.timestamp - a.timestamp < self._alert_cooldown
            ]
            
            if similar_alerts:
                return  # Skip due to cooldown
            
            # Store alert
            self._active_alerts[alert.alert_id] = alert
            self._stats["alerts_generated"] += 1
            
            # Log alert
            await self.log_warning(
                "AlertSystem",
                "generate_alert",
                f"Alert generated: {alert.title}",
                details={
                    'alert_id': alert.alert_id,
                    'severity': alert.severity.value,
                    'component': alert.component,
                    'description': alert.description
                }
            )
            
            # Notify callbacks
            for callback in self._alert_callbacks:
                try:
                    await callback(alert)
                except Exception as e:
                    print(f"Error in alert callback: {e}")
                    
        except Exception as e:
            print(f"Error generating alert: {e}")
    
    def _get_suggested_actions_for_alert(self, component: str, details: Dict[str, Any]) -> List[str]:
        """Get suggested actions for an alert."""
        actions = []
        
        component_lower = component.lower()
        
        if 'media' in component_lower:
            actions.extend([
                "Check network connectivity",
                "Verify media file accessibility",
                "Review download queue status"
            ])
        elif 'session' in component_lower:
            actions.extend([
                "Check session health",
                "Attempt session recovery",
                "Verify account credentials"
            ])
        elif 'database' in component_lower:
            actions.extend([
                "Check database connectivity",
                "Review query performance",
                "Monitor database resources"
            ])
        elif 'performance' in str(details).lower():
            actions.extend([
                "Monitor system resources",
                "Check for memory leaks",
                "Review operation efficiency"
            ])
        
        if not actions:
            actions.append("Review system logs for more details")
        
        return actions
    
    async def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        while True:
            try:
                await asyncio.sleep(60)  # Run every minute
                
                # Check system health
                await self._check_system_health()
                
                # Clean up old data
                await self._cleanup_old_data()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60)
    
    async def _check_system_health(self) -> None:
        """Check overall system health."""
        try:
            # Check memory usage
            metrics = await asyncio.get_event_loop().run_in_executor(
                self.executor, self._get_system_metrics_sync
            )
            
            if metrics['memory_mb'] > self._memory_threshold_mb:
                await self._generate_alert(
                    AlertSeverity.HIGH,
                    "High Memory Usage",
                    f"Memory usage is {metrics['memory_mb']:.1f}MB (threshold: {self._memory_threshold_mb}MB)",
                    "SystemMonitor",
                    {'memory_mb': metrics['memory_mb'], 'threshold_mb': self._memory_threshold_mb}
                )
            
            # Check recent error rate
            recent_errors = [
                log for log in self._recent_logs
                if log.level in [LogLevel.ERROR, LogLevel.CRITICAL]
                and datetime.now() - log.timestamp < timedelta(minutes=10)
            ]
            
            if len(recent_errors) > 10:  # More than 10 errors in 10 minutes
                await self._generate_alert(
                    AlertSeverity.MEDIUM,
                    "High Error Rate",
                    f"{len(recent_errors)} errors in the last 10 minutes",
                    "SystemMonitor",
                    {'error_count': len(recent_errors), 'time_window_minutes': 10}
                )
                
        except Exception as e:
            print(f"Error checking system health: {e}")
    
    async def _cleanup_old_data(self) -> None:
        """Clean up old data to prevent memory leaks."""
        try:
            now = datetime.now()
            
            # Clean up old error patterns
            expired_patterns = [
                key for key, pattern in self._error_patterns.items()
                if now - pattern.last_occurrence > timedelta(hours=24)
            ]
            
            for key in expired_patterns:
                del self._error_patterns[key]
            
            # Clean up resolved alerts older than 24 hours
            expired_alerts = [
                alert_id for alert_id, alert in self._active_alerts.items()
                if alert.is_resolved and alert.resolved_at
                and now - alert.resolved_at > timedelta(hours=24)
            ]
            
            for alert_id in expired_alerts:
                del self._active_alerts[alert_id]
                
        except Exception as e:
            print(f"Error cleaning up old data: {e}")
    
    def add_alert_callback(self, callback: Callable[[Alert], None]) -> None:
        """Add callback for alert notifications."""
        self._alert_callbacks.append(callback)
    
    def remove_alert_callback(self, callback: Callable[[Alert], None]) -> None:
        """Remove alert callback."""
        if callback in self._alert_callbacks:
            self._alert_callbacks.remove(callback)
    
    async def resolve_alert(self, alert_id: str) -> bool:
        """Mark an alert as resolved."""
        try:
            if alert_id in self._active_alerts:
                alert = self._active_alerts[alert_id]
                alert.is_resolved = True
                alert.resolved_at = datetime.now()
                
                await self.log_info(
                    "AlertSystem",
                    "resolve_alert",
                    f"Alert resolved: {alert.title}",
                    details={'alert_id': alert_id}
                )
                
                return True
            return False
            
        except Exception as e:
            await self.log_error("AlertSystem", "resolve_alert", f"Error resolving alert {alert_id}", e)
            return False
    
    async def get_recent_logs(self, level: Optional[LogLevel] = None, component: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent logs with optional filtering."""
        try:
            logs = list(self._recent_logs)
            
            # Apply filters
            if level:
                logs = [log for log in logs if log.level == level]
            
            if component:
                logs = [log for log in logs if log.component == component]
            
            # Sort by timestamp (newest first) and limit
            logs.sort(key=lambda x: x.timestamp, reverse=True)
            logs = logs[:limit]
            
            # Convert to dict format
            return [
                {
                    'timestamp': log.timestamp.isoformat(),
                    'level': log.level.value,
                    'component': log.component,
                    'operation': log.operation,
                    'message': log.message,
                    'details': log.details,
                    'user_id': log.user_id,
                    'duration_ms': log.duration_ms,
                    'error_code': log.error_code
                }
                for log in logs
            ]
            
        except Exception as e:
            print(f"Error getting recent logs: {e}")
            return []
    
    async def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get all active (unresolved) alerts."""
        try:
            active_alerts = [
                alert for alert in self._active_alerts.values()
                if not alert.is_resolved
            ]
            
            return [
                {
                    'alert_id': alert.alert_id,
                    'severity': alert.severity.value,
                    'title': alert.title,
                    'description': alert.description,
                    'component': alert.component,
                    'timestamp': alert.timestamp.isoformat(),
                    'details': alert.details,
                    'suggested_actions': alert.suggested_actions
                }
                for alert in active_alerts
            ]
            
        except Exception as e:
            print(f"Error getting active alerts: {e}")
            return []
    
    async def get_error_patterns(self) -> List[Dict[str, Any]]:
        """Get detected error patterns."""
        try:
            return [
                {
                    'pattern_id': pattern.pattern_id,
                    'error_type': pattern.error_type,
                    'message_pattern': pattern.message_pattern,
                    'component': pattern.component,
                    'count': pattern.count,
                    'first_occurrence': pattern.first_occurrence.isoformat(),
                    'last_occurrence': pattern.last_occurrence.isoformat(),
                    'severity': pattern.severity.value,
                    'suggested_action': pattern.suggested_action
                }
                for pattern in self._error_patterns.values()
            ]
            
        except Exception as e:
            print(f"Error getting error patterns: {e}")
            return []
    
    async def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance metrics summary."""
        try:
            if not self._performance_metrics:
                return {}
            
            metrics = list(self._performance_metrics)
            
            # Calculate averages
            avg_duration = sum(m.duration_ms for m in metrics) / len(metrics)
            avg_memory = sum(m.memory_usage_mb for m in metrics) / len(metrics)
            avg_cpu = sum(m.cpu_percent for m in metrics) / len(metrics)
            
            # Find slowest operations
            slowest = sorted(metrics, key=lambda x: x.duration_ms, reverse=True)[:5]
            
            return {
                'total_operations': len(metrics),
                'average_duration_ms': round(avg_duration, 2),
                'average_memory_mb': round(avg_memory, 2),
                'average_cpu_percent': round(avg_cpu, 2),
                'performance_violations': self._stats["performance_violations"],
                'slowest_operations': [
                    {
                        'operation': m.operation,
                        'duration_ms': m.duration_ms,
                        'timestamp': m.timestamp.isoformat()
                    }
                    for m in slowest
                ]
            }
            
        except Exception as e:
            print(f"Error getting performance summary: {e}")
            return {}
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive logging statistics."""
        return {
            'system_stats': self._stats.copy(),
            'active_alerts_count': len([a for a in self._active_alerts.values() if not a.is_resolved]),
            'error_patterns_count': len(self._error_patterns),
            'recent_logs_count': len(self._recent_logs),
            'performance_metrics_count': len(self._performance_metrics),
            'settings': {
                'pattern_detection_enabled': self._pattern_detection_enabled,
                'pattern_threshold': self._pattern_threshold,
                'performance_monitoring_enabled': self._performance_monitoring_enabled,
                'performance_threshold_ms': self._performance_threshold_ms,
                'memory_threshold_mb': self._memory_threshold_mb
            }
        }
    
    async def generate_diagnostic_report(self) -> Dict[str, Any]:
        """Generate comprehensive diagnostic report."""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'system_health': await self._get_system_health_report(),
                'recent_activity': await self.get_recent_logs(limit=50),
                'active_alerts': await self.get_active_alerts(),
                'error_patterns': await self.get_error_patterns(),
                'performance_summary': await self.get_performance_summary(),
                'statistics': await self.get_statistics()
            }
            
            # Save report to file
            report_file = self.log_dir / f"diagnostic_report_{int(time.time())}.json"
            async with aiofiles.open(report_file, 'w') as f:
                await f.write(json.dumps(report, indent=2))
            
            await self.log_info(
                "DiagnosticSystem",
                "generate_report",
                f"Diagnostic report generated: {report_file}",
                details={'report_file': str(report_file)}
            )
            
            return report
            
        except Exception as e:
            await self.log_error("DiagnosticSystem", "generate_report", "Error generating diagnostic report", e)
            return {'error': str(e)}
    
    async def _get_system_health_report(self) -> Dict[str, Any]:
        """Get system health report."""
        try:
            metrics = await asyncio.get_event_loop().run_in_executor(
                self.executor, self._get_system_metrics_sync
            )
            
            # Determine health status
            health_status = "healthy"
            issues = []
            
            if metrics['memory_mb'] > self._memory_threshold_mb:
                health_status = "warning"
                issues.append(f"High memory usage: {metrics['memory_mb']:.1f}MB")
            
            if metrics['cpu_percent'] > 80:
                health_status = "warning"
                issues.append(f"High CPU usage: {metrics['cpu_percent']:.1f}%")
            
            # Check recent error rate
            recent_errors = [
                log for log in self._recent_logs
                if log.level in [LogLevel.ERROR, LogLevel.CRITICAL]
                and datetime.now() - log.timestamp < timedelta(minutes=30)
            ]
            
            if len(recent_errors) > 20:
                health_status = "critical"
                issues.append(f"High error rate: {len(recent_errors)} errors in 30 minutes")
            
            return {
                'status': health_status,
                'issues': issues,
                'metrics': metrics,
                'uptime_hours': (datetime.now() - datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() / 3600
            }
            
        except Exception as e:
            return {
                'status': 'unknown',
                'error': str(e)
            }
    
    async def shutdown(self) -> None:
        """Shutdown the logging system."""
        try:
            await self.log_info("EnhancedLoggingSystem", "shutdown", "Shutting down logging system")
            
            # Stop monitoring task
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            # Shutdown executor
            self.executor.shutdown(wait=True)
            
            self._initialized = False
            
        except Exception as e:
            print(f"Error during logging system shutdown: {e}")


# Global instance
enhanced_logging = EnhancedLoggingSystem()