"""
Shared types and enums for queue management components.

This module contains shared data structures to avoid circular imports
between DownloadQueueManager and WorkerCoordinator.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional
from datetime import datetime


class TaskPriority(Enum):
    """Enumeration for task priorities."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class WorkerStatus(Enum):
    """Enumeration for worker statuses."""
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"
    STOPPED = "stopped"


class QueueStatus(Enum):
    """Enumeration for queue statuses."""
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class WorkerInfo:
    """Data class for worker information."""
    worker_id: str
    status: WorkerStatus
    current_task_id: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    last_activity: Optional[datetime] = None
    error_message: Optional[str] = None
    performance_metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskItem:
    """Data class for queue task items."""
    task_id: str
    priority: TaskPriority
    task_data: Any
    created_at: datetime
    attempts: int = 0
    max_attempts: int = 3
    last_error: Optional[str] = None
    assigned_worker: Optional[str] = None
    download_task: Optional[Any] = None  # DownloadTask - avoiding circular import
    retry_count: int = 0
    
    def __lt__(self, other):
        # Lower priority number = higher priority
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        # If same priority, older tasks first
        return self.created_at < other.created_at


@dataclass
class QueueStatistics:
    """Data class for queue statistics."""
    total_tasks: int = 0
    queued_tasks: int = 0
    processing_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    active_workers: int = 0
    queue_status: QueueStatus = QueueStatus.STOPPED
    average_processing_time: float = 0.0
    success_rate: float = 0.0