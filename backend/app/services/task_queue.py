import asyncio
from typing import Dict, Any, Callable, Optional, List
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
import uuid


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskPriority(int, Enum):
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


@dataclass
class Task:
    id: str
    name: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: int = 0
    metadata: dict = field(default_factory=dict)


class TaskQueue:
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.tasks: Dict[str, Task] = {}
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.workers: List[asyncio.Task] = []
        self.running = False
        self.event_handlers: Dict[str, List[Callable]] = {}
    
    async def start(self):
        if self.running:
            return
        
        self.running = True
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker(i))
            self.workers.append(worker)
    
    async def stop(self):
        self.running = False
        for worker in self.workers:
            worker.cancel()
        self.workers.clear()
    
    async def _worker(self, worker_id: int):
        while self.running:
            try:
                priority, task_id = await asyncio.wait_for(
                    self.queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            
            task = self.tasks.get(task_id)
            if not task or task.status == TaskStatus.CANCELLED:
                continue
            
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.utcnow()
            await self._emit_event("task_started", task)
            
            try:
                if asyncio.iscoroutinefunction(task.func):
                    task.result = await task.func(*task.args, **task.kwargs)
                else:
                    task.result = task.func(*task.args, **task.kwargs)
                
                task.status = TaskStatus.COMPLETED
                task.progress = 100
            except Exception as e:
                task.status = TaskStatus.FAILED
                task.error = str(e)
            finally:
                task.completed_at = datetime.utcnow()
                await self._emit_event("task_completed", task)
    
    async def enqueue(
        self,
        name: str,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        metadata: dict = None
    ) -> str:
        task_id = str(uuid.uuid4())
        
        task = Task(
            id=task_id,
            name=name,
            func=func,
            args=args,
            kwargs=kwargs or {},
            priority=priority,
            metadata=metadata or {}
        )
        
        self.tasks[task_id] = task
        await self.queue.put((-priority.value, task_id))
        await self._emit_event("task_enqueued", task)
        
        return task_id
    
    def get_task(self, task_id: str) -> Optional[Task]:
        return self.tasks.get(task_id)
    
    def get_all_tasks(self) -> List[Task]:
        return list(self.tasks.values())
    
    def get_pending_tasks(self) -> List[Task]:
        return [t for t in self.tasks.values() if t.status == TaskStatus.PENDING]
    
    def get_running_tasks(self) -> List[Task]:
        return [t for t in self.tasks.values() if t.status == TaskStatus.RUNNING]
    
    async def cancel_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if task and task.status in [TaskStatus.PENDING, TaskStatus.RUNNING]:
            task.status = TaskStatus.CANCELLED
            await self._emit_event("task_cancelled", task)
            return True
        return False
    
    def update_progress(self, task_id: str, progress: int):
        task = self.tasks.get(task_id)
        if task:
            task.progress = min(100, max(0, progress))
    
    def on(self, event: str, handler: Callable):
        if event not in self.event_handlers:
            self.event_handlers[event] = []
        self.event_handlers[event].append(handler)
    
    async def _emit_event(self, event: str, task: Task):
        handlers = self.event_handlers.get(event, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(task)
                else:
                    handler(task)
            except Exception as e:
                print(f"Error in event handler: {e}")
    
    def cleanup_old_tasks(self, max_age_hours: int = 24):
        now = datetime.utcnow()
        to_remove = []
        
        for task_id, task in self.tasks.items():
            if task.completed_at:
                age = (now - task.completed_at).total_seconds() / 3600
                if age > max_age_hours:
                    to_remove.append(task_id)
        
        for task_id in to_remove:
            del self.tasks[task_id]
        
        return len(to_remove)


task_queue = TaskQueue(max_workers=5)
