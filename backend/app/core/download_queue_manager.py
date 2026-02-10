import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Set
from collections import defaultdict
import heapq
import uuid

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, or_
from sqlalchemy.orm import selectinload

from backend.app.models.download_task import DownloadTask
from backend.app.models.media import MediaFile
from backend.app.db.database import async_session_maker
from backend.app.core.queue_types import (
    TaskPriority, WorkerStatus, QueueStatus, WorkerInfo, TaskItem, QueueStatistics
)


class DownloadQueueManager:
    """
    Manages media download queues with priority handling, worker coordination,
    backpressure mechanisms, and comprehensive monitoring.
    """
    
    def __init__(
        self,
        max_workers: int = 5,
        max_queue_size: int = 1000,
        worker_timeout: int = 300,
        enable_backpressure: bool = True,
        load_balancing_strategy = None  # Avoid circular import
    ):
        # Import here to avoid circular import
        from backend.app.core.worker_coordinator import WorkerCoordinator, LoadBalancingStrategy
        
        if load_balancing_strategy is None:
            load_balancing_strategy = LoadBalancingStrategy.LEAST_LOADED
            
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self.worker_timeout = worker_timeout
        self.enable_backpressure = enable_backpressure
        
        # Worker coordination
        self.worker_coordinator = WorkerCoordinator(
            load_balancing_strategy=load_balancing_strategy,
            worker_timeout=worker_timeout
        )
        
        # Queue management
        self._task_queue: List[TaskItem] = []
        self._queue_lock = asyncio.Lock()
        self._queue_condition = asyncio.Condition(self._queue_lock)
        
        # Worker management
        self._workers: Dict[str, WorkerInfo] = {}
        self._worker_tasks: Dict[str, asyncio.Task] = {}
        self._worker_semaphore = asyncio.Semaphore(max_workers)
        
        # Status tracking
        self._queue_status = QueueStatus.STOPPED
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        
        # Statistics
        self._stats = QueueStatistics()
        self._task_history: List[Dict[str, Any]] = []
        self._processing_times: List[float] = []
        self._last_stats_update = time.time()
        
        # Task tracking
        self._active_tasks: Dict[str, TaskItem] = {}
        self._completed_tasks: Set[str] = set()
        self._failed_tasks: Set[str] = set()
        
        # Backpressure management
        self._backpressure_active = False
        self._backpressure_threshold = int(max_queue_size * 0.8)
        self._backpressure_callbacks: List[Callable] = []
        
        # Monitoring
        self._monitor_task: Optional[asyncio.Task] = None
        self._monitor_interval = 30  # seconds
    
    async def start(self):
        """Starts the queue manager and worker pool."""
        if self._is_running:
            self.logger.warning("Queue manager is already running")
            return
        
        self.logger.info(f"Starting download queue manager with {self.max_workers} workers")
        
        self._is_running = True
        self._queue_status = QueueStatus.RUNNING
        self._shutdown_event.clear()
        
        # Start worker pool
        await self._start_workers()
        
        # Start worker coordination
        await self.worker_coordinator.start_coordination(self._workers)
        
        # Start monitoring task
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        
        # Update statistics
        await self._update_statistics()
        
        self.logger.info("Download queue manager started successfully")
    
    async def stop(self, timeout: int = 30):
        """Stops the queue manager and all workers."""
        if not self._is_running:
            return
        
        self.logger.info("Stopping download queue manager...")
        
        self._is_running = False
        self._queue_status = QueueStatus.STOPPED
        self._shutdown_event.set()
        
        # Stop monitoring
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        # Stop worker coordination
        await self.worker_coordinator.stop_coordination()
        
        # Stop all workers
        await self._stop_workers(timeout)
        
        # Final statistics update
        await self._update_statistics()
        
        self.logger.info("Download queue manager stopped")
    
    async def pause(self):
        """Pauses queue processing."""
        if self._queue_status == QueueStatus.RUNNING:
            self._queue_status = QueueStatus.PAUSED
            self.logger.info("Queue processing paused")
    
    async def resume(self):
        """Resumes queue processing."""
        if self._queue_status == QueueStatus.PAUSED:
            self._queue_status = QueueStatus.RUNNING
            async with self._queue_condition:
                self._queue_condition.notify_all()
            self.logger.info("Queue processing resumed")
    
    async def enqueue_download(
        self,
        download_task: DownloadTask,
        priority: TaskPriority = TaskPriority.NORMAL
    ) -> str:
        """
        Enqueues a download task with specified priority.
        
        Args:
            download_task: The download task to enqueue
            priority: Task priority (lower number = higher priority)
            
        Returns:
            Task ID for tracking
        """
        if not self._is_running:
            raise RuntimeError("Queue manager is not running")
        
        # Check backpressure
        if self.enable_backpressure and len(self._task_queue) >= self._backpressure_threshold:
            if not self._backpressure_active:
                self._backpressure_active = True
                await self._trigger_backpressure_callbacks()
            
            # If queue is full, reject the task
            if len(self._task_queue) >= self.max_queue_size:
                raise RuntimeError("Queue is full, cannot enqueue new tasks")
        
        task_id = download_task.task_id or str(uuid.uuid4())
        download_task.task_id = task_id
        
        task_item = TaskItem(
            task_id=task_id,
            priority=priority,
            task_data=download_task,
            created_at=datetime.utcnow(),
            download_task=download_task
        )
        
        async with self._queue_lock:
            heapq.heappush(self._task_queue, task_item)
            self._stats.queued_tasks += 1
            self._stats.total_tasks += 1
            
            # Update task status in database
            download_task.status = "queued"
            download_task.priority = priority.value
            
            async with async_session_maker() as db:
                db.add(download_task)
                await db.commit()
        
        # Notify workers
        async with self._queue_condition:
            self._queue_condition.notify()
        
        self.logger.debug(f"Enqueued task {task_id} with priority {priority}")
        return task_id
    
    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancels a queued or processing task.
        
        Args:
            task_id: ID of the task to cancel
            
        Returns:
            True if task was cancelled, False if not found
        """
        async with self._queue_lock:
            # Check if task is in queue
            for i, task_item in enumerate(self._task_queue):
                if task_item.task_id == task_id:
                    del self._task_queue[i]
                    heapq.heapify(self._task_queue)  # Restore heap property
                    self._stats.queued_tasks -= 1
                    
                    # Update database
                    async with async_session_maker() as db:
                        await db.execute(
                            update(DownloadTask)
                            .where(DownloadTask.task_id == task_id)
                            .values(status="cancelled")
                        )
                        await db.commit()
                    
                    self.logger.info(f"Cancelled queued task {task_id}")
                    return True
            
            # Check if task is currently processing
            if task_id in self._active_tasks:
                # Find the worker processing this task
                for worker_id, worker_info in self._workers.items():
                    if worker_info.current_task_id == task_id:
                        # Cancel the worker task
                        if worker_id in self._worker_tasks:
                            self._worker_tasks[worker_id].cancel()
                        
                        # Update database
                        async with async_session_maker() as db:
                            await db.execute(
                                update(DownloadTask)
                                .where(DownloadTask.task_id == task_id)
                                .values(status="cancelled")
                            )
                            await db.commit()
                        
                        self.logger.info(f"Cancelled processing task {task_id}")
                        return True
        
        return False
    
    async def get_queue_statistics(self) -> QueueStatistics:
        """Returns current queue statistics."""
        await self._update_statistics()
        return self._stats
    
    async def get_worker_info(self) -> List[WorkerInfo]:
        """Returns information about all workers."""
        return list(self._workers.values())
    
    async def get_task_status(self, task_id: str) -> Optional[str]:
        """
        Gets the current status of a task.
        
        Args:
            task_id: ID of the task to check
            
        Returns:
            Task status string or None if not found
        """
        # Check active tasks
        if task_id in self._active_tasks:
            return "processing"
        
        # Check completed/failed sets
        if task_id in self._completed_tasks:
            return "completed"
        if task_id in self._failed_tasks:
            return "failed"
        
        # Check queue
        async with self._queue_lock:
            for task_item in self._task_queue:
                if task_item.task_id == task_id:
                    return "queued"
        
        # Check database
        async with async_session_maker() as db:
            result = await db.execute(
                select(DownloadTask.status).where(DownloadTask.task_id == task_id)
            )
            status = result.scalar_one_or_none()
            return status
    
    async def get_worker_recommendations(self) -> Dict[str, Any]:
        """Gets worker optimization recommendations."""
        return await self.worker_coordinator.get_worker_recommendations(self._workers)
    
    async def rebalance_workload(self):
        """Rebalances workload across workers."""
        await self.worker_coordinator.rebalance_workload(self._workers)
    
    async def add_backpressure_callback(self, callback: Callable):
        """Adds a callback to be called when backpressure is activated."""
        self._backpressure_callbacks.append(callback)
    
    async def remove_backpressure_callback(self, callback: Callable):
        """Removes a backpressure callback."""
        if callback in self._backpressure_callbacks:
            self._backpressure_callbacks.remove(callback)
    
    async def _start_workers(self):
        """Starts the worker pool."""
        for i in range(self.max_workers):
            worker_id = f"worker-{i+1}"
            worker_info = WorkerInfo(
                worker_id=worker_id,
                status=WorkerStatus.IDLE,
                start_time=datetime.utcnow()
            )
            self._workers[worker_id] = worker_info
            
            # Start worker task
            worker_task = asyncio.create_task(self._worker_loop(worker_id))
            self._worker_tasks[worker_id] = worker_task
        
        self.logger.info(f"Started {self.max_workers} workers")
    
    async def _stop_workers(self, timeout: int):
        """Stops all workers with timeout."""
        # Cancel all worker tasks
        for worker_task in self._worker_tasks.values():
            worker_task.cancel()
        
        # Wait for workers to finish with timeout
        if self._worker_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._worker_tasks.values(), return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                self.logger.warning(f"Some workers did not stop within {timeout} seconds")
        
        # Update worker statuses
        for worker_info in self._workers.values():
            worker_info.status = WorkerStatus.STOPPED
        
        self._worker_tasks.clear()
    
    async def _worker_loop(self, worker_id: str):
        """Main worker loop for processing tasks."""
        worker_info = self._workers[worker_id]
        
        try:
            while self._is_running and not self._shutdown_event.is_set():
                try:
                    # Wait for tasks or shutdown
                    task_item = await self._get_next_task()
                    if not task_item:
                        continue
                    
                    # Process the task
                    await self._process_task(worker_id, task_item)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"Worker {worker_id} error: {e}")
                    worker_info.status = WorkerStatus.ERROR
                    worker_info.error_message = str(e)
                    await asyncio.sleep(5)  # Brief pause before retrying
                    worker_info.status = WorkerStatus.IDLE
                    worker_info.error_message = None
        
        except asyncio.CancelledError:
            pass
        finally:
            worker_info.status = WorkerStatus.STOPPED
            worker_info.current_task_id = None
            self.logger.debug(f"Worker {worker_id} stopped")
    
    async def _get_next_task(self) -> Optional[TaskItem]:
        """Gets the next task from the queue."""
        async with self._queue_condition:
            while self._is_running:
                # Check if queue is paused
                if self._queue_status == QueueStatus.PAUSED:
                    await self._queue_condition.wait()
                    continue
                
                # Check if there are tasks in queue
                if self._task_queue:
                    task_item = heapq.heappop(self._task_queue)
                    self._stats.queued_tasks -= 1
                    self._stats.processing_tasks += 1
                    return task_item
                
                # Wait for new tasks
                await self._queue_condition.wait()
        
        return None
    
    async def _process_task(self, worker_id: str, task_item: TaskItem):
        """Processes a single task."""
        worker_info = self._workers[worker_id]
        task_id = task_item.task_id
        download_task = task_item.download_task
        
        start_time = time.time()
        
        try:
            # Update worker status
            worker_info.status = WorkerStatus.BUSY
            worker_info.current_task_id = task_id
            worker_info.last_activity = datetime.utcnow()
            
            # Add to active tasks
            self._active_tasks[task_id] = task_item
            
            # Update task status in database
            async with async_session_maker() as db:
                await db.execute(
                    update(DownloadTask)
                    .where(DownloadTask.task_id == task_id)
                    .values(
                        status="processing",
                        assigned_worker=worker_id,
                        started_at=datetime.utcnow()
                    )
                )
                await db.commit()
            
            self.logger.debug(f"Worker {worker_id} processing task {task_id}")
            
            # Process the actual download task
            success = await self._execute_download_task(download_task)
            
            processing_time = time.time() - start_time
            self._processing_times.append(processing_time)
            
            # Update statistics
            if success:
                self._completed_tasks.add(task_id)
                self._stats.completed_tasks += 1
                worker_info.tasks_completed += 1
                
                # Update database
                async with async_session_maker() as db:
                    await db.execute(
                        update(DownloadTask)
                        .where(DownloadTask.task_id == task_id)
                        .values(
                            status="completed",
                            completed_at=datetime.utcnow()
                        )
                    )
                    await db.commit()
                
                self.logger.debug(f"Task {task_id} completed successfully")
            else:
                self._failed_tasks.add(task_id)
                self._stats.failed_tasks += 1
                worker_info.tasks_failed += 1
                
                # Update database
                async with async_session_maker() as db:
                    await db.execute(
                        update(DownloadTask)
                        .where(DownloadTask.task_id == task_id)
                        .values(
                            status="failed",
                            completed_at=datetime.utcnow(),
                            error_message="Download execution failed"
                        )
                    )
                    await db.commit()
                
                self.logger.warning(f"Task {task_id} failed")
            
            # Update worker processing time average
            if worker_info.tasks_completed > 0:
                worker_info.processing_time_avg = (
                    (worker_info.processing_time_avg * (worker_info.tasks_completed - 1) + processing_time)
                    / worker_info.tasks_completed
                )
            
            # Report completion to worker coordinator
            await self.worker_coordinator.report_task_completion(
                task_id, worker_id, success, processing_time
            )
            
        except Exception as e:
            self.logger.error(f"Error processing task {task_id}: {e}")
            
            self._failed_tasks.add(task_id)
            self._stats.failed_tasks += 1
            worker_info.tasks_failed += 1
            
            # Update database
            async with async_session_maker() as db:
                await db.execute(
                    update(DownloadTask)
                    .where(DownloadTask.task_id == task_id)
                    .values(
                        status="failed",
                        completed_at=datetime.utcnow(),
                        error_message=str(e)
                    )
                )
                await db.commit()
        
        finally:
            # Clean up
            self._stats.processing_tasks -= 1
            if task_id in self._active_tasks:
                del self._active_tasks[task_id]
            
            worker_info.status = WorkerStatus.IDLE
            worker_info.current_task_id = None
            worker_info.last_activity = datetime.utcnow()
    
    async def _execute_download_task(self, download_task: DownloadTask) -> bool:
        """
        Executes the actual download task.
        This is a placeholder - should be implemented with actual download logic.
        
        Args:
            download_task: The download task to execute
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # This is where the actual download logic would go
            # For now, we'll simulate the download process
            
            # Import here to avoid circular imports
            from backend.app.services.telegram_service import telegram_manager
            from backend.app.core.media_validator import MediaValidator
            
            # Get the media file
            async with async_session_maker() as db:
                result = await db.execute(
                    select(MediaFile)
                    .options(selectinload(MediaFile.message))
                    .where(MediaFile.id == download_task.media_file_id)
                )
                media_file = result.scalar_one_or_none()
                
                if not media_file:
                    self.logger.error(f"Media file {download_task.media_file_id} not found")
                    return False
                
                # Get an active client
                active_clients = list(telegram_manager.clients.values())
                if not active_clients:
                    self.logger.error("No active Telegram clients available")
                    return False
                
                client = active_clients[0]  # Use first available client
                
                # Simulate download process
                await asyncio.sleep(0.1)  # Simulate download time
                
                # For now, just mark as successful
                # In real implementation, this would:
                # 1. Download the media file using the client
                # 2. Validate the downloaded file
                # 3. Update the media file record
                # 4. Handle any errors appropriately
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error executing download task: {e}")
            return False
    
    async def _update_statistics(self):
        """Updates queue statistics."""
        current_time = time.time()
        
        # Update basic counts
        self._stats.total_workers = len(self._workers)
        self._stats.active_workers = sum(
            1 for w in self._workers.values() 
            if w.status == WorkerStatus.BUSY
        )
        self._stats.queue_status = self._queue_status
        self._stats.backlog_size = len(self._task_queue)
        
        # Calculate average processing time
        if self._processing_times:
            self._stats.average_processing_time = sum(self._processing_times) / len(self._processing_times)
            # Keep only recent processing times (last 100)
            if len(self._processing_times) > 100:
                self._processing_times = self._processing_times[-100:]
        
        # Calculate throughput
        time_diff = current_time - self._last_stats_update
        if time_diff > 0:
            completed_in_period = len([
                t for t in self._task_history 
                if t.get('completed_at', 0) > self._last_stats_update
            ])
            self._stats.throughput_per_minute = (completed_in_period / time_diff) * 60
        
        # Estimate completion time
        if self._stats.backlog_size > 0 and self._stats.throughput_per_minute > 0:
            minutes_remaining = self._stats.backlog_size / self._stats.throughput_per_minute
            self._stats.estimated_completion_time = datetime.utcnow() + timedelta(minutes=minutes_remaining)
        else:
            self._stats.estimated_completion_time = None
        
        self._last_stats_update = current_time
    
    async def _monitor_loop(self):
        """Background monitoring loop."""
        while self._is_running:
            try:
                await self._update_statistics()
                
                # Check for stuck workers
                current_time = datetime.utcnow()
                for worker_id, worker_info in self._workers.items():
                    if (worker_info.status == WorkerStatus.BUSY and 
                        worker_info.last_activity and
                        (current_time - worker_info.last_activity).seconds > self.worker_timeout):
                        
                        self.logger.warning(f"Worker {worker_id} appears stuck, restarting...")
                        
                        # Cancel the worker task
                        if worker_id in self._worker_tasks:
                            self._worker_tasks[worker_id].cancel()
                        
                        # Restart the worker
                        worker_info.status = WorkerStatus.IDLE
                        worker_info.current_task_id = None
                        worker_info.error_message = "Restarted due to timeout"
                        
                        worker_task = asyncio.create_task(self._worker_loop(worker_id))
                        self._worker_tasks[worker_id] = worker_task
                
                # Check backpressure
                if self._backpressure_active and len(self._task_queue) < self._backpressure_threshold * 0.5:
                    self._backpressure_active = False
                    self.logger.info("Backpressure deactivated")
                
                await asyncio.sleep(self._monitor_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(self._monitor_interval)
    
    async def _trigger_backpressure_callbacks(self):
        """Triggers all registered backpressure callbacks."""
        for callback in self._backpressure_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                self.logger.error(f"Error in backpressure callback: {e}")