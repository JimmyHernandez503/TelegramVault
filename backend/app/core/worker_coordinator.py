import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import statistics
import uuid

from backend.app.core.queue_types import WorkerInfo, WorkerStatus, TaskItem


class LoadBalancingStrategy(Enum):
    """Enumeration for load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    FASTEST_WORKER = "fastest_worker"
    PRIORITY_BASED = "priority_based"


class HealthCheckStatus(Enum):
    """Enumeration for health check status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class WorkerMetrics:
    """Data class for detailed worker metrics."""
    worker_id: str
    tasks_per_minute: float = 0.0
    average_task_time: float = 0.0
    success_rate: float = 1.0
    error_rate: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    last_health_check: Optional[datetime] = None
    health_status: HealthCheckStatus = HealthCheckStatus.UNKNOWN
    consecutive_failures: int = 0
    total_uptime: timedelta = field(default_factory=lambda: timedelta())


@dataclass
class LoadBalancingResult:
    """Data class for load balancing results."""
    selected_worker_id: str
    reason: str
    load_scores: Dict[str, float]
    strategy_used: LoadBalancingStrategy


class WorkerCoordinator:
    """
    Advanced worker coordination with load balancing, health monitoring,
    and intelligent task distribution.
    """
    
    def __init__(
        self,
        load_balancing_strategy: LoadBalancingStrategy = LoadBalancingStrategy.LEAST_LOADED,
        health_check_interval: int = 30,
        max_consecutive_failures: int = 3,
        worker_timeout: int = 300
    ):
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.load_balancing_strategy = load_balancing_strategy
        self.health_check_interval = health_check_interval
        self.max_consecutive_failures = max_consecutive_failures
        self.worker_timeout = worker_timeout
        
        # Worker tracking
        self._worker_metrics: Dict[str, WorkerMetrics] = {}
        self._worker_assignments: Dict[str, str] = {}  # task_id -> worker_id
        self._worker_queues: Dict[str, List[str]] = {}  # worker_id -> task_ids
        
        # Load balancing
        self._round_robin_index = 0
        self._load_scores: Dict[str, float] = {}
        
        # Health monitoring
        self._health_monitor_task: Optional[asyncio.Task] = None
        self._is_monitoring = False
        
        # Performance tracking
        self._performance_history: Dict[str, List[Tuple[datetime, float]]] = {}
        self._task_completion_times: Dict[str, List[float]] = {}
        
        # Coordination callbacks
        self._worker_failure_callbacks: List[Callable] = []
        self._load_rebalance_callbacks: List[Callable] = []
    
    async def start_coordination(self, workers: Dict[str, WorkerInfo]):
        """Starts worker coordination and monitoring."""
        self.logger.info("Starting worker coordination")
        
        # Initialize metrics for all workers
        for worker_id, worker_info in workers.items():
            if worker_id not in self._worker_metrics:
                self._worker_metrics[worker_id] = WorkerMetrics(
                    worker_id=worker_id,
                    last_health_check=datetime.utcnow()
                )
            self._worker_queues[worker_id] = []
        
        # Start health monitoring
        self._is_monitoring = True
        self._health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        
        self.logger.info(f"Worker coordination started for {len(workers)} workers")
    
    async def stop_coordination(self):
        """Stops worker coordination and monitoring."""
        self.logger.info("Stopping worker coordination")
        
        self._is_monitoring = False
        if self._health_monitor_task:
            self._health_monitor_task.cancel()
            try:
                await self._health_monitor_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Worker coordination stopped")
    
    async def assign_task(
        self,
        task_item: TaskItem,
        available_workers: List[str],
        worker_info: Dict[str, WorkerInfo]
    ) -> LoadBalancingResult:
        """
        Assigns a task to the best available worker using load balancing.
        
        Args:
            task_item: The task to assign
            available_workers: List of available worker IDs
            worker_info: Dictionary of worker information
            
        Returns:
            LoadBalancingResult with assignment details
        """
        if not available_workers:
            raise ValueError("No available workers for task assignment")
        
        # Update load scores
        await self._update_load_scores(available_workers, worker_info)
        
        # Select worker based on strategy
        if self.load_balancing_strategy == LoadBalancingStrategy.ROUND_ROBIN:
            result = await self._round_robin_assignment(available_workers)
        elif self.load_balancing_strategy == LoadBalancingStrategy.LEAST_LOADED:
            result = await self._least_loaded_assignment(available_workers)
        elif self.load_balancing_strategy == LoadBalancingStrategy.FASTEST_WORKER:
            result = await self._fastest_worker_assignment(available_workers)
        elif self.load_balancing_strategy == LoadBalancingStrategy.PRIORITY_BASED:
            result = await self._priority_based_assignment(task_item, available_workers)
        else:
            result = await self._least_loaded_assignment(available_workers)
        
        # Record assignment
        self._worker_assignments[task_item.task_id] = result.selected_worker_id
        self._worker_queues[result.selected_worker_id].append(task_item.task_id)
        
        self.logger.debug(
            f"Assigned task {task_item.task_id} to worker {result.selected_worker_id} "
            f"using {result.strategy_used.value} strategy: {result.reason}"
        )
        
        return result
    
    async def report_task_completion(
        self,
        task_id: str,
        worker_id: str,
        success: bool,
        processing_time: float
    ):
        """
        Reports task completion and updates worker metrics.
        
        Args:
            task_id: ID of the completed task
            worker_id: ID of the worker that completed the task
            success: Whether the task was successful
            processing_time: Time taken to process the task
        """
        # Update worker metrics
        if worker_id in self._worker_metrics:
            metrics = self._worker_metrics[worker_id]
            
            # Update task completion times
            if worker_id not in self._task_completion_times:
                self._task_completion_times[worker_id] = []
            self._task_completion_times[worker_id].append(processing_time)
            
            # Keep only recent completion times (last 50)
            if len(self._task_completion_times[worker_id]) > 50:
                self._task_completion_times[worker_id] = self._task_completion_times[worker_id][-50:]
            
            # Update average task time
            metrics.average_task_time = statistics.mean(self._task_completion_times[worker_id])
            
            # Update success/error rates
            if success:
                metrics.consecutive_failures = 0
            else:
                metrics.consecutive_failures += 1
            
            # Calculate tasks per minute
            current_time = datetime.utcnow()
            if worker_id not in self._performance_history:
                self._performance_history[worker_id] = []
            
            self._performance_history[worker_id].append((current_time, processing_time))
            
            # Keep only last hour of performance data
            cutoff_time = current_time - timedelta(hours=1)
            self._performance_history[worker_id] = [
                (timestamp, time_taken) for timestamp, time_taken in self._performance_history[worker_id]
                if timestamp > cutoff_time
            ]
            
            # Calculate tasks per minute
            if len(self._performance_history[worker_id]) > 1:
                time_span = (
                    self._performance_history[worker_id][-1][0] - 
                    self._performance_history[worker_id][0][0]
                ).total_seconds() / 60  # Convert to minutes
                
                if time_span > 0:
                    metrics.tasks_per_minute = len(self._performance_history[worker_id]) / time_span
            
            # Update health status based on consecutive failures
            if metrics.consecutive_failures >= self.max_consecutive_failures:
                metrics.health_status = HealthCheckStatus.UNHEALTHY
                await self._handle_unhealthy_worker(worker_id)
            elif metrics.consecutive_failures > 0:
                metrics.health_status = HealthCheckStatus.DEGRADED
            else:
                metrics.health_status = HealthCheckStatus.HEALTHY
        
        # Clean up assignment tracking
        if task_id in self._worker_assignments:
            del self._worker_assignments[task_id]
        
        if worker_id in self._worker_queues and task_id in self._worker_queues[worker_id]:
            self._worker_queues[worker_id].remove(task_id)
    
    async def get_worker_recommendations(
        self,
        worker_info: Dict[str, WorkerInfo]
    ) -> Dict[str, Any]:
        """
        Provides recommendations for worker pool optimization.
        
        Args:
            worker_info: Dictionary of current worker information
            
        Returns:
            Dictionary with optimization recommendations
        """
        recommendations = {
            "scaling_recommendation": "maintain",
            "reason": "",
            "suggested_worker_count": len(worker_info),
            "performance_issues": [],
            "optimization_suggestions": []
        }
        
        # Analyze worker performance
        healthy_workers = 0
        degraded_workers = 0
        unhealthy_workers = 0
        
        avg_task_times = []
        tasks_per_minute_total = 0
        
        for worker_id, metrics in self._worker_metrics.items():
            if metrics.health_status == HealthCheckStatus.HEALTHY:
                healthy_workers += 1
            elif metrics.health_status == HealthCheckStatus.DEGRADED:
                degraded_workers += 1
            elif metrics.health_status == HealthCheckStatus.UNHEALTHY:
                unhealthy_workers += 1
            
            if metrics.average_task_time > 0:
                avg_task_times.append(metrics.average_task_time)
            
            tasks_per_minute_total += metrics.tasks_per_minute
        
        # Scaling recommendations
        total_workers = len(worker_info)
        utilization_rate = (total_workers - len([w for w in worker_info.values() if w.status == WorkerStatus.IDLE])) / total_workers
        
        if utilization_rate > 0.8:
            recommendations["scaling_recommendation"] = "scale_up"
            recommendations["reason"] = f"High utilization rate: {utilization_rate:.1%}"
            recommendations["suggested_worker_count"] = min(total_workers + 2, total_workers * 2)
        elif utilization_rate < 0.3 and total_workers > 2:
            recommendations["scaling_recommendation"] = "scale_down"
            recommendations["reason"] = f"Low utilization rate: {utilization_rate:.1%}"
            recommendations["suggested_worker_count"] = max(2, total_workers - 1)
        
        # Performance issues
        if unhealthy_workers > 0:
            recommendations["performance_issues"].append(
                f"{unhealthy_workers} unhealthy workers need attention"
            )
        
        if degraded_workers > total_workers * 0.3:
            recommendations["performance_issues"].append(
                f"{degraded_workers} workers showing degraded performance"
            )
        
        if avg_task_times and statistics.mean(avg_task_times) > 60:  # More than 1 minute average
            recommendations["performance_issues"].append(
                "Average task processing time is high"
            )
        
        # Optimization suggestions
        if tasks_per_minute_total < total_workers * 0.5:  # Less than 0.5 tasks per minute per worker
            recommendations["optimization_suggestions"].append(
                "Consider optimizing task processing logic"
            )
        
        if len(set(metrics.health_status for metrics in self._worker_metrics.values())) > 1:
            recommendations["optimization_suggestions"].append(
                "Worker performance is inconsistent - investigate resource allocation"
            )
        
        return recommendations
    
    async def rebalance_workload(self, worker_info: Dict[str, WorkerInfo]):
        """
        Rebalances workload across workers if needed.
        
        Args:
            worker_info: Dictionary of current worker information
        """
        # Check if rebalancing is needed
        queue_sizes = [len(queue) for queue in self._worker_queues.values()]
        if not queue_sizes:
            return
        
        max_queue_size = max(queue_sizes)
        min_queue_size = min(queue_sizes)
        
        # If difference is significant, rebalance
        if max_queue_size - min_queue_size > 3:
            self.logger.info("Rebalancing workload across workers")
            
            # Find overloaded and underloaded workers
            overloaded_workers = [
                worker_id for worker_id, queue in self._worker_queues.items()
                if len(queue) > statistics.mean(queue_sizes) + 1
            ]
            
            underloaded_workers = [
                worker_id for worker_id, queue in self._worker_queues.items()
                if len(queue) < statistics.mean(queue_sizes) - 1
            ]
            
            # Move tasks from overloaded to underloaded workers
            for overloaded_worker in overloaded_workers:
                if not underloaded_workers:
                    break
                
                overloaded_queue = self._worker_queues[overloaded_worker]
                if len(overloaded_queue) > 1:
                    # Move the last task (lowest priority) to an underloaded worker
                    task_id = overloaded_queue.pop()
                    underloaded_worker = underloaded_workers[0]
                    self._worker_queues[underloaded_worker].append(task_id)
                    self._worker_assignments[task_id] = underloaded_worker
                    
                    self.logger.debug(
                        f"Moved task {task_id} from {overloaded_worker} to {underloaded_worker}"
                    )
                    
                    # Update underloaded workers list
                    if len(self._worker_queues[underloaded_worker]) >= statistics.mean(queue_sizes):
                        underloaded_workers.remove(underloaded_worker)
            
            # Trigger rebalance callbacks
            for callback in self._load_rebalance_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback()
                    else:
                        callback()
                except Exception as e:
                    self.logger.error(f"Error in rebalance callback: {e}")
    
    async def add_worker_failure_callback(self, callback: Callable):
        """Adds a callback for worker failure events."""
        self._worker_failure_callbacks.append(callback)
    
    async def add_load_rebalance_callback(self, callback: Callable):
        """Adds a callback for load rebalancing events."""
        self._load_rebalance_callbacks.append(callback)
    
    def get_worker_metrics(self) -> Dict[str, WorkerMetrics]:
        """Returns current worker metrics."""
        return self._worker_metrics.copy()
    
    def get_load_distribution(self) -> Dict[str, int]:
        """Returns current load distribution across workers."""
        return {
            worker_id: len(queue) 
            for worker_id, queue in self._worker_queues.items()
        }
    
    async def _update_load_scores(
        self,
        available_workers: List[str],
        worker_info: Dict[str, WorkerInfo]
    ):
        """Updates load scores for all available workers."""
        for worker_id in available_workers:
            score = 0.0
            
            # Factor 1: Current queue size (lower is better)
            queue_size = len(self._worker_queues.get(worker_id, []))
            score += queue_size * 10
            
            # Factor 2: Average processing time (lower is better)
            if worker_id in self._worker_metrics:
                metrics = self._worker_metrics[worker_id]
                score += metrics.average_task_time
                
                # Factor 3: Error rate (lower is better)
                score += metrics.consecutive_failures * 5
                
                # Factor 4: Health status
                if metrics.health_status == HealthCheckStatus.UNHEALTHY:
                    score += 100
                elif metrics.health_status == HealthCheckStatus.DEGRADED:
                    score += 20
            
            # Factor 5: Current worker status
            if worker_id in worker_info:
                if worker_info[worker_id].status == WorkerStatus.BUSY:
                    score += 50
                elif worker_info[worker_id].status == WorkerStatus.ERROR:
                    score += 200
            
            self._load_scores[worker_id] = score
    
    async def _round_robin_assignment(self, available_workers: List[str]) -> LoadBalancingResult:
        """Assigns task using round-robin strategy."""
        selected_worker = available_workers[self._round_robin_index % len(available_workers)]
        self._round_robin_index += 1
        
        return LoadBalancingResult(
            selected_worker_id=selected_worker,
            reason="Round-robin selection",
            load_scores=self._load_scores.copy(),
            strategy_used=LoadBalancingStrategy.ROUND_ROBIN
        )
    
    async def _least_loaded_assignment(self, available_workers: List[str]) -> LoadBalancingResult:
        """Assigns task to least loaded worker."""
        selected_worker = min(available_workers, key=lambda w: self._load_scores.get(w, float('inf')))
        
        return LoadBalancingResult(
            selected_worker_id=selected_worker,
            reason=f"Lowest load score: {self._load_scores.get(selected_worker, 0):.1f}",
            load_scores=self._load_scores.copy(),
            strategy_used=LoadBalancingStrategy.LEAST_LOADED
        )
    
    async def _fastest_worker_assignment(self, available_workers: List[str]) -> LoadBalancingResult:
        """Assigns task to fastest worker based on average processing time."""
        fastest_worker = None
        fastest_time = float('inf')
        
        for worker_id in available_workers:
            if worker_id in self._worker_metrics:
                avg_time = self._worker_metrics[worker_id].average_task_time
                if avg_time > 0 and avg_time < fastest_time:
                    fastest_time = avg_time
                    fastest_worker = worker_id
        
        # Fallback to least loaded if no timing data available
        if fastest_worker is None:
            return await self._least_loaded_assignment(available_workers)
        
        return LoadBalancingResult(
            selected_worker_id=fastest_worker,
            reason=f"Fastest average time: {fastest_time:.2f}s",
            load_scores=self._load_scores.copy(),
            strategy_used=LoadBalancingStrategy.FASTEST_WORKER
        )
    
    async def _priority_based_assignment(
        self,
        task_item: TaskItem,
        available_workers: List[str]
    ) -> LoadBalancingResult:
        """Assigns task based on priority and worker capabilities."""
        # For high priority tasks, use the fastest available worker
        if task_item.priority <= 1:  # High priority
            return await self._fastest_worker_assignment(available_workers)
        else:
            # For normal/low priority tasks, use least loaded
            return await self._least_loaded_assignment(available_workers)
    
    async def _health_monitor_loop(self):
        """Background health monitoring loop."""
        while self._is_monitoring:
            try:
                current_time = datetime.utcnow()
                
                # Update health status for all workers
                for worker_id, metrics in self._worker_metrics.items():
                    # Check if worker has been inactive for too long
                    if (metrics.last_health_check and 
                        (current_time - metrics.last_health_check).seconds > self.worker_timeout):
                        
                        if metrics.health_status != HealthCheckStatus.UNHEALTHY:
                            self.logger.warning(f"Worker {worker_id} health check timeout")
                            metrics.health_status = HealthCheckStatus.UNHEALTHY
                            await self._handle_unhealthy_worker(worker_id)
                    
                    # Update last health check
                    metrics.last_health_check = current_time
                
                await asyncio.sleep(self.health_check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health monitor loop: {e}")
                await asyncio.sleep(self.health_check_interval)
    
    async def _handle_unhealthy_worker(self, worker_id: str):
        """Handles an unhealthy worker."""
        self.logger.warning(f"Handling unhealthy worker: {worker_id}")
        
        # Trigger failure callbacks
        for callback in self._worker_failure_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(worker_id)
                else:
                    callback(worker_id)
            except Exception as e:
                self.logger.error(f"Error in worker failure callback: {e}")
        
        # Redistribute tasks from unhealthy worker
        if worker_id in self._worker_queues and self._worker_queues[worker_id]:
            tasks_to_redistribute = self._worker_queues[worker_id].copy()
            self._worker_queues[worker_id].clear()
            
            # Find healthy workers to redistribute tasks to
            healthy_workers = [
                wid for wid, metrics in self._worker_metrics.items()
                if metrics.health_status == HealthCheckStatus.HEALTHY and wid != worker_id
            ]
            
            if healthy_workers:
                for i, task_id in enumerate(tasks_to_redistribute):
                    target_worker = healthy_workers[i % len(healthy_workers)]
                    self._worker_queues[target_worker].append(task_id)
                    self._worker_assignments[task_id] = target_worker
                    
                    self.logger.debug(
                        f"Redistributed task {task_id} from {worker_id} to {target_worker}"
                    )
            else:
                self.logger.error("No healthy workers available for task redistribution")