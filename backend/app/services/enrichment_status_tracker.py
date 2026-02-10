"""
Enrichment Status Tracker

Tracks user enrichment operations to prevent duplicates and provide status information.
Thread-safe using asyncio locks.
"""

from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
import asyncio


class EnrichmentStatus(Enum):
    """Status of user enrichment operation"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class EnrichmentOperation:
    """Tracks a single enrichment operation"""
    telegram_id: int
    status: EnrichmentStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0


class EnrichmentStatusTracker:
    """
    Tracks enrichment operations to prevent duplicates and provide status.
    Thread-safe using asyncio locks.
    """
    
    def __init__(self, cache_ttl_seconds: int = 300):
        self._operations: Dict[int, EnrichmentOperation] = {}
        self._lock = asyncio.Lock()
        self._cache_ttl = cache_ttl_seconds
        
        # Metrics tracking
        self._metrics = {
            "total_enrichments": 0,
            "successful_enrichments": 0,
            "failed_enrichments": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "duplicate_requests_prevented": 0,
            "total_enrichment_time_ms": 0,
            "average_enrichment_time_ms": 0
        }
    
    async def is_enrichment_needed(self, telegram_id: int) -> bool:
        """Check if enrichment is needed for a user"""
        async with self._lock:
            if telegram_id not in self._operations:
                self._metrics["cache_misses"] += 1
                return True
            
            op = self._operations[telegram_id]
            
            # If completed or failed, check if cache expired
            if op.status in [EnrichmentStatus.COMPLETED, EnrichmentStatus.FAILED]:
                elapsed = (datetime.utcnow() - op.started_at).total_seconds()
                if elapsed > self._cache_ttl:
                    del self._operations[telegram_id]
                    self._metrics["cache_misses"] += 1
                    return True
                self._metrics["cache_hits"] += 1
                return False
            
            # If pending or in progress, no need to enrich again
            self._metrics["duplicate_requests_prevented"] += 1
            return False
    
    async def start_enrichment(self, telegram_id: int) -> bool:
        """Mark enrichment as started. Returns False if already in progress."""
        async with self._lock:
            if telegram_id in self._operations:
                op = self._operations[telegram_id]
                if op.status in [EnrichmentStatus.PENDING, EnrichmentStatus.IN_PROGRESS]:
                    return False
            
            self._operations[telegram_id] = EnrichmentOperation(
                telegram_id=telegram_id,
                status=EnrichmentStatus.IN_PROGRESS,
                started_at=datetime.utcnow()
            )
            self._metrics["total_enrichments"] += 1
            return True
    
    async def complete_enrichment(self, telegram_id: int) -> None:
        """Mark enrichment as completed"""
        async with self._lock:
            if telegram_id in self._operations:
                op = self._operations[telegram_id]
                op.status = EnrichmentStatus.COMPLETED
                op.completed_at = datetime.utcnow()
                
                # Update metrics
                self._metrics["successful_enrichments"] += 1
                
                # Calculate enrichment time
                enrichment_time_ms = (op.completed_at - op.started_at).total_seconds() * 1000
                self._metrics["total_enrichment_time_ms"] += enrichment_time_ms
                
                # Update average
                if self._metrics["successful_enrichments"] > 0:
                    self._metrics["average_enrichment_time_ms"] = (
                        self._metrics["total_enrichment_time_ms"] / 
                        self._metrics["successful_enrichments"]
                    )
    
    async def fail_enrichment(self, telegram_id: int, error_message: str) -> None:
        """Mark enrichment as failed"""
        async with self._lock:
            if telegram_id in self._operations:
                op = self._operations[telegram_id]
                op.status = EnrichmentStatus.FAILED
                op.completed_at = datetime.utcnow()
                op.error_message = error_message
                op.retry_count += 1
                
                # Update metrics
                self._metrics["failed_enrichments"] += 1
    
    async def get_status(self, telegram_id: int) -> Optional[EnrichmentStatus]:
        """Get current enrichment status for a user"""
        async with self._lock:
            if telegram_id in self._operations:
                return self._operations[telegram_id].status
            return None
    
    async def cleanup_expired(self) -> int:
        """Remove expired cache entries. Returns count of removed entries."""
        async with self._lock:
            now = datetime.utcnow()
            expired = []
            
            for telegram_id, op in self._operations.items():
                if op.status in [EnrichmentStatus.COMPLETED, EnrichmentStatus.FAILED]:
                    elapsed = (now - op.started_at).total_seconds()
                    if elapsed > self._cache_ttl:
                        expired.append(telegram_id)
            
            for telegram_id in expired:
                del self._operations[telegram_id]
            
            return len(expired)
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get enrichment metrics"""
        async with self._lock:
            # Calculate success rate
            total_completed = self._metrics["successful_enrichments"] + self._metrics["failed_enrichments"]
            success_rate = (
                self._metrics["successful_enrichments"] / total_completed 
                if total_completed > 0 else 0
            )
            
            # Calculate cache hit rate
            total_cache_checks = self._metrics["cache_hits"] + self._metrics["cache_misses"]
            cache_hit_rate = (
                self._metrics["cache_hits"] / total_cache_checks 
                if total_cache_checks > 0 else 0
            )
            
            return {
                "total_enrichments": self._metrics["total_enrichments"],
                "successful_enrichments": self._metrics["successful_enrichments"],
                "failed_enrichments": self._metrics["failed_enrichments"],
                "success_rate": round(success_rate * 100, 2),
                "cache_hits": self._metrics["cache_hits"],
                "cache_misses": self._metrics["cache_misses"],
                "cache_hit_rate": round(cache_hit_rate * 100, 2),
                "duplicate_requests_prevented": self._metrics["duplicate_requests_prevented"],
                "average_enrichment_time_ms": round(self._metrics["average_enrichment_time_ms"], 2),
                "active_operations": len(self._operations),
                "cache_ttl_seconds": self._cache_ttl
            }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get detailed enrichment statistics"""
        async with self._lock:
            # Count operations by status
            status_counts = {
                "pending": 0,
                "in_progress": 0,
                "completed": 0,
                "failed": 0
            }
            
            for op in self._operations.values():
                status_counts[op.status.value] += 1
            
            # Get recent failures
            recent_failures = []
            for telegram_id, op in self._operations.items():
                if op.status == EnrichmentStatus.FAILED and op.error_message:
                    recent_failures.append({
                        "telegram_id": telegram_id,
                        "error": op.error_message,
                        "retry_count": op.retry_count,
                        "failed_at": op.completed_at.isoformat() if op.completed_at else None
                    })
            
            # Sort by most recent
            recent_failures.sort(
                key=lambda x: x["failed_at"] if x["failed_at"] else "", 
                reverse=True
            )
            
            return {
                "status_counts": status_counts,
                "recent_failures": recent_failures[:10],  # Last 10 failures
                "metrics": await self.get_metrics()
            }
