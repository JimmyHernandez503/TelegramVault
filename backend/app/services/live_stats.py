import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger("live_stats")


@dataclass
class MetricWindow:
    buckets: Dict[int, int] = field(default_factory=dict)
    window_seconds: int = 60
    
    def record(self, count: int = 1):
        now_ts = int(datetime.utcnow().timestamp())
        if self.buckets:
            oldest = min(self.buckets.keys())
            if now_ts - oldest > 3900:
                self._cleanup()
        self.buckets[now_ts] = self.buckets.get(now_ts, 0) + count
    
    def get_rate_per_second(self) -> float:
        self._cleanup()
        if not self.buckets:
            return 0.0
        
        now_ts = int(datetime.utcnow().timestamp())
        cutoff = now_ts - self.window_seconds
        total = sum(c for ts, c in self.buckets.items() if ts > cutoff)
        return total / self.window_seconds
    
    def get_rate_per_minute(self) -> float:
        return self.get_rate_per_second() * 60
    
    def get_count_last_minute(self) -> int:
        now_ts = int(datetime.utcnow().timestamp())
        cutoff = now_ts - 60
        return sum(c for ts, c in self.buckets.items() if ts > cutoff)
    
    def get_count_last_hour(self) -> int:
        now_ts = int(datetime.utcnow().timestamp())
        cutoff = now_ts - 3600
        return sum(c for ts, c in self.buckets.items() if ts > cutoff)
    
    def _cleanup(self):
        cutoff = int(datetime.utcnow().timestamp()) - 3900
        old_keys = [k for k in self.buckets if k < cutoff]
        for k in old_keys:
            del self.buckets[k]


class LiveStatsService:
    def __init__(self):
        self._metrics: Dict[str, MetricWindow] = {}
        self._counters: Dict[str, int] = {}
        self._start_time = datetime.utcnow()
        self._initialize_metrics()
    
    def _initialize_metrics(self):
        metric_names = [
            "messages_processed",
            "messages_saved",
            "media_downloaded",
            "media_queued",
            "members_scraped",
            "users_enriched",
            "detections_found",
            "stories_downloaded",
            "invites_processed",
            "backfill_messages"
        ]
        for name in metric_names:
            self._metrics[name] = MetricWindow()
            self._counters[name] = 0
    
    def record(self, metric_name: str, count: int = 1):
        if metric_name not in self._metrics:
            self._metrics[metric_name] = MetricWindow()
            self._counters[metric_name] = 0
        
        self._metrics[metric_name].record(count)
        self._counters[metric_name] += count
    
    def get_rate(self, metric_name: str, per: str = "second") -> float:
        if metric_name not in self._metrics:
            return 0.0
        
        if per == "minute":
            return self._metrics[metric_name].get_rate_per_minute()
        return self._metrics[metric_name].get_rate_per_second()
    
    def get_count_last_minute(self, metric_name: str) -> int:
        if metric_name not in self._metrics:
            return 0
        return self._metrics[metric_name].get_count_last_minute()
    
    def get_count_last_hour(self, metric_name: str) -> int:
        if metric_name not in self._metrics:
            return 0
        return self._metrics[metric_name].get_count_last_hour()
    
    def get_total(self, metric_name: str) -> int:
        return self._counters.get(metric_name, 0)
    
    def get_uptime_seconds(self) -> int:
        return int((datetime.utcnow() - self._start_time).total_seconds())
    
    def get_all_stats(self) -> Dict[str, Any]:
        stats = {
            "uptime_seconds": self.get_uptime_seconds(),
            "metrics": {}
        }
        
        for name in self._metrics:
            stats["metrics"][name] = {
                "per_second": round(self.get_rate(name, "second"), 2),
                "per_minute": round(self.get_rate(name, "minute"), 1),
                "last_minute": self.get_count_last_minute(name),
                "last_hour": self.get_count_last_hour(name),
                "total_session": self.get_total(name)
            }
        
        return stats
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            "uptime_seconds": self.get_uptime_seconds(),
            "messages": {
                "per_second": round(self.get_rate("messages_saved", "second"), 2),
                "per_minute": round(self.get_rate("messages_saved", "minute"), 1),
                "last_minute": self.get_count_last_minute("messages_saved"),
                "last_hour": self.get_count_last_hour("messages_saved")
            },
            "media": {
                "per_second": round(self.get_rate("media_downloaded", "second"), 2),
                "per_minute": round(self.get_rate("media_downloaded", "minute"), 1),
                "last_minute": self.get_count_last_minute("media_downloaded"),
                "last_hour": self.get_count_last_hour("media_downloaded"),
                "queued": self.get_count_last_minute("media_queued")
            },
            "members": {
                "per_second": round(self.get_rate("members_scraped", "second"), 2),
                "per_minute": round(self.get_rate("members_scraped", "minute"), 1),
                "last_minute": self.get_count_last_minute("members_scraped"),
                "last_hour": self.get_count_last_hour("members_scraped")
            },
            "detections": {
                "per_second": round(self.get_rate("detections_found", "second"), 2),
                "per_minute": round(self.get_rate("detections_found", "minute"), 1),
                "last_minute": self.get_count_last_minute("detections_found"),
                "last_hour": self.get_count_last_hour("detections_found")
            },
            "users": {
                "per_second": round(self.get_rate("users_enriched", "second"), 2),
                "per_minute": round(self.get_rate("users_enriched", "minute"), 1),
                "last_minute": self.get_count_last_minute("users_enriched"),
                "last_hour": self.get_count_last_hour("users_enriched")
            },
            "stories": {
                "per_minute": round(self.get_rate("stories_downloaded", "minute"), 1),
                "last_hour": self.get_count_last_hour("stories_downloaded")
            },
            "backfill": {
                "per_second": round(self.get_rate("backfill_messages", "second"), 2),
                "per_minute": round(self.get_rate("backfill_messages", "minute"), 1),
                "last_minute": self.get_count_last_minute("backfill_messages")
            }
        }


live_stats = LiveStatsService()
