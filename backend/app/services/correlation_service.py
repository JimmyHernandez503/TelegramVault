import asyncio
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import math

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.membership import GroupMembership
from backend.app.models.user_activity import UserActivity, UserCorrelation


class CorrelationAnalyzer:
    
    def __init__(self):
        self.cache: Dict[str, Any] = {}
        self.cache_ttl = 3600
    
    async def compute_user_message_correlation(
        self,
        db: AsyncSession,
        user_a_id: int,
        user_b_id: int,
        hours: int = 168
    ) -> Dict[str, Any]:
        since = datetime.utcnow() - timedelta(hours=hours)
        
        result_a = await db.execute(
            select(TelegramMessage.date)
            .where(and_(
                TelegramMessage.sender_id == user_a_id,
                TelegramMessage.date >= since
            ))
            .order_by(TelegramMessage.date)
        )
        times_a = [r[0] for r in result_a.fetchall()]
        
        result_b = await db.execute(
            select(TelegramMessage.date)
            .where(and_(
                TelegramMessage.sender_id == user_b_id,
                TelegramMessage.date >= since
            ))
            .order_by(TelegramMessage.date)
        )
        times_b = [r[0] for r in result_b.fetchall()]
        
        if len(times_a) < 5 or len(times_b) < 5:
            return {
                "correlation": 0,
                "sample_size": min(len(times_a), len(times_b)),
                "sufficient_data": False
            }
        
        hourly_a = self._to_hourly_bins(times_a, since, hours)
        hourly_b = self._to_hourly_bins(times_b, since, hours)
        
        correlation = self._pearson_correlation(hourly_a, hourly_b)
        
        return {
            "correlation": correlation,
            "sample_size": len(times_a) + len(times_b),
            "sufficient_data": True,
            "user_a_messages": len(times_a),
            "user_b_messages": len(times_b)
        }
    
    def _to_hourly_bins(self, timestamps: List[datetime], start: datetime, hours: int) -> List[int]:
        bins = [0] * hours
        for ts in timestamps:
            hour_idx = int((ts - start).total_seconds() // 3600)
            if 0 <= hour_idx < hours:
                bins[hour_idx] += 1
        return bins
    
    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        n = len(x)
        if n == 0:
            return 0
        
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        
        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        
        sum_sq_x = sum((xi - mean_x) ** 2 for xi in x)
        sum_sq_y = sum((yi - mean_y) ** 2 for yi in y)
        
        denominator = math.sqrt(sum_sq_x * sum_sq_y)
        
        if denominator == 0:
            return 0
        
        return numerator / denominator
    
    async def compute_shared_groups(
        self,
        db: AsyncSession,
        user_a_id: int,
        user_b_id: int
    ) -> Dict[str, Any]:
        result_a = await db.execute(
            select(GroupMembership.group_id)
            .where(GroupMembership.user_id == user_a_id)
        )
        groups_a = set(r[0] for r in result_a.fetchall())
        
        result_b = await db.execute(
            select(GroupMembership.group_id)
            .where(GroupMembership.user_id == user_b_id)
        )
        groups_b = set(r[0] for r in result_b.fetchall())
        
        shared = groups_a & groups_b
        all_groups = groups_a | groups_b
        
        jaccard = len(shared) / len(all_groups) if all_groups else 0
        
        return {
            "shared_groups": len(shared),
            "user_a_groups": len(groups_a),
            "user_b_groups": len(groups_b),
            "jaccard_similarity": jaccard
        }
    
    async def find_correlated_users(
        self,
        db: AsyncSession,
        target_user_id: int,
        group_id: Optional[int] = None,
        min_correlation: float = 0.5,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        if group_id:
            result = await db.execute(
                select(GroupMembership.user_id)
                .where(and_(
                    GroupMembership.group_id == group_id,
                    GroupMembership.user_id != target_user_id
                ))
                .limit(100)
            )
        else:
            target_groups = await db.execute(
                select(GroupMembership.group_id)
                .where(GroupMembership.user_id == target_user_id)
            )
            group_ids = [r[0] for r in target_groups.fetchall()]
            
            if not group_ids:
                return []
            
            result = await db.execute(
                select(GroupMembership.user_id)
                .where(and_(
                    GroupMembership.group_id.in_(group_ids),
                    GroupMembership.user_id != target_user_id
                ))
                .distinct()
                .limit(100)
            )
        
        candidate_ids = [r[0] for r in result.fetchall()]
        
        correlations = []
        for other_id in candidate_ids:
            corr = await self.compute_user_message_correlation(db, target_user_id, other_id)
            shared = await self.compute_shared_groups(db, target_user_id, other_id)
            
            combined_score = (
                corr["correlation"] * 0.6 +
                shared["jaccard_similarity"] * 0.4
            )
            
            if combined_score >= min_correlation or corr["correlation"] >= min_correlation:
                correlations.append({
                    "user_id": other_id,
                    "message_correlation": corr["correlation"],
                    "shared_groups": shared["shared_groups"],
                    "jaccard_similarity": shared["jaccard_similarity"],
                    "combined_score": combined_score,
                    "sample_size": corr["sample_size"]
                })
        
        correlations.sort(key=lambda x: x["combined_score"], reverse=True)
        return correlations[:limit]
    
    async def detect_clusters(
        self,
        db: AsyncSession,
        group_id: int,
        min_cluster_size: int = 3,
        min_correlation: float = 0.4
    ) -> List[Dict[str, Any]]:
        result = await db.execute(
            select(GroupMembership.user_id)
            .where(GroupMembership.group_id == group_id)
            .limit(200)
        )
        user_ids = [r[0] for r in result.fetchall()]
        
        if len(user_ids) < min_cluster_size:
            return []
        
        adjacency: Dict[int, List[int]] = defaultdict(list)
        
        for i, user_a in enumerate(user_ids[:50]):
            for user_b in user_ids[i+1:50]:
                corr = await self.compute_user_message_correlation(db, user_a, user_b, hours=72)
                if corr["correlation"] >= min_correlation:
                    adjacency[user_a].append(user_b)
                    adjacency[user_b].append(user_a)
        
        visited = set()
        clusters = []
        
        for user_id in user_ids[:50]:
            if user_id in visited:
                continue
            
            cluster = []
            stack = [user_id]
            
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                cluster.append(current)
                
                for neighbor in adjacency.get(current, []):
                    if neighbor not in visited:
                        stack.append(neighbor)
            
            if len(cluster) >= min_cluster_size:
                clusters.append({
                    "user_ids": cluster,
                    "size": len(cluster),
                    "density": len(adjacency.get(cluster[0], [])) / len(cluster) if cluster else 0
                })
        
        return clusters
    
    async def analyze_leader_follower(
        self,
        db: AsyncSession,
        group_id: int,
        hours: int = 168
    ) -> List[Dict[str, Any]]:
        since = datetime.utcnow() - timedelta(hours=hours)
        
        result = await db.execute(
            select(
                TelegramMessage.sender_id,
                func.count().label('msg_count'),
                func.count(TelegramMessage.reply_to_msg_id.isnot(None)).label('replies_received')
            )
            .where(and_(
                TelegramMessage.group_id == group_id,
                TelegramMessage.date >= since
            ))
            .group_by(TelegramMessage.sender_id)
            .order_by(func.count().desc())
            .limit(20)
        )
        
        leaders = []
        for row in result.fetchall():
            leaders.append({
                "user_id": row[0],
                "message_count": row[1],
                "replies_received": row[2] or 0,
                "influence_score": row[1] * 0.3 + (row[2] or 0) * 0.7
            })
        
        leaders.sort(key=lambda x: x["influence_score"], reverse=True)
        return leaders


correlation_analyzer = CorrelationAnalyzer()
