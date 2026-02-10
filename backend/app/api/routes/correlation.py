from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel

from backend.app.api.deps import get_current_user, get_db
from backend.app.models.user import AppUser
from backend.app.services.correlation_service import correlation_analyzer
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


class CorrelationRequest(BaseModel):
    user_a_id: int
    user_b_id: int
    hours: int = 168


@router.get("/users/{user_id}/correlated")
async def find_correlated_users(
    user_id: int,
    group_id: Optional[int] = None,
    min_correlation: float = Query(0.3, ge=0, le=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: AppUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    results = await correlation_analyzer.find_correlated_users(
        db, user_id, group_id, min_correlation, limit
    )
    return {"correlations": results, "target_user": user_id}


@router.post("/compute")
async def compute_correlation(
    request: CorrelationRequest,
    current_user: AppUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    message_corr = await correlation_analyzer.compute_user_message_correlation(
        db, request.user_a_id, request.user_b_id, request.hours
    )
    shared = await correlation_analyzer.compute_shared_groups(
        db, request.user_a_id, request.user_b_id
    )
    
    return {
        "user_a": request.user_a_id,
        "user_b": request.user_b_id,
        "message_correlation": message_corr,
        "shared_groups": shared,
        "combined_score": (
            message_corr["correlation"] * 0.6 +
            shared["jaccard_similarity"] * 0.4
        )
    }


@router.get("/groups/{group_id}/clusters")
async def detect_clusters(
    group_id: int,
    min_cluster_size: int = Query(3, ge=2, le=20),
    min_correlation: float = Query(0.4, ge=0, le=1),
    current_user: AppUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    clusters = await correlation_analyzer.detect_clusters(
        db, group_id, min_cluster_size, min_correlation
    )
    return {"group_id": group_id, "clusters": clusters, "count": len(clusters)}


@router.get("/groups/{group_id}/leaders")
async def analyze_leaders(
    group_id: int,
    hours: int = Query(168, ge=24, le=720),
    current_user: AppUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    leaders = await correlation_analyzer.analyze_leader_follower(db, group_id, hours)
    return {"group_id": group_id, "leaders": leaders}
