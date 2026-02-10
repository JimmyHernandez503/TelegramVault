from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.db.database import get_db
from backend.app.api.deps import get_current_user
from backend.app.models.user import AppUser
from backend.app.services.search_service import search_service

router = APIRouter()


@router.get("/")
async def global_search(
    q: str = Query(..., min_length=2, description="Search query"),
    group_id: Optional[int] = Query(None, description="Filter by group ID"),
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    types: Optional[str] = Query(None, description="Comma-separated types: messages,users,detections"),
    date_from: Optional[str] = Query(None, description="Start date (ISO format)"),
    date_to: Optional[str] = Query(None, description="End date (ISO format)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    source_types = None
    if types:
        source_types = [t.strip() for t in types.split(",") if t.strip()]
    
    parsed_date_from = None
    parsed_date_to = None
    
    if date_from:
        try:
            parsed_date_from = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
        except ValueError:
            pass
    
    if date_to:
        try:
            parsed_date_to = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
        except ValueError:
            pass
    
    results = await search_service.search_all(
        db=db,
        query=q,
        group_id=group_id,
        user_id=user_id,
        source_types=source_types,
        date_from=parsed_date_from,
        date_to=parsed_date_to,
        limit=limit,
        offset=offset
    )
    
    return results


@router.post("/rebuild-index")
async def rebuild_search_index(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    counts = await search_service.rebuild_search_index(db)
    return {
        "success": True,
        "message": "Search index rebuilt successfully",
        "indexed": counts
    }


@router.get("/suggestions")
async def get_search_suggestions(
    q: str = Query(..., min_length=2),
    limit: int = Query(10, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from sqlalchemy import text
    
    suggestions = []
    
    try:
        result = await db.execute(
            text("""
                SELECT DISTINCT username 
                FROM telegram_users 
                WHERE username ILIKE :pattern
                LIMIT :limit
            """),
            {"pattern": f"%{q}%", "limit": limit}
        )
        usernames = [row[0] for row in result.fetchall() if row[0]]
        suggestions.extend([{"type": "user", "value": f"@{u}"} for u in usernames])
        
        result = await db.execute(
            text("""
                SELECT DISTINCT title 
                FROM telegram_groups 
                WHERE title ILIKE :pattern
                LIMIT :limit
            """),
            {"pattern": f"%{q}%", "limit": limit}
        )
        titles = [row[0] for row in result.fetchall() if row[0]]
        suggestions.extend([{"type": "group", "value": t} for t in titles])
        
    except Exception as e:
        print(f"[Search] Suggestions error: {e}")
    
    return {"suggestions": suggestions[:limit]}
