from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from typing import Optional

from backend.app.db.database import get_db
from backend.app.api.deps import get_current_user
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.history import UserStory
from backend.app.models.config import GlobalConfig

router = APIRouter(prefix="/stories", tags=["stories"])


@router.get("/settings")
async def get_story_settings(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "story_check_interval_hours")
    )
    config = result.scalar_one_or_none()
    interval = float(config.value) if config and config.value else 1
    
    result2 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "story_batch_size")
    )
    config2 = result2.scalar_one_or_none()
    batch_size = int(config2.value) if config2 and config2.value else 100
    
    result3 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "story_parallel_workers")
    )
    config3 = result3.scalar_one_or_none()
    parallel_workers = int(config3.value) if config3 and config3.value else 5
    
    return {"interval_hours": interval, "batch_size": batch_size, "parallel_workers": parallel_workers}


@router.put("/settings")
async def update_story_settings(
    data: dict,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    interval = data.get("interval_hours", 1)
    batch_size = data.get("batch_size", 100)
    parallel_workers = data.get("parallel_workers", 5)
    
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "story_check_interval_hours")
    )
    config = result.scalar_one_or_none()
    if config:
        config.value = str(interval)
    else:
        config = GlobalConfig(key="story_check_interval_hours", value=str(interval))
        db.add(config)
    
    result2 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "story_batch_size")
    )
    config2 = result2.scalar_one_or_none()
    if config2:
        config2.value = str(batch_size)
    else:
        config2 = GlobalConfig(key="story_batch_size", value=str(batch_size))
        db.add(config2)
    
    result3 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "story_parallel_workers")
    )
    config3 = result3.scalar_one_or_none()
    if config3:
        config3.value = str(parallel_workers)
    else:
        config3 = GlobalConfig(key="story_parallel_workers", value=str(parallel_workers))
        db.add(config3)
    
    await db.commit()
    return {"status": "ok", "interval_hours": interval, "batch_size": batch_size, "parallel_workers": parallel_workers}


@router.get("/users")
async def get_users_with_stories(
    page: int = 1,
    limit: int = 50,
    watchlist_only: bool = False,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    from sqlalchemy import or_, exists
    from sqlalchemy.orm import aliased
    
    story_subq = select(UserStory.user_id).distinct().subquery()
    
    query = select(TelegramUser).where(
        or_(
            TelegramUser.has_stories == True,
            TelegramUser.id.in_(select(story_subq.c.user_id))
        )
    )
    
    if watchlist_only:
        query = query.where(TelegramUser.is_watchlist == True)
    
    count_query = select(func.count()).select_from(TelegramUser).where(
        or_(
            TelegramUser.has_stories == True,
            TelegramUser.id.in_(select(story_subq.c.user_id))
        )
    )
    if watchlist_only:
        count_query = count_query.where(TelegramUser.is_watchlist == True)
    count_result = await db.execute(count_query)
    total = count_result.scalar() or 0
    
    query = query.order_by(desc(TelegramUser.last_seen)).offset((page - 1) * limit).limit(limit)
    result = await db.execute(query)
    users = result.scalars().all()
    
    users_data = []
    for user in users:
        story_count = await db.execute(
            select(func.count()).select_from(UserStory).where(UserStory.user_id == user.id)
        )
        
        users_data.append({
            "id": user.id,
            "telegram_id": user.telegram_id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "photo_path": user.current_photo_path,
            "is_watchlist": user.is_watchlist,
            "is_favorite": user.is_favorite,
            "story_count": story_count.scalar() or 0,
            "last_seen": user.last_seen.isoformat() if user.last_seen else None
        })
    
    return {
        "users": users_data,
        "total": total,
        "page": page,
        "pages": (total + limit - 1) // limit
    }


@router.get("/user/{user_id}")
async def get_user_stories(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    user_result = await db.execute(
        select(TelegramUser).where(TelegramUser.id == user_id)
    )
    user = user_result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    stories_result = await db.execute(
        select(UserStory).where(UserStory.user_id == user_id).order_by(desc(UserStory.posted_at))
    )
    stories = stories_result.scalars().all()
    
    return {
        "user": {
            "id": user.id,
            "telegram_id": user.telegram_id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "photo_path": user.current_photo_path,
            "is_watchlist": user.is_watchlist
        },
        "stories": [
            {
                "id": s.id,
                "story_id": s.story_id,
                "story_type": s.story_type,
                "file_path": s.file_path,
                "caption": s.caption,
                "width": s.width,
                "height": s.height,
                "duration": s.duration,
                "views_count": s.views_count,
                "posted_at": s.posted_at.isoformat() if s.posted_at else None,
                "expires_at": s.expires_at.isoformat() if s.expires_at else None,
                "is_pinned": s.is_pinned,
                "created_at": s.created_at.isoformat() if hasattr(s, 'created_at') and s.created_at else None
            }
            for s in stories
        ]
    }


@router.post("/download-now")
async def download_stories_now(
    data: dict,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    import asyncio
    from backend.app.services.story_monitor import story_monitor
    
    if not story_monitor._running:
        raise HTTPException(status_code=400, detail="Story monitor not running")
    
    try:
        asyncio.create_task(story_monitor.force_check_now())
        return {"status": "ok", "message": "Massive story scan started in background"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/monitor-stats")
async def get_monitor_stats(
    _: dict = Depends(get_current_user)
):
    from backend.app.services.story_monitor import story_monitor
    return await story_monitor.get_stats()


@router.get("/stats")
async def get_story_stats(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    total_users = await db.execute(
        select(func.count()).select_from(TelegramUser).where(TelegramUser.has_stories == True)
    )
    
    total_stories = await db.execute(
        select(func.count()).select_from(UserStory)
    )
    
    watchlist_with_stories = await db.execute(
        select(func.count()).select_from(TelegramUser).where(
            TelegramUser.has_stories == True,
            TelegramUser.is_watchlist == True
        )
    )
    
    return {
        "users_with_stories": total_users.scalar() or 0,
        "total_stories_downloaded": total_stories.scalar() or 0,
        "watchlist_with_stories": watchlist_with_stories.scalar() or 0
    }
