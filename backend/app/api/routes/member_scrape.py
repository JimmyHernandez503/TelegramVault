from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Optional
from backend.app.db.database import get_db
from backend.app.models.config import GlobalConfig
from backend.app.models.telegram_group import TelegramGroup
from backend.app.api.routes.auth import get_current_user
from backend.app.services.member_scrape_scheduler import get_member_scrape_scheduler

router = APIRouter(prefix="/member-scrape", tags=["member-scrape"])


class ScrapeIntervalUpdate(BaseModel):
    interval_hours: int


class ManualScrapeRequest(BaseModel):
    group_ids: Optional[list[int]] = None


@router.get("/settings")
async def get_scrape_settings(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "member_scrape_interval_hours")
    )
    config = result.scalar_one_or_none()
    
    interval_hours = 24
    if config and config.value:
        try:
            interval_hours = int(config.value)
        except ValueError:
            pass
    
    return {
        "interval_hours": interval_hours,
        "enabled": interval_hours > 0
    }


@router.put("/settings")
async def update_scrape_settings(
    data: ScrapeIntervalUpdate,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "member_scrape_interval_hours")
    )
    config = result.scalar_one_or_none()
    
    if config:
        config.value = str(data.interval_hours)
        config.value_type = "int"
    else:
        config = GlobalConfig(
            key="member_scrape_interval_hours",
            value=str(data.interval_hours),
            value_type="int"
        )
        db.add(config)
    
    await db.commit()
    
    return {
        "interval_hours": data.interval_hours,
        "enabled": data.interval_hours > 0,
        "message": f"Member scrape interval set to {data.interval_hours} hours" if data.interval_hours > 0 else "Automatic member scraping disabled"
    }


@router.get("/groups")
async def get_scrape_eligible_groups(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(
        select(TelegramGroup).where(
            TelegramGroup.group_type.in_(["group", "supergroup", "megagroup"]),
            TelegramGroup.assigned_account_id.isnot(None)
        ).order_by(TelegramGroup.title)
    )
    groups = result.scalars().all()
    
    return [
        {
            "id": g.id,
            "title": g.title,
            "member_count": g.member_count,
            "is_monitoring": g.is_monitoring,
            "last_member_scrape_at": g.last_member_scrape_at.isoformat() if g.last_member_scrape_at else None,
            "group_type": g.group_type
        }
        for g in groups
    ]


@router.post("/scrape-now")
async def scrape_members_now(
    data: ManualScrapeRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    scheduler = get_member_scrape_scheduler()
    if not scheduler:
        raise HTTPException(status_code=500, detail="Member scrape scheduler not initialized")
    
    async def run_scrape():
        await scheduler.scrape_now(data.group_ids)
    
    background_tasks.add_task(run_scrape)
    
    group_count = len(data.group_ids) if data.group_ids else "all monitored"
    return {
        "status": "started",
        "message": f"Started member scrape for {group_count} groups"
    }


@router.post("/scrape-all")
async def scrape_all_members(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    scheduler = get_member_scrape_scheduler()
    if not scheduler:
        raise HTTPException(status_code=500, detail="Member scrape scheduler not initialized")
    
    async def run_scrape():
        await scheduler.scrape_now(None)
    
    background_tasks.add_task(run_scrape)
    
    return {
        "status": "started",
        "message": "Started member scrape for all monitored groups"
    }
