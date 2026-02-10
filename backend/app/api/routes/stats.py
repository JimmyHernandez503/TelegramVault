from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from typing import Any, Dict, List

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.telegram_account import TelegramAccount
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.media import MediaFile
from backend.app.models.detection import Detection
from backend.app.models.invite import InviteLink
from backend.app.schemas.stats import DashboardStats
from backend.app.services.live_stats import live_stats

router = APIRouter()


@router.get("/public/system")
async def get_public_system_stats(
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Public endpoint for system statistics (no authentication required).
    Used for login page display.
    """
    result = await db.execute(text("""
        SELECT 
            (SELECT COUNT(*) FROM telegram_users) as total_users,
            (SELECT COUNT(*) FROM telegram_messages) as total_messages,
            (SELECT COALESCE(SUM(file_size), 0) FROM media_files) as total_storage,
            (SELECT COUNT(*) FROM telegram_accounts WHERE status IN ('connected', 'active')) as active_accounts
    """))
    row = result.first()
    
    if not row:
        return {
            "users": 0,
            "messages": 0,
            "storage_gb": 0,
            "uptime": 99.9
        }
    
    # Convert storage from bytes to GB
    storage_gb = round((row.total_storage or 0) / (1024 ** 3), 2)
    
    # Calculate uptime based on active accounts (simplified metric)
    uptime = 99.9 if row.active_accounts > 0 else 0.0
    
    return {
        "users": row.total_users or 0,
        "messages": row.total_messages or 0,
        "storage_gb": float(storage_gb),
        "uptime": float(uptime)
    }


@router.get("/live")
async def get_live_stats(
    current_user: AppUser = Depends(get_current_user)
) -> Dict[str, Any]:
    return live_stats.get_summary()


@router.get("/live/detailed")
async def get_live_stats_detailed(
    current_user: AppUser = Depends(get_current_user)
) -> Dict[str, Any]:
    return live_stats.get_all_stats()


@router.get("/services")
async def get_services_status(
    current_user: AppUser = Depends(get_current_user)
) -> Dict[str, Any]:
    from backend.app.services.client_load_balancer import load_balancer
    from backend.app.services.backfill_service import backfill_service
    from backend.app.services.live_monitor import live_monitor
    from backend.app.services.autojoin_service import autojoin_service
    from backend.app.services.story_monitor import story_monitor
    from backend.app.services.member_scrape_scheduler import member_scrape_scheduler
    from backend.app.services.user_enricher import user_enricher
    from backend.app.services.media_ingestion import media_ingestion
    from backend.app.services.rate_limit_manager import rate_limit_manager
    from backend.app.services.profile_photo_scanner import profile_photo_scanner
    from backend.app.services.media_retry_service import media_retry_service
    
    services = []
    
    lb_stats = load_balancer.get_stats()
    services.append({
        "name": "Load Balancer",
        "status": "running" if lb_stats["available_clients"] > 0 else "degraded",
        "details": {
            "total_clients": lb_stats["total_clients"],
            "available_clients": lb_stats["available_clients"],
            "accounts": lb_stats["accounts"]
        }
    })
    
    backfill_status = backfill_service.get_status()
    services.append({
        "name": "Backfill Service",
        "status": "running" if backfill_status["active_count"] > 0 else "idle",
        "details": backfill_status
    })
    
    monitor_status = live_monitor.get_status()
    services.append({
        "name": "Live Monitor",
        "status": "running" if monitor_status["active_monitors"] > 0 else "idle",
        "details": monitor_status
    })
    
    autojoin_status = autojoin_service.get_status()
    services.append({
        "name": "AutoJoin Service",
        "status": "running" if autojoin_status.get("running") else "stopped",
        "details": autojoin_status
    })
    
    story_status = story_monitor.get_status()
    services.append({
        "name": "Story Monitor",
        "status": "running" if story_status.get("running") else "stopped",
        "details": story_status
    })
    
    scheduler_status = member_scrape_scheduler.get_status()
    services.append({
        "name": "Member Scrape Scheduler",
        "status": "running" if scheduler_status.get("running") else "stopped",
        "details": scheduler_status
    })
    
    enricher_status = user_enricher.get_status()
    services.append({
        "name": "User Enricher",
        "status": "running" if enricher_status.get("running") else "idle",
        "details": enricher_status
    })
    
    media_status = media_ingestion.get_status()
    services.append({
        "name": "Media Ingestion",
        "status": "running" if media_status.get("running") else "idle",
        "details": media_status
    })
    
    rate_limit_status = rate_limit_manager.get_status()
    rl_status = "normal"
    if rate_limit_status.get("global_slowdown"):
        rl_status = "slowdown"
    elif rate_limit_status.get("potentially_banned_accounts", 0) > 0:
        rl_status = "degraded"
    elif rate_limit_status.get("blocked_accounts", 0) > 0:
        rl_status = "throttled"
    
    services.append({
        "name": "Rate Limit Manager",
        "status": rl_status,
        "details": rate_limit_status
    })
    
    photo_scanner_status = profile_photo_scanner.get_status()
    services.append({
        "name": "Profile Photo Scanner",
        "status": "scanning" if photo_scanner_status.get("is_scanning") else ("running" if photo_scanner_status.get("running") else "stopped"),
        "details": photo_scanner_status
    })
    
    media_retry_status = media_retry_service.get_status()
    services.append({
        "name": "Media Retry",
        "status": "running" if media_retry_status.get("running") else "stopped",
        "details": media_retry_status
    })
    
    return {
        "services": services,
        "summary": {
            "total": len(services),
            "running": sum(1 for s in services if s["status"] == "running"),
            "idle": sum(1 for s in services if s["status"] == "idle"),
            "stopped": sum(1 for s in services if s["status"] == "stopped"),
            "degraded": sum(1 for s in services if s["status"] in ["degraded", "slowdown", "throttled"])
        }
    }


@router.get("/dashboard", response_model=DashboardStats)
async def get_dashboard_stats(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(text("""
        SELECT 
            (SELECT COUNT(*) FROM telegram_messages) as total_messages,
            (SELECT COUNT(*) FROM telegram_users) as total_users,
            (SELECT COUNT(*) FROM telegram_groups) as total_groups,
            (SELECT COUNT(*) FROM media_files) as total_media,
            (SELECT COUNT(*) FROM detections) as total_detections,
            (SELECT COUNT(*) FROM telegram_accounts) as total_accounts,
            (SELECT COUNT(*) FROM telegram_accounts WHERE status IN ('connected', 'active')) as active_accounts,
            (SELECT COUNT(*) FROM invite_links WHERE status = 'pending') as pending_invites,
            (SELECT COUNT(*) FROM telegram_groups WHERE status = 'backfilling') as backfills_in_progress
    """))
    row = result.first()
    
    if not row:
        return DashboardStats(
            total_messages=0, total_users=0, total_groups=0, total_media=0,
            total_detections=0, active_accounts=0, total_accounts=0,
            pending_invites=0, backfills_in_progress=0
        )
    
    return DashboardStats(
        total_messages=row.total_messages or 0,
        total_users=row.total_users or 0,
        total_groups=row.total_groups or 0,
        total_media=row.total_media or 0,
        total_detections=row.total_detections or 0,
        active_accounts=row.active_accounts or 0,
        total_accounts=row.total_accounts or 0,
        pending_invites=row.pending_invites or 0,
        backfills_in_progress=row.backfills_in_progress or 0
    )
