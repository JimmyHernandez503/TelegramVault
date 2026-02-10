from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from typing import Optional

from backend.app.db.database import get_db
from backend.app.models.config import GlobalConfig
from backend.app.api.deps import get_current_user

router = APIRouter()


class ProfilePhotoScanSettings(BaseModel):
    interval_hours: int = 24
    batch_size: int = 50
    parallel_workers: int = 3
    enabled: bool = True


@router.get("/settings")
async def get_profile_photo_settings(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "photo_scan_interval_hours")
    )
    interval_config = result.scalar_one_or_none()
    
    result2 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "photo_scan_batch_size")
    )
    batch_config = result2.scalar_one_or_none()
    
    result3 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "photo_scan_parallel_workers")
    )
    workers_config = result3.scalar_one_or_none()
    
    result4 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "photo_scan_enabled")
    )
    enabled_config = result4.scalar_one_or_none()
    
    return {
        "interval_hours": int(interval_config.value) if interval_config and interval_config.value else 24,
        "batch_size": int(batch_config.value) if batch_config and batch_config.value else 50,
        "parallel_workers": int(workers_config.value) if workers_config and workers_config.value else 3,
        "enabled": enabled_config.value.lower() in ("true", "1", "yes") if enabled_config and enabled_config.value else True
    }


@router.put("/settings")
async def update_profile_photo_settings(
    settings: ProfilePhotoScanSettings,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    configs = [
        ("photo_scan_interval_hours", str(settings.interval_hours), "int"),
        ("photo_scan_batch_size", str(settings.batch_size), "int"),
        ("photo_scan_parallel_workers", str(settings.parallel_workers), "int"),
        ("photo_scan_enabled", "true" if settings.enabled else "false", "bool"),
    ]
    
    for key, value, value_type in configs:
        result = await db.execute(select(GlobalConfig).where(GlobalConfig.key == key))
        config = result.scalar_one_or_none()
        
        if config:
            config.value = value
        else:
            config = GlobalConfig(key=key, value=value, value_type=value_type)
            db.add(config)
    
    await db.commit()
    
    return {"success": True, "message": "Configuracion de escaneo de fotos actualizada"}


@router.post("/scan-now")
async def trigger_profile_photo_scan(
    current_user = Depends(get_current_user)
):
    from backend.app.services.profile_photo_scanner import profile_photo_scanner
    
    if not profile_photo_scanner._running:
        return {"success": False, "message": "El scanner no esta activo"}
    
    profile_photo_scanner.trigger_scan()
    
    return {
        "success": True,
        "message": "Escaneo de fotos de perfil iniciado"
    }


@router.get("/status")
async def get_profile_photo_scanner_status(
    current_user = Depends(get_current_user)
):
    from backend.app.services.profile_photo_scanner import profile_photo_scanner
    
    return profile_photo_scanner.get_status()
