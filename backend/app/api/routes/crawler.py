from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from typing import Optional

from backend.app.db.database import get_db
from backend.app.models.config import GlobalConfig
from backend.app.api.deps import get_current_user

router = APIRouter()


class CrawlerServerSettings(BaseModel):
    enabled: bool = False
    port: int = 8080


@router.get("/settings")
async def get_crawler_settings(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "crawler_server_enabled")
    )
    enabled_config = result.scalar_one_or_none()
    
    result2 = await db.execute(
        select(GlobalConfig).where(GlobalConfig.key == "crawler_server_port")
    )
    port_config = result2.scalar_one_or_none()
    
    return {
        "enabled": enabled_config.value.lower() in ("true", "1", "yes") if enabled_config and enabled_config.value else False,
        "port": int(port_config.value) if port_config and port_config.value else 8080
    }


@router.put("/settings")
async def update_crawler_settings(
    settings: CrawlerServerSettings,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    configs = [
        ("crawler_server_enabled", "true" if settings.enabled else "false", "bool"),
        ("crawler_server_port", str(settings.port), "int"),
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
    
    return {"success": True, "message": "Configuracion del servidor de crawler actualizada"}


@router.get("/status")
async def get_crawler_status(
    current_user = Depends(get_current_user)
):
    import os
    is_running = os.path.exists("/tmp/crawler_server.pid")
    
    return {
        "running": is_running,
        "url": "http://0.0.0.0:8080" if is_running else None
    }
