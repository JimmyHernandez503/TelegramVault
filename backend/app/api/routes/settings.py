from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from typing import Optional, Dict, Any
import json

from backend.app.db.database import get_db
from backend.app.models.config import GlobalConfig, DomainWatchlist
from backend.app.api.deps import get_current_user

router = APIRouter()


class ConfigUpdate(BaseModel):
    key: str
    value: str
    value_type: str = "string"


class ConfigBulkUpdate(BaseModel):
    configs: Dict[str, Any]


class DomainWatchlistCreate(BaseModel):
    domain: str
    description: Optional[str] = None


DEFAULT_CONFIGS = {
    "default_api_id": {"value": "", "type": "string", "label": "API ID por defecto", "category": "telegram"},
    "default_api_hash": {"value": "", "type": "string", "label": "API Hash por defecto", "category": "telegram"},
    "ocr_enabled": {"value": "true", "type": "bool", "label": "OCR habilitado globalmente", "category": "ocr"},
    "ocr_languages": {"value": "spa+eng", "type": "string", "label": "Idiomas OCR", "category": "ocr"},
    "auto_backfill": {"value": "true", "type": "bool", "label": "Backfill automatico al unirse", "category": "backfill"},
    "backfill_media": {"value": "true", "type": "bool", "label": "Incluir multimedia en backfill", "category": "backfill"},
    "backfill_members": {"value": "true", "type": "bool", "label": "Scraping de miembros en backfill", "category": "backfill"},
    "download_media": {"value": "true", "type": "bool", "label": "Descargar multimedia", "category": "media"},
    "download_documents": {"value": "true", "type": "bool", "label": "Descargar documentos", "category": "media"},
    "max_document_size_mb": {"value": "100", "type": "int", "label": "Tamaño maximo documentos (MB)", "category": "media"},
    "allowed_extensions": {"value": "pdf,doc,docx,xls,xlsx,txt,csv,zip,rar,7z,json,xml", "type": "string", "label": "Extensiones permitidas", "category": "media"},
    "rate_limit_mode": {"value": "balanced", "type": "string", "label": "Modo rate limiting", "category": "rate_limit"},
    "bypass_restrictions": {"value": "true", "type": "bool", "label": "Bypass de restricciones noforwards", "category": "advanced"},
    "parallel_downloads": {"value": "5", "type": "int", "label": "Descargas paralelas", "category": "advanced"},
    "detect_coordinated": {"value": "true", "type": "bool", "label": "Detectar cuentas coordinadas", "category": "detection"},
    "coordination_window_minutes": {"value": "5", "type": "int", "label": "Ventana coordinacion (min)", "category": "detection"},
    "photo_scan_interval_hours": {"value": "24", "type": "int", "label": "Intervalo escaneo fotos (horas)", "category": "profile_photos"},
    "photo_scan_batch_size": {"value": "50", "type": "int", "label": "Usuarios por lote", "category": "profile_photos"},
    "photo_scan_parallel_workers": {"value": "3", "type": "int", "label": "Workers paralelos", "category": "profile_photos"},
    "photo_scan_enabled": {"value": "true", "type": "bool", "label": "Escaneo automatico habilitado", "category": "profile_photos"},
    "passive_enrichment_enabled": {"value": "true", "type": "bool", "label": "Enriquecimiento pasivo habilitado", "category": "enrichment"},
    "enrichment_batch_size": {"value": "50", "type": "int", "label": "Tamaño de lote de enriquecimiento", "category": "enrichment"},
    "enrichment_interval_minutes": {"value": "5", "type": "int", "label": "Intervalo de enriquecimiento (min)", "category": "enrichment"},
    "media_retry_enabled": {"value": "true", "type": "bool", "label": "Reintento de multimedia habilitado", "category": "media_retry"},
    "media_retry_interval_minutes": {"value": "5", "type": "int", "label": "Intervalo de reintento (min)", "category": "media_retry"},
    "media_retry_batch_size": {"value": "100", "type": "int", "label": "Tamaño de lote de reintento", "category": "media_retry"},
    "media_retry_max_attempts": {"value": "3", "type": "int", "label": "Intentos máximos", "category": "media_retry"},
}


async def get_or_create_config(db: AsyncSession, key: str, default_value: str = "", value_type: str = "string") -> GlobalConfig:
    result = await db.execute(select(GlobalConfig).where(GlobalConfig.key == key))
    config = result.scalar_one_or_none()
    if not config:
        config = GlobalConfig(key=key, value=default_value, value_type=value_type)
        db.add(config)
        await db.commit()
        await db.refresh(config)
    return config


@router.get("/")
async def get_all_settings(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(select(GlobalConfig))
    configs = result.scalars().all()
    
    config_dict = {}
    for c in configs:
        value = c.value
        if c.value_type == "bool":
            value = "true" if value and value.lower() in ("true", "1", "yes") else "false"
        config_dict[c.key] = {"value": value, "type": c.value_type}
    
    for key, default in DEFAULT_CONFIGS.items():
        if key not in config_dict:
            config_dict[key] = {
                "value": default["value"],
                "type": default["type"],
                "label": default["label"],
                "category": default["category"]
            }
        else:
            config_dict[key]["label"] = default["label"]
            config_dict[key]["category"] = default["category"]
    
    return {"configs": config_dict, "categories": get_categories()}


def get_categories():
    return {
        "telegram": {"name": "Telegram API", "icon": "key"},
        "ocr": {"name": "OCR", "icon": "scan"},
        "backfill": {"name": "Backfill", "icon": "download"},
        "media": {"name": "Multimedia", "icon": "image"},
        "rate_limit": {"name": "Rate Limiting", "icon": "clock"},
        "advanced": {"name": "Avanzado", "icon": "settings"},
        "detection": {"name": "Deteccion", "icon": "search"},
        "profile_photos": {"name": "Fotos de Perfil", "icon": "camera"},
        "enrichment": {"name": "Enriquecimiento de Usuarios", "icon": "users"},
        "media_retry": {"name": "Reintento de Multimedia", "icon": "refresh"},
    }


@router.put("/")
async def update_settings(
    data: ConfigBulkUpdate,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    for key, value in data.configs.items():
        result = await db.execute(select(GlobalConfig).where(GlobalConfig.key == key))
        config = result.scalar_one_or_none()
        
        value_type = DEFAULT_CONFIGS.get(key, {}).get("type", "string")
        
        if value_type == "bool":
            str_value = "true" if value in (True, "true", "True", "1", 1) else "false"
        elif value is not None:
            str_value = str(value)
        else:
            str_value = ""
        
        if config:
            config.value = str_value
        else:
            config = GlobalConfig(key=key, value=str_value, value_type=value_type)
            db.add(config)
    
    await db.commit()
    return {"success": True, "message": "Configuracion actualizada"}


@router.get("/config/{key}")
async def get_config(
    key: str,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(select(GlobalConfig).where(GlobalConfig.key == key))
    config = result.scalar_one_or_none()
    
    if not config:
        default = DEFAULT_CONFIGS.get(key)
        if default:
            return {"key": key, "value": default["value"], "type": default["type"]}
        raise HTTPException(status_code=404, detail="Config not found")
    
    return {"key": config.key, "value": config.value, "type": config.value_type}


@router.get("/defaults")
async def get_default_api_credentials(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    api_id_result = await db.execute(select(GlobalConfig).where(GlobalConfig.key == "default_api_id"))
    api_hash_result = await db.execute(select(GlobalConfig).where(GlobalConfig.key == "default_api_hash"))
    
    api_id = api_id_result.scalar_one_or_none()
    api_hash = api_hash_result.scalar_one_or_none()
    
    return {
        "api_id": api_id.value if api_id else "",
        "api_hash": api_hash.value if api_hash else ""
    }


@router.get("/watchlist/domains")
async def get_domain_watchlist(
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(select(DomainWatchlist).order_by(DomainWatchlist.mention_count.desc()))
    domains = result.scalars().all()
    return {"domains": [
        {
            "id": d.id,
            "domain": d.domain,
            "description": d.description,
            "is_active": d.is_active,
            "mention_count": d.mention_count
        } for d in domains
    ]}


@router.post("/watchlist/domains")
async def add_domain_to_watchlist(
    data: DomainWatchlistCreate,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    existing = await db.execute(select(DomainWatchlist).where(DomainWatchlist.domain == data.domain))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Domain already in watchlist")
    
    domain = DomainWatchlist(domain=data.domain, description=data.description)
    db.add(domain)
    await db.commit()
    await db.refresh(domain)
    
    return {"id": domain.id, "domain": domain.domain}


@router.delete("/watchlist/domains/{domain_id}")
async def remove_domain_from_watchlist(
    domain_id: int,
    db: AsyncSession = Depends(get_db),
    current_user = Depends(get_current_user)
):
    result = await db.execute(select(DomainWatchlist).where(DomainWatchlist.id == domain_id))
    domain = result.scalar_one_or_none()
    
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
    
    await db.delete(domain)
    await db.commit()
    
    return {"success": True}
