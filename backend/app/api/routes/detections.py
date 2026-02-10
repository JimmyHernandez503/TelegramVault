from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List, Optional
from pydantic import BaseModel

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.detection import Detection, RegexDetector
from backend.app.services.detection_service import detection_service

router = APIRouter()


class ScanTextRequest(BaseModel):
    text: str


class CreateDetectorRequest(BaseModel):
    name: str
    pattern: str
    category: str
    description: Optional[str] = None
    priority: int = 5


@router.get("/")
async def list_detections(
    detection_type: Optional[str] = None,
    group_id: Optional[int] = None,
    user_id: Optional[int] = None,
    limit: int = Query(default=100, le=500),
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_user import TelegramUser
    from backend.app.models.telegram_group import TelegramGroup
    from backend.app.models.telegram_message import TelegramMessage
    from sqlalchemy.orm import selectinload
    
    query = (
        select(
            Detection,
            TelegramUser.first_name.label("sender_first_name"),
            TelegramUser.last_name.label("sender_last_name"),
            TelegramUser.username.label("sender_username"),
            TelegramGroup.title.label("group_title"),
            TelegramMessage.date.label("message_date"),
            TelegramMessage.telegram_id.label("telegram_message_id")
        )
        .outerjoin(TelegramUser, Detection.user_id == TelegramUser.id)
        .outerjoin(TelegramGroup, Detection.group_id == TelegramGroup.id)
        .outerjoin(TelegramMessage, Detection.message_id == TelegramMessage.id)
        .order_by(Detection.created_at.desc())
    )
    
    if detection_type:
        query = query.where(Detection.detection_type == detection_type)
    if group_id:
        query = query.where(Detection.group_id == group_id)
    if user_id:
        query = query.where(Detection.user_id == user_id)
    
    query = query.offset(offset).limit(limit)
    result = await db.execute(query)
    rows = result.all()
    
    return [
        {
            "id": row.Detection.id,
            "detection_type": row.Detection.detection_type,
            "matched_text": row.Detection.matched_text,
            "context_before": row.Detection.context_before,
            "context_after": row.Detection.context_after,
            "source": row.Detection.source,
            "message_id": row.Detection.message_id,
            "telegram_message_id": row.telegram_message_id,
            "user_id": row.Detection.user_id,
            "sender_name": f"{row.sender_first_name or ''} {row.sender_last_name or ''}".strip() or row.sender_username or None,
            "sender_username": row.sender_username,
            "group_id": row.Detection.group_id,
            "group_title": row.group_title,
            "message_date": row.message_date.isoformat() if row.message_date else None,
            "created_at": row.Detection.created_at.isoformat() if row.Detection.created_at else None
        }
        for row in rows
    ]


@router.get("/stats")
async def detection_stats(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from sqlalchemy import text
    
    result = await db.execute(text("""
        SELECT 
            COUNT(*) FILTER (WHERE detection_type = 'email') as email,
            COUNT(*) FILTER (WHERE detection_type = 'phone') as phone,
            COUNT(*) FILTER (WHERE detection_type = 'crypto') as crypto,
            COUNT(*) FILTER (WHERE detection_type = 'url') as url,
            COUNT(*) FILTER (WHERE detection_type = 'invite_link') as invite_link,
            COUNT(*) FILTER (WHERE detection_type = 'telegram_link') as telegram_link,
            COUNT(*) FILTER (WHERE detection_type = 'telegram_username') as telegram_username,
            COUNT(*) FILTER (WHERE detection_type = 'credit_card') as credit_card,
            COUNT(*) FILTER (WHERE detection_type = 'hash') as hash,
            COUNT(*) FILTER (WHERE detection_type = 'ip_address') as ip_address,
            COUNT(*) as total,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'email') as unique_email,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'phone') as unique_phone,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'crypto') as unique_crypto,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'url') as unique_url,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'invite_link') as unique_invite_link,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'telegram_link') as unique_telegram_link,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'telegram_username') as unique_telegram_username,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'credit_card') as unique_credit_card,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'hash') as unique_hash,
            COUNT(DISTINCT LOWER(matched_text)) FILTER (WHERE detection_type = 'ip_address') as unique_ip_address
        FROM detections
    """))
    row = result.first()
    
    if not row:
        return {"total": 0, "unique_counts": {}}
    
    return {
        "email": row.email or 0,
        "phone": row.phone or 0,
        "crypto": row.crypto or 0,
        "url": row.url or 0,
        "invite_link": row.invite_link or 0,
        "telegram_link": row.telegram_link or 0,
        "telegram_username": row.telegram_username or 0,
        "credit_card": row.credit_card or 0,
        "hash": row.hash or 0,
        "ip_address": row.ip_address or 0,
        "total": row.total or 0,
        "unique_counts": {
            "email": row.unique_email or 0,
            "phone": row.unique_phone or 0,
            "crypto": row.unique_crypto or 0,
            "url": row.unique_url or 0,
            "invite_link": row.unique_invite_link or 0,
            "telegram_link": row.unique_telegram_link or 0,
            "telegram_username": row.unique_telegram_username or 0,
            "credit_card": row.unique_credit_card or 0,
            "hash": row.unique_hash or 0,
            "ip_address": row.unique_ip_address or 0
        }
    }


@router.get("/grouped")
async def list_grouped_detections(
    detection_type: Optional[str] = None,
    domain: Optional[str] = None,
    limit: int = Query(default=100, le=500),
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from sqlalchemy import text
    
    type_filter = "AND d.detection_type = :detection_type" if detection_type else ""
    domain_filter = "AND LOWER(d.matched_text) LIKE :domain_pattern" if domain else ""
    
    sql = text(f"""
    WITH ranked AS (
        SELECT 
            d.id, d.matched_text, d.detection_type, d.created_at,
            d.user_id, d.group_id,
            u.id as sender_db_id, u.telegram_id as sender_telegram_id,
            u.first_name as sender_first_name, u.last_name as sender_last_name,
            u.username as sender_username, u.current_photo_path as sender_photo,
            g.title as group_title,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(d.matched_text), d.detection_type 
                ORDER BY d.created_at DESC
            ) as rn
        FROM detections d
        LEFT JOIN telegram_users u ON d.user_id = u.id
        LEFT JOIN telegram_groups g ON d.group_id = g.id
        WHERE 1=1 {type_filter} {domain_filter}
    ),
    grouped AS (
        SELECT 
            LOWER(matched_text) as normalized_value,
            detection_type,
            COUNT(*) as occurrence_count,
            MIN(created_at) as first_seen,
            MAX(created_at) as last_seen
        FROM ranked
        GROUP BY LOWER(matched_text), detection_type
        ORDER BY COUNT(*) DESC
        LIMIT :limit_val OFFSET :offset_val
    )
    SELECT 
        g.normalized_value, g.detection_type, g.occurrence_count, g.first_seen, g.last_seen,
        r.matched_text as sample_value,
        r.sender_db_id, r.sender_telegram_id, r.sender_first_name, r.sender_last_name,
        r.sender_username, r.sender_photo, r.group_title
    FROM grouped g
    LEFT JOIN ranked r ON LOWER(r.matched_text) = g.normalized_value 
        AND r.detection_type = g.detection_type AND r.rn = 1
    ORDER BY g.occurrence_count DESC
    """)
    
    params = {"limit_val": limit, "offset_val": offset}
    if detection_type:
        params["detection_type"] = detection_type
    if domain:
        params["domain_pattern"] = f"%{domain.lower()}%"
    
    result = await db.execute(sql, params)
    rows = result.all()
    
    return [
        {
            "value": row.sample_value or row.normalized_value,
            "normalized_value": row.normalized_value,
            "detection_type": row.detection_type,
            "occurrence_count": row.occurrence_count,
            "first_seen": row.first_seen.isoformat() if row.first_seen else None,
            "last_seen": row.last_seen.isoformat() if row.last_seen else None,
            "sample_sender": {
                "id": row.sender_db_id,
                "telegram_id": row.sender_telegram_id,
                "name": f"{row.sender_first_name or ''} {row.sender_last_name or ''}".strip() or row.sender_username,
                "username": row.sender_username,
                "photo": row.sender_photo
            } if row.sender_db_id else None,
            "sample_group": row.group_title
        }
        for row in rows
    ]


@router.get("/occurrences/{normalized_value:path}")
async def get_detection_occurrences(
    normalized_value: str,
    detection_type: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_user import TelegramUser
    from backend.app.models.telegram_group import TelegramGroup
    from backend.app.models.telegram_message import TelegramMessage
    
    query = (
        select(
            Detection,
            TelegramUser.id.label("sender_db_id"),
            TelegramUser.telegram_id.label("sender_telegram_id"),
            TelegramUser.first_name.label("sender_first_name"),
            TelegramUser.last_name.label("sender_last_name"),
            TelegramUser.username.label("sender_username"),
            TelegramUser.current_photo_path.label("sender_photo"),
            TelegramGroup.id.label("group_db_id"),
            TelegramGroup.title.label("group_title"),
            TelegramGroup.photo_path.label("group_photo"),
            TelegramMessage.date.label("message_date"),
            TelegramMessage.telegram_id.label("telegram_message_id"),
            TelegramMessage.text.label("message_text")
        )
        .outerjoin(TelegramUser, Detection.user_id == TelegramUser.id)
        .outerjoin(TelegramGroup, Detection.group_id == TelegramGroup.id)
        .outerjoin(TelegramMessage, Detection.message_id == TelegramMessage.id)
        .where(func.lower(Detection.matched_text) == normalized_value.lower())
        .order_by(Detection.created_at.desc())
    )
    
    if detection_type:
        query = query.where(Detection.detection_type == detection_type)
    
    query = query.limit(limit)
    result = await db.execute(query)
    rows = result.all()
    
    return [
        {
            "id": row.Detection.id,
            "matched_text": row.Detection.matched_text,
            "context_before": row.Detection.context_before,
            "context_after": row.Detection.context_after,
            "message_text": row.message_text,
            "sender": {
                "id": row.sender_db_id,
                "telegram_id": row.sender_telegram_id,
                "name": f"{row.sender_first_name or ''} {row.sender_last_name or ''}".strip() or row.sender_username,
                "username": row.sender_username,
                "photo": f"/{row.sender_photo}" if row.sender_photo else None
            } if row.sender_db_id else None,
            "group": {
                "id": row.group_db_id,
                "title": row.group_title,
                "photo": f"/{row.group_photo}" if row.group_photo else None
            } if row.group_db_id else None,
            "message_date": row.message_date.isoformat() if row.message_date else None,
            "telegram_message_id": row.telegram_message_id,
            "created_at": row.Detection.created_at.isoformat() if row.Detection.created_at else None
        }
        for row in rows
    ]


@router.get("/url-domains")
async def get_url_domain_stats(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from urllib.parse import urlparse
    
    result = await db.execute(
        select(Detection.matched_text)
        .where(Detection.detection_type == "url")
    )
    urls = result.scalars().all()
    
    domain_counts = {}
    for url in urls:
        try:
            parsed = urlparse(url if url.startswith("http") else f"https://{url}")
            domain = parsed.netloc or parsed.path.split("/")[0]
            domain = domain.lower().replace("www.", "")
            if domain:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
        except:
            pass
    
    sorted_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)
    
    return [
        {"domain": domain, "count": count}
        for domain, count in sorted_domains[:50]
    ]


@router.get("/detectors")
async def list_detectors(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(RegexDetector).order_by(RegexDetector.category))
    detectors = result.scalars().all()
    
    return [
        {
            "id": d.id,
            "name": d.name,
            "description": d.description,
            "pattern": d.pattern,
            "category": d.category,
            "priority": d.priority,
            "is_builtin": d.is_builtin,
            "is_active": d.is_active
        }
        for d in detectors
    ]


@router.post("/detectors")
async def create_detector(
    data: CreateDetectorRequest,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    import re
    try:
        re.compile(data.pattern)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"Invalid regex pattern: {str(e)}")
    
    detector = RegexDetector(
        name=data.name,
        description=data.description,
        pattern=data.pattern,
        category=data.category,
        priority=data.priority,
        is_builtin=False,
        is_active=True
    )
    
    db.add(detector)
    await db.commit()
    await db.refresh(detector)
    
    return {
        "id": detector.id,
        "name": detector.name,
        "pattern": detector.pattern,
        "category": detector.category
    }


@router.post("/scan")
async def scan_text_for_detections(
    data: ScanTextRequest,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    detections = await detection_service.scan_text_no_save(db, data.text)
    return {
        "text_length": len(data.text),
        "detections_count": len(detections),
        "detections": detections
    }


@router.put("/detectors/{detector_id}/toggle")
async def toggle_detector(
    detector_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(RegexDetector).where(RegexDetector.id == detector_id))
    detector = result.scalar_one_or_none()
    
    if not detector:
        raise HTTPException(status_code=404, detail="Detector not found")
    
    detector.is_active = not detector.is_active
    await db.commit()
    
    return {"id": detector.id, "is_active": detector.is_active}


@router.delete("/detectors/{detector_id}")
async def delete_detector(
    detector_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(RegexDetector).where(RegexDetector.id == detector_id))
    detector = result.scalar_one_or_none()
    
    if not detector:
        raise HTTPException(status_code=404, detail="Detector not found")
    
    if detector.is_builtin:
        raise HTTPException(status_code=400, detail="Cannot delete built-in detector")
    
    await db.delete(detector)
    await db.commit()
    
    return {"success": True}


@router.post("/seed-defaults")
async def seed_default_detectors(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    created = await detection_service.seed_builtin_detectors(db)
    if created == 0:
        return {"message": "All built-in detectors already exist", "created": 0}
    return {"message": f"Created {created} new detectors", "created": created}


@router.post("/reprocess")
async def reprocess_detections(
    limit: int = Query(default=1000, le=10000),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_message import TelegramMessage
    
    result = await db.execute(
        select(TelegramMessage)
        .where(TelegramMessage.text.isnot(None), TelegramMessage.text != "")
        .order_by(TelegramMessage.id.desc())
        .limit(limit)
    )
    messages = result.scalars().all()
    
    total_detections = 0
    processed = 0
    
    for msg in messages:
        if msg.text:
            detections = await detection_service.scan_text(
                db=db,
                text=msg.text,
                message_id=msg.id,
                group_id=msg.group_id,
                user_id=msg.sender_id,
                source="message_reprocess",
                auto_commit=False,
                skip_existing=True
            )
            total_detections += len(detections)
            processed += 1
    
    await db.commit()
    
    return {
        "processed_messages": processed,
        "detections_created": total_detections
    }
