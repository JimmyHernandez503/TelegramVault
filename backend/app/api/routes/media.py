from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from typing import Optional, List
from pydantic import BaseModel

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.media import MediaFile
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_group import TelegramGroup
from backend.app.services.media_retry_service import media_retry_service

router = APIRouter()


class MediaStats(BaseModel):
    total: int
    photos: int
    videos: int
    gifs: int
    audio: int
    documents: int
    voice: int
    stickers: int
    video_notes: int
    total_size_bytes: int
    ocr_completed: int
    ocr_pending: int


class MediaItem(BaseModel):
    id: int
    file_type: str
    file_path: Optional[str]
    file_name: Optional[str]
    file_size: Optional[int]
    mime_type: Optional[str]
    width: Optional[int]
    height: Optional[int]
    duration: Optional[int]
    ocr_status: str
    ocr_text: Optional[str]
    group_id: Optional[int]
    group_name: Optional[str]
    created_at: Optional[str]


class GroupOption(BaseModel):
    id: int
    name: str
    media_count: int


@router.get("/stats", response_model=MediaStats)
async def get_media_stats(
    group_id: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    base_query = select(MediaFile)
    if group_id:
        base_query = base_query.join(TelegramMessage).where(TelegramMessage.group_id == group_id)
    
    total = await db.scalar(select(func.count()).select_from(base_query.subquery()))
    
    async def count_type(file_type: str) -> int:
        q = select(func.count(MediaFile.id)).where(MediaFile.file_type == file_type)
        if group_id:
            q = q.join(TelegramMessage).where(TelegramMessage.group_id == group_id)
        return await db.scalar(q) or 0
    
    photos = await count_type("photo")
    videos = await count_type("video")
    gifs = await count_type("gif")
    audio = await count_type("audio")
    documents = await count_type("document")
    voice = await count_type("voice")
    stickers = await count_type("sticker")
    video_notes = await count_type("video_note")
    
    size_q = select(func.coalesce(func.sum(MediaFile.file_size), 0))
    if group_id:
        size_q = size_q.join(TelegramMessage).where(TelegramMessage.group_id == group_id)
    total_size = await db.scalar(size_q) or 0
    
    ocr_completed_q = select(func.count(MediaFile.id)).where(MediaFile.ocr_status == "completed")
    if group_id:
        ocr_completed_q = ocr_completed_q.join(TelegramMessage).where(TelegramMessage.group_id == group_id)
    ocr_completed = await db.scalar(ocr_completed_q) or 0
    
    ocr_pending_q = select(func.count(MediaFile.id)).where(MediaFile.ocr_status == "pending")
    if group_id:
        ocr_pending_q = ocr_pending_q.join(TelegramMessage).where(TelegramMessage.group_id == group_id)
    ocr_pending = await db.scalar(ocr_pending_q) or 0
    
    return MediaStats(
        total=total or 0,
        photos=photos,
        videos=videos,
        gifs=gifs,
        audio=audio,
        documents=documents,
        voice=voice,
        stickers=stickers,
        video_notes=video_notes,
        total_size_bytes=total_size,
        ocr_completed=ocr_completed,
        ocr_pending=ocr_pending
    )


@router.get("/groups", response_model=List[GroupOption])
async def get_groups_with_media(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(
        select(
            TelegramGroup.id,
            TelegramGroup.title,
            func.count(MediaFile.id).label("media_count")
        )
        .join(TelegramMessage, TelegramMessage.group_id == TelegramGroup.id)
        .join(MediaFile, MediaFile.message_id == TelegramMessage.id)
        .group_by(TelegramGroup.id, TelegramGroup.title)
        .order_by(func.count(MediaFile.id).desc())
    )
    
    groups = []
    for row in result.all():
        groups.append(GroupOption(
            id=row.id,
            name=row.title or f"Group {row.id}",
            media_count=row.media_count
        ))
    
    return groups


@router.get("/", response_model=List[MediaItem])
async def get_media_list(
    group_id: Optional[int] = Query(None),
    file_type: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    query = (
        select(
            MediaFile,
            TelegramMessage.group_id,
            TelegramGroup.title.label("group_name")
        )
        .join(TelegramMessage, MediaFile.message_id == TelegramMessage.id)
        .outerjoin(TelegramGroup, TelegramGroup.id == TelegramMessage.group_id)
    )
    
    if group_id:
        query = query.where(TelegramMessage.group_id == group_id)
    
    if file_type:
        query = query.where(MediaFile.file_type == file_type)
    
    query = query.order_by(MediaFile.id.desc()).offset(offset).limit(limit)
    
    result = await db.execute(query)
    items = []
    
    for row in result.all():
        media = row[0]
        grp_id = row[1]
        group_name = row[2]
        
        items.append(MediaItem(
            id=media.id,
            file_type=media.file_type,
            file_path=media.file_path,
            file_name=media.file_name,
            file_size=media.file_size,
            mime_type=media.mime_type,
            width=media.width,
            height=media.height,
            duration=media.duration,
            ocr_status=media.ocr_status,
            ocr_text=media.ocr_text,
            group_id=grp_id,
            group_name=group_name,
            created_at=media.created_at.isoformat() if media.created_at else None
        ))
    
    return items


class RetrySettings(BaseModel):
    enabled: Optional[bool] = None
    interval_minutes: Optional[int] = None
    batch_size: Optional[int] = None
    max_retries: Optional[int] = None
    parallel_downloads: Optional[int] = None


@router.get("/retry/status")
async def get_retry_status(
    current_user: AppUser = Depends(get_current_user)
):
    status = media_retry_service.get_status()
    status["stats"]["pending_count"] = await media_retry_service.get_pending_count()
    return status


@router.post("/retry/settings")
async def update_retry_settings(
    settings: RetrySettings,
    current_user: AppUser = Depends(get_current_user)
):
    updates = {k: v for k, v in settings.dict().items() if v is not None}
    media_retry_service.update_settings(**updates)
    return media_retry_service.get_status()


@router.post("/retry/start")
async def start_retry_service(
    current_user: AppUser = Depends(get_current_user)
):
    await media_retry_service.start()
    return {"status": "started"}


@router.post("/retry/stop")
async def stop_retry_service(
    current_user: AppUser = Depends(get_current_user)
):
    await media_retry_service.stop()
    return {"status": "stopped"}


@router.post("/retry/now")
async def retry_media_now(
    current_user: AppUser = Depends(get_current_user)
):
    result = await media_retry_service.retry_now()
    return result


@router.get("/retry/pending")
async def get_pending_media(
    limit: int = Query(50, le=200),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(
        select(
            MediaFile.id,
            MediaFile.file_type,
            MediaFile.download_error,
            MediaFile.created_at,
            TelegramMessage.group_id,
            TelegramGroup.title.label("group_name")
        )
        .join(TelegramMessage, MediaFile.message_id == TelegramMessage.id)
        .outerjoin(TelegramGroup, TelegramGroup.id == TelegramMessage.group_id)
        .where(
            and_(
                MediaFile.file_path.is_(None),
                MediaFile.is_duplicate == False
            )
        )
        .order_by(MediaFile.created_at.desc())
        .limit(limit)
    )
    
    items = []
    for row in result.all():
        items.append({
            "id": row.id,
            "file_type": row.file_type,
            "error": row.download_error,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "group_id": row.group_id,
            "group_name": row.group_name
        })
    
    return {"count": len(items), "items": items}
