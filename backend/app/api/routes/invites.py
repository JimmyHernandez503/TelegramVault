from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import os
import re

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.invite import InviteLink
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_user import TelegramUser
from backend.app.schemas.telegram import InviteLinkCreate, InviteLinkResponse

router = APIRouter()


class PreviewRequest(BaseModel):
    link: str
    account_id: int


@router.get("/")
async def list_invites(
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    query = (
        select(
            InviteLink,
            TelegramGroup.title.label("source_group_title"),
            TelegramUser.first_name.label("source_user_name"),
            TelegramUser.username.label("source_user_username")
        )
        .outerjoin(TelegramGroup, InviteLink.source_group_id == TelegramGroup.id)
        .outerjoin(TelegramUser, InviteLink.source_user_id == TelegramUser.id)
    )
    
    if status:
        query = query.where(InviteLink.status == status)
    
    query = query.order_by(InviteLink.created_at.desc())
    
    result = await db.execute(query)
    rows = result.all()
    
    return [
        {
            "id": row.InviteLink.id,
            "link": row.InviteLink.link,
            "invite_hash": row.InviteLink.invite_hash,
            "status": row.InviteLink.status,
            "retry_count": row.InviteLink.retry_count,
            "last_error": row.InviteLink.last_error,
            "preview_title": row.InviteLink.preview_title,
            "preview_about": row.InviteLink.preview_about,
            "preview_member_count": row.InviteLink.preview_member_count,
            "preview_photo_path": row.InviteLink.preview_photo_path,
            "preview_is_channel": row.InviteLink.preview_is_channel,
            "preview_fetched_at": row.InviteLink.preview_fetched_at.isoformat() if row.InviteLink.preview_fetched_at else None,
            "source_group_id": row.InviteLink.source_group_id,
            "source_group_title": row.source_group_title,
            "source_user_id": row.InviteLink.source_user_id,
            "source_user_name": f"{row.source_user_name or ''}" or row.source_user_username or None,
            "joined_group_id": row.InviteLink.joined_group_id,
            "created_at": row.InviteLink.created_at.isoformat() if row.InviteLink.created_at else None
        }
        for row in rows
    ]


@router.post("/", response_model=InviteLinkResponse)
async def create_invite(
    invite_data: InviteLinkCreate,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(InviteLink).where(InviteLink.link == invite_data.link))
    existing = result.scalar_one_or_none()
    
    if existing:
        raise HTTPException(status_code=400, detail="Invite link already exists")
    
    invite = InviteLink(link=invite_data.link)
    db.add(invite)
    await db.commit()
    await db.refresh(invite)
    
    return invite


@router.delete("/{invite_id}")
async def delete_invite(
    invite_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(InviteLink).where(InviteLink.id == invite_id))
    invite = result.scalar_one_or_none()
    
    if not invite:
        raise HTTPException(status_code=404, detail="Invite not found")
    
    await db.delete(invite)
    await db.commit()
    
    return {"message": "Invite deleted successfully"}


@router.post("/preview")
async def preview_invite(
    data: PreviewRequest,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.telegram_service import telegram_manager
    
    client = telegram_manager.clients.get(data.account_id)
    if not client:
        raise HTTPException(status_code=400, detail="Account not connected")
    
    try:
        from telethon.tl.functions.messages import CheckChatInviteRequest
        from telethon.errors import InviteHashExpiredError, InviteHashInvalidError
        
        hash_match = re.search(r"(?:joinchat/|\+)([a-zA-Z0-9_-]+)", data.link)
        if not hash_match:
            raise HTTPException(status_code=400, detail="Invalid invite link format")
        
        invite_hash = hash_match.group(1)
        
        try:
            invite_info = await client(CheckChatInviteRequest(hash=invite_hash))
        except InviteHashExpiredError:
            return {"error": "expired", "message": "Invite link has expired"}
        except InviteHashInvalidError:
            return {"error": "invalid", "message": "Invite link is invalid"}
        
        preview_data = {
            "success": True,
            "title": None,
            "about": None,
            "member_count": None,
            "is_channel": False,
            "photo_path": None
        }
        
        if hasattr(invite_info, 'chat'):
            chat = invite_info.chat
            preview_data["title"] = getattr(chat, 'title', None)
            preview_data["member_count"] = getattr(chat, 'participants_count', None)
            preview_data["is_channel"] = getattr(chat, 'broadcast', False)
            
            if hasattr(chat, 'photo') and chat.photo:
                try:
                    os.makedirs("media/invite_previews", exist_ok=True)
                    photo_path = f"media/invite_previews/{invite_hash}.jpg"
                    await client.download_profile_photo(chat, file=photo_path)
                    if os.path.exists(photo_path):
                        preview_data["photo_path"] = photo_path
                except Exception as e:
                    print(f"Failed to download invite preview photo: {e}")
        elif hasattr(invite_info, 'title'):
            preview_data["title"] = invite_info.title
            preview_data["member_count"] = getattr(invite_info, 'participants_count', None)
            preview_data["is_channel"] = getattr(invite_info, 'broadcast', False)
            preview_data["about"] = getattr(invite_info, 'about', None)
        
        result = await db.execute(select(InviteLink).where(InviteLink.link == data.link))
        existing = result.scalar_one_or_none()
        
        if existing:
            existing.preview_title = preview_data["title"]
            existing.preview_about = preview_data.get("about")
            existing.preview_member_count = preview_data["member_count"]
            existing.preview_photo_path = preview_data["photo_path"]
            existing.preview_is_channel = preview_data["is_channel"]
            existing.preview_fetched_at = datetime.utcnow()
            await db.commit()
        
        return preview_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview invite: {str(e)}")


@router.post("/{invite_id}/fetch-preview")
async def fetch_invite_preview(
    invite_id: int,
    account_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(InviteLink).where(InviteLink.id == invite_id))
    invite = result.scalar_one_or_none()
    
    if not invite:
        raise HTTPException(status_code=404, detail="Invite not found")
    
    preview_result = await preview_invite(
        PreviewRequest(link=invite.link, account_id=account_id),
        db=db,
        current_user=current_user
    )
    
    return {
        "invite_id": invite_id,
        "preview": preview_result
    }


@router.post("/{invite_id}/join-now")
async def join_invite_now(
    invite_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.autojoin_service import autojoin_service
    
    result = await autojoin_service.join_now(invite_id)
    return result


@router.get("/autojoin/config")
async def get_autojoin_config(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.autojoin_service import autojoin_service
    return await autojoin_service.get_stats()


@router.put("/autojoin/config")
async def update_autojoin_config(
    data: dict,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.config import GlobalConfig
    
    config_map = {
        "enabled": "autojoin_enabled",
        "mode": "autojoin_mode",
        "delay_minutes": "autojoin_delay_minutes",
        "enabled_accounts": "autojoin_enabled_accounts",
        "auto_backfill": "autojoin_auto_backfill",
        "auto_scrape_members": "autojoin_auto_scrape_members",
        "auto_monitor": "autojoin_auto_monitor",
        "auto_stories": "autojoin_auto_stories",
        "max_joins_per_day": "autojoin_max_joins_per_day"
    }
    
    for key, db_key in config_map.items():
        if key in data:
            value = data[key]
            if isinstance(value, bool):
                value = "true" if value else "false"
            elif isinstance(value, list):
                value = ",".join(str(x) for x in value)
            else:
                value = str(value)
            
            result = await db.execute(
                select(GlobalConfig).where(GlobalConfig.key == db_key)
            )
            config = result.scalar_one_or_none()
            if config:
                config.value = value
            else:
                config = GlobalConfig(key=db_key, value=value)
                db.add(config)
    
    await db.commit()
    return {"status": "ok"}


@router.post("/fetch-all-previews")
async def fetch_all_previews(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.autojoin_service import autojoin_service
    
    result = await autojoin_service.fetch_all_previews()
    return result
