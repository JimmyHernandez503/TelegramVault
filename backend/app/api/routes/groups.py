from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc
from sqlalchemy.orm import selectinload

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.media import MediaFile
from backend.app.schemas.telegram import TelegramGroupCreate, TelegramGroupUpdate, TelegramGroupResponse

router = APIRouter()


@router.get("/", response_model=list[TelegramGroupResponse])
async def list_groups(
    skip: int = 0,
    limit: int = 100,
    status: str | None = None,
    group_type: str | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    query = select(TelegramGroup)
    
    if status:
        query = query.where(TelegramGroup.status == status)
    if group_type:
        query = query.where(TelegramGroup.group_type == group_type)
    
    query = query.order_by(TelegramGroup.updated_at.desc()).offset(skip).limit(limit)
    
    result = await db.execute(query)
    groups = result.scalars().all()
    return groups


@router.post("/", response_model=TelegramGroupResponse)
async def create_group(
    group_data: TelegramGroupCreate,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(
        select(TelegramGroup).where(TelegramGroup.telegram_id == group_data.telegram_id)
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        raise HTTPException(status_code=400, detail="Group already exists")
    
    group = TelegramGroup(**group_data.model_dump())
    db.add(group)
    await db.commit()
    await db.refresh(group)
    
    return group


@router.get("/{group_id}", response_model=TelegramGroupResponse)
async def get_group(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    return group


@router.patch("/{group_id}", response_model=TelegramGroupResponse)
async def update_group(
    group_id: int,
    group_data: TelegramGroupUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    update_data = group_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(group, field, value)
    
    await db.commit()
    await db.refresh(group)
    
    return group


@router.delete("/{group_id}")
async def delete_group(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    await db.delete(group)
    await db.commit()
    
    return {"message": "Group deleted successfully"}


@router.post("/{group_id}/toggle-monitoring")
async def toggle_monitoring(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    if group.status == "active":
        group.status = "paused"
    else:
        group.status = "active"
    
    await db.commit()
    
    return {"id": group.id, "status": group.status}


@router.get("/monitoring/status")
async def get_monitoring_status(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_account import TelegramAccount
    from sqlalchemy.orm import selectinload
    
    result = await db.execute(
        select(TelegramGroup).options(selectinload(TelegramGroup.assigned_account))
    )
    groups = result.scalars().all()
    
    active = [g for g in groups if g.status == "active"]
    paused = [g for g in groups if g.status == "paused"]
    backfilling = [g for g in groups if g.status == "backfilling"]
    error = [g for g in groups if g.status == "error"]
    
    return {
        "total": len(groups),
        "active": len(active),
        "paused": len(paused),
        "backfilling": len(backfilling),
        "error": len(error),
        "groups": [
            {
                "id": g.id,
                "telegram_id": g.telegram_id,
                "title": g.title,
                "username": g.username,
                "group_type": g.group_type,
                "status": g.status,
                "member_count": g.member_count,
                "messages_count": g.messages_count,
                "photo_path": g.photo_path,
                "backfill_enabled": g.backfill_enabled,
                "download_media": g.download_media,
                "ocr_enabled": g.ocr_enabled,
                "assigned_account_id": g.assigned_account_id,
                "assigned_account": {
                    "id": g.assigned_account.id,
                    "phone": g.assigned_account.phone,
                    "username": g.assigned_account.username,
                    "status": g.assigned_account.status
                } if g.assigned_account else None
            }
            for g in groups
        ]
    }


@router.post("/{group_id}/assign-account")
async def assign_account_to_group(
    group_id: int,
    account_id: int = Query(default=0, description="Account ID to assign, 0 to unassign"),
    auto_backfill: bool = Query(default=True, description="Automatically start backfill"),
    auto_monitor: bool = Query(default=True, description="Automatically start monitoring"),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_account import TelegramAccount
    from backend.app.services.telegram_service import telegram_manager
    
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    old_account_id = group.assigned_account_id
    
    if account_id and account_id > 0:
        acc_result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
        account = acc_result.scalar_one_or_none()
        if not account:
            raise HTTPException(status_code=404, detail="Account not found")
        group.assigned_account_id = account_id
        group.status = "active"
    else:
        if old_account_id and telegram_manager.live_monitor.is_monitoring(group_id):
            await telegram_manager.live_monitor.stop_monitor(group_id)
        group.assigned_account_id = None
    
    await db.commit()
    
    monitor_started = False
    backfill_started = False
    
    if account_id and account_id > 0:
        if auto_monitor:
            try:
                await telegram_manager.live_monitor.auto_start_for_group(group_id)
                monitor_started = True
            except Exception as e:
                print(f"[Groups] Monitor auto-start failed for group {group_id}: {e}")
        
        if auto_backfill and not group.backfill_done:
            try:
                await telegram_manager.backfill_service.start_backfill(
                    account_id=account_id,
                    channel_id=group_id,
                    telegram_id=group.telegram_id
                )
                backfill_started = True
            except Exception as e:
                print(f"[Groups] Backfill auto-start failed for group {group_id}: {e}")
    
    return {
        "id": group.id, 
        "assigned_account_id": group.assigned_account_id,
        "status": group.status,
        "auto_started": bool(account_id and account_id > 0),
        "monitor_started": monitor_started,
        "backfill_started": backfill_started
    }


@router.post("/{group_id}/start-backfill")
async def start_group_backfill(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.telegram_service import telegram_manager
    
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    if not group.assigned_account_id:
        raise HTTPException(status_code=400, detail="No account assigned to this group")
    
    if group.backfill_in_progress:
        return {"status": "already_running", "group_id": group_id, "message": "Backfill already in progress"}
    
    try:
        await telegram_manager.backfill_service.start_backfill(
            account_id=group.assigned_account_id,
            channel_id=group_id,
            telegram_id=group.telegram_id
        )
        return {"status": "started", "group_id": group_id, "group_title": group.title}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{group_id}/scrape-members")
async def scrape_group_members(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.telegram_service import telegram_manager
    from backend.app.services.member_scraper import member_scraper
    
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    if not group.assigned_account_id:
        raise HTTPException(status_code=400, detail="No account assigned to this group")
    
    client = telegram_manager.clients.get(group.assigned_account_id)
    if not client:
        raise HTTPException(status_code=400, detail="Account not connected")
    
    try:
        stats = await member_scraper.scrape_group_members(
            client=client,
            group=group,
            db=db,
            account_id=group.assigned_account_id
        )
        return {
            "status": "completed", 
            "group_id": group_id, 
            "members_scraped": stats.get("total_scraped", 0),
            "new_users": stats.get("new_users", 0),
            "errors": stats.get("errors", [])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{group_id}/members")
async def get_group_members(
    group_id: int,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.membership import GroupMembership
    
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    members_result = await db.execute(
        select(TelegramUser, GroupMembership)
        .join(GroupMembership, TelegramUser.id == GroupMembership.user_id)
        .where(GroupMembership.group_id == group_id)
        .order_by(TelegramUser.messages_count.desc())
        .offset(offset)
        .limit(limit)
    )
    rows = members_result.all()
    
    count_result = await db.execute(
        select(func.count()).select_from(GroupMembership).where(GroupMembership.group_id == group_id)
    )
    total = count_result.scalar() or 0
    
    members = []
    for user, membership in rows:
        role = "admin" if membership.is_admin else "member"
        if membership.admin_title:
            role = membership.admin_title
        members.append({
            "id": user.id,
            "telegram_id": user.telegram_id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "phone": user.phone,
            "photo_path": user.current_photo_path,
            "is_premium": user.is_premium,
            "is_bot": user.is_bot,
            "messages_count": user.messages_count,
            "is_watchlist": user.is_watchlist,
            "is_favorite": user.is_favorite,
            "joined_at": membership.joined_at.isoformat() if membership.joined_at else None,
            "role": role
        })
    
    return {"members": members, "total": total, "group_id": group_id, "group_title": group.title}


@router.get("/{group_id}/messages")
async def get_group_messages(
    group_id: int,
    limit: int = Query(default=50, le=200),
    offset_id: int = Query(default=0, description="Offset by message telegram_id for pagination"),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    all_raw_messages = []
    grouped_data: dict[int, dict] = {}
    current_offset_id = offset_id
    batch_size = limit * 5
    max_iterations = 5
    iteration = 0
    
    while len(all_raw_messages) < batch_size and iteration < max_iterations:
        iteration += 1
        
        query = (
            select(TelegramMessage, TelegramUser)
            .outerjoin(TelegramUser, TelegramMessage.sender_id == TelegramUser.id)
            .where(TelegramMessage.group_id == group_id)
        )
        
        if current_offset_id > 0:
            query = query.where(TelegramMessage.telegram_id < current_offset_id)
        
        query = query.order_by(desc(TelegramMessage.telegram_id)).limit(batch_size)
        
        result = await db.execute(query)
        rows = result.all()
        
        if not rows:
            break
        
        for msg, user in rows:
            current_offset_id = msg.telegram_id
            
            media_result = await db.execute(
                select(MediaFile).where(MediaFile.message_id == msg.id)
            )
            media_files = media_result.scalars().all()
            media = media_files[0] if media_files else None
            
            sender_name = "Unknown"
            sender_username = None
            sender_telegram_id = None
            
            if user:
                sender_telegram_id = user.telegram_id
                if user.telegram_id and user.telegram_id < 0:
                    sender_name = group.title
                    sender_username = group.username
                else:
                    sender_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                    if not sender_name:
                        sender_name = user.username or f"User {user.telegram_id}"
                    sender_username = user.username
                    
                    # Trigger enrichment if sender is unknown
                    if sender_name == f"User {user.telegram_id}" or not user.current_photo_path:
                        from backend.app.services.enrichment_utils import trigger_user_enrichment
                        from backend.app.services.telegram_service import telegram_manager
                        
                        if group.assigned_account_id:
                            client = telegram_manager.clients.get(group.assigned_account_id)
                            if client:
                                await trigger_user_enrichment(
                                    client=client,
                                    telegram_id=user.telegram_id,
                                    group_id=group_id,
                                    source="groups_api"
                                )
            
            media_items = [
                {"type": m.file_type, "path": m.file_path, "file_name": m.file_name}
                for m in media_files
            ] if media_files else []
            
            if msg.grouped_id:
                if msg.grouped_id not in grouped_data:
                    grouped_data[msg.grouped_id] = {
                        "media": [],
                        "caption": None,
                        "first_msg_telegram_id": msg.telegram_id
                    }
                grouped_data[msg.grouped_id]["media"].extend(media_items)
                if msg.text and msg.text.strip() and not grouped_data[msg.grouped_id]["caption"]:
                    grouped_data[msg.grouped_id]["caption"] = msg.text
            
            all_raw_messages.append({
                "id": msg.id,
                "message_id": msg.telegram_id,
                "text": msg.text or "",
                "date": msg.date.isoformat() if msg.date else None,
                "sender_id": sender_telegram_id,
                "sender_name": sender_name,
                "sender_username": sender_username,
                "sender_photo": user.current_photo_path if user and user.current_photo_path else None,
                "media_type": media.file_type if media else None,
                "media_path": media.file_path if media else None,
                "media_items": media_items,
                "views": msg.views or 0,
                "forwards": msg.forwards or 0,
                "reactions": {},
                "reply_to_msg_id": msg.reply_to_msg_id,
                "grouped_id": msg.grouped_id
            })
        
        if len(rows) < batch_size:
            break
    
    messages = []
    seen_grouped_ids: set[int] = set()
    
    for msg_data in all_raw_messages:
        grouped_id = msg_data.get("grouped_id")
        
        if grouped_id:
            if grouped_id in seen_grouped_ids:
                continue
            seen_grouped_ids.add(grouped_id)
            
            gd = grouped_data.get(grouped_id, {})
            msg_data["media_items"] = gd.get("media", [])
            if gd.get("caption"):
                msg_data["text"] = gd["caption"]
        
        messages.append(msg_data)
        
        if len(messages) >= limit:
            break
    
    return {
        "messages": messages[:limit],
        "count": len(messages[:limit]),
        "group": {
            "id": group.id,
            "telegram_id": group.telegram_id,
            "title": group.title,
            "member_count": group.member_count,
            "messages_count": group.messages_count
        }
    }
