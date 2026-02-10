from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.telegram_user import TelegramUser
from backend.app.models.telegram_group import TelegramGroup
from backend.app.models.telegram_message import TelegramMessage
from backend.app.models.membership import GroupMembership
from backend.app.models.history import UserProfilePhoto, UserProfileHistory, UserStory
from backend.app.models.media import MediaFile
from backend.app.schemas.telegram import TelegramUserResponse

router = APIRouter()


@router.get("/", response_model=list[TelegramUserResponse])
async def list_users(
    skip: int = 0,
    limit: int = 100,
    watchlist_only: bool = False,
    favorites_only: bool = False,
    search: str | None = None,
    auto_enrich: bool = Query(default=True, description="Automatically trigger enrichment for incomplete users"),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    query = select(TelegramUser)
    
    if watchlist_only:
        query = query.where(TelegramUser.is_watchlist == True)
    if favorites_only:
        query = query.where(TelegramUser.is_favorite == True)
    if search:
        query = query.where(
            (TelegramUser.username.ilike(f"%{search}%")) |
            (TelegramUser.first_name.ilike(f"%{search}%")) |
            (TelegramUser.last_name.ilike(f"%{search}%"))
        )
    
    query = query.order_by(TelegramUser.messages_count.desc()).offset(skip).limit(limit)
    
    result = await db.execute(query)
    users = result.scalars().all()
    
    # Auto-enrich users without photos
    if auto_enrich:
        from backend.app.services.enrichment_utils import trigger_user_enrichment
        from backend.app.models.telegram_account import TelegramAccount
        from backend.app.services.telegram_service import telegram_manager
        import asyncio
        
        # Get a connected client
        acc_result = await db.execute(
            select(TelegramAccount).where(TelegramAccount.is_active == True).limit(1)
        )
        account = acc_result.scalar_one_or_none()
        
        if account:
            client = telegram_manager.clients.get(account.id)
            if client and client.is_connected():
                # Trigger enrichment for users without photos (in background)
                for user in users:
                    if not user.current_photo_path and user.telegram_id > 0 and user.access_hash:
                        asyncio.create_task(
                            trigger_user_enrichment(
                                client=client,
                                telegram_id=user.telegram_id,
                                source="list_users_api"
                            )
                        )
    
    return users


@router.get("/{user_id}/detail")
async def get_user_detail(
    user_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    try:
        id_value = int(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID")
    
    user = None
    if id_value <= 2147483647:
        result = await db.execute(
            select(TelegramUser)
            .options(selectinload(TelegramUser.profile_photos))
            .where(TelegramUser.id == id_value)
        )
        user = result.scalar_one_or_none()
    
    if not user:
        result = await db.execute(
            select(TelegramUser)
            .options(selectinload(TelegramUser.profile_photos))
            .where(TelegramUser.telegram_id == id_value)
        )
        user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    memberships_result = await db.execute(
        select(GroupMembership, TelegramGroup)
        .join(TelegramGroup, GroupMembership.group_id == TelegramGroup.id)
        .where(GroupMembership.user_id == user.id)
        .order_by(GroupMembership.joined_at.desc().nullsfirst())
    )
    memberships = []
    for row in memberships_result.all():
        membership, group = row
        memberships.append({
            "group_id": group.id,
            "group_title": group.title,
            "group_username": group.username,
            "is_channel": group.group_type == "channel",
            "is_admin": membership.is_admin,
            "admin_title": membership.admin_title,
            "joined_at": membership.joined_at.isoformat() if membership.joined_at else None,
            "is_active": membership.is_active,
            "leave_reason": membership.leave_reason
        })
    
    media_result = await db.execute(
        select(MediaFile, TelegramMessage.group_id, TelegramGroup.title, TelegramMessage.date)
        .join(TelegramMessage, MediaFile.message_id == TelegramMessage.id)
        .outerjoin(TelegramGroup, TelegramGroup.id == TelegramMessage.group_id)
        .where(TelegramMessage.sender_id == user.id)
        .order_by(MediaFile.created_at.desc())
        .limit(100)
    )
    media_files = []
    for row in media_result.all():
        media, group_id, group_title, msg_date = row
        media_files.append({
            "id": media.id,
            "file_type": media.file_type,
            "file_path": media.file_path,
            "file_name": media.file_name,
            "file_size": media.file_size,
            "mime_type": media.mime_type,
            "width": media.width,
            "height": media.height,
            "duration": media.duration,
            "ocr_text": media.ocr_text,
            "group_id": group_id,
            "group_title": group_title,
            "message_date": msg_date.isoformat() if msg_date else None,
            "created_at": media.created_at.isoformat() if media.created_at else None
        })
    
    history_result = await db.execute(
        select(UserProfileHistory)
        .where(UserProfileHistory.user_id == user.id)
        .order_by(UserProfileHistory.changed_at.desc())
        .limit(50)
    )
    history = []
    for h in history_result.scalars().all():
        history.append({
            "field": h.field_changed,
            "old_value": h.old_value,
            "new_value": h.new_value,
            "changed_at": h.changed_at.isoformat() if h.changed_at else None
        })
    
    profile_photos = []
    for photo in user.profile_photos:
        profile_photos.append({
            "id": photo.id,
            "file_path": photo.file_path,
            "is_current": photo.is_current,
            "is_video": photo.is_video or False,
            "captured_at": photo.captured_at.isoformat() if photo.captured_at else None,
            "created_at": photo.created_at.isoformat() if photo.created_at else None
        })
    
    stories_result = await db.execute(
        select(UserStory)
        .where(UserStory.user_id == user.id)
        .order_by(UserStory.posted_at.desc())
    )
    stories = []
    for s in stories_result.scalars().all():
        stories.append({
            "id": s.id,
            "story_id": s.story_id,
            "story_type": s.story_type,
            "file_path": s.file_path,
            "caption": s.caption,
            "width": s.width,
            "height": s.height,
            "duration": s.duration,
            "views_count": s.views_count,
            "is_pinned": s.is_pinned,
            "posted_at": s.posted_at.isoformat() if s.posted_at else None,
            "expires_at": s.expires_at.isoformat() if s.expires_at else None
        })
    
    return {
        "id": user.id,
        "telegram_id": user.telegram_id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "phone": user.phone,
        "bio": user.bio,
        "is_premium": user.is_premium,
        "is_verified": user.is_verified,
        "is_bot": user.is_bot,
        "is_scam": user.is_scam,
        "is_fake": user.is_fake,
        "is_restricted": user.is_restricted,
        "is_deleted": user.is_deleted,
        "is_watchlist": user.is_watchlist,
        "is_favorite": user.is_favorite,
        "last_seen": user.last_seen.isoformat() if user.last_seen else None,
        "current_photo_path": user.current_photo_path,
        "has_stories": user.has_stories,
        "messages_count": user.messages_count,
        "groups_count": user.groups_count,
        "media_count": user.media_count,
        "attachments_count": user.attachments_count,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
        "memberships": memberships,
        "media_files": media_files,
        "profile_photos": profile_photos,
        "stories": stories,
        "history": history
    }


@router.get("/{user_id}", response_model=TelegramUserResponse)
async def get_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramUser).where(TelegramUser.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user


@router.post("/{user_id}/watchlist")
async def toggle_watchlist(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramUser).where(TelegramUser.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user.is_watchlist = not user.is_watchlist
    await db.commit()
    
    return {"is_watchlist": user.is_watchlist}


@router.post("/{user_id}/favorite")
async def toggle_favorite(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramUser).where(TelegramUser.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user.is_favorite = not user.is_favorite
    await db.commit()
    
    return {"is_favorite": user.is_favorite}


@router.get("/{user_id}/messages")
async def get_user_messages(
    user_id: int,
    group_id: int | None = None,
    limit: int = Query(default=100, le=500),
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramUser).where(TelegramUser.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    query = (
        select(TelegramMessage, TelegramGroup.title.label("group_title"), TelegramGroup.id.label("gid"))
        .outerjoin(TelegramGroup, TelegramMessage.group_id == TelegramGroup.id)
        .where(TelegramMessage.sender_id == user.id)
    )
    
    if group_id:
        query = query.where(TelegramMessage.group_id == group_id)
    
    query = query.order_by(TelegramMessage.date.desc()).offset(offset).limit(limit)
    
    result = await db.execute(query)
    rows = result.all()
    
    messages = []
    for row in rows:
        msg = row[0]
        messages.append({
            "id": msg.id,
            "telegram_id": msg.telegram_id,
            "text": msg.text,
            "message_type": msg.message_type,
            "date": msg.date.isoformat() if msg.date else None,
            "views": msg.views,
            "forwards": msg.forwards,
            "group_id": row.gid,
            "group_title": row.group_title
        })
    
    groups_result = await db.execute(
        select(TelegramGroup.id, TelegramGroup.title, func.count(TelegramMessage.id).label("msg_count"))
        .join(TelegramMessage, TelegramMessage.group_id == TelegramGroup.id)
        .where(TelegramMessage.sender_id == user.id)
        .group_by(TelegramGroup.id, TelegramGroup.title)
        .order_by(func.count(TelegramMessage.id).desc())
    )
    groups = [
        {"id": g.id, "title": g.title, "message_count": g.msg_count}
        for g in groups_result.all()
    ]
    
    if group_id:
        filtered_count = next((g["message_count"] for g in groups if g["id"] == group_id), 0)
    else:
        filtered_count = user.messages_count
    
    return {
        "messages": messages,
        "groups": groups,
        "total_messages": user.messages_count,
        "filtered_count": filtered_count
    }


@router.post("/{user_id}/download-stories")
async def download_user_stories(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_account import TelegramAccount
    from backend.app.services.telegram_service import telegram_manager
    from backend.app.services.story_service import StoryService
    
    result = await db.execute(select(TelegramUser).where(TelegramUser.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if not user.has_stories:
        return {"success": False, "error": "User has no stories", "stories": []}
    
    accounts_result = await db.execute(
        select(TelegramAccount).where(TelegramAccount.is_active == True)
    )
    accounts = accounts_result.scalars().all()
    
    if not accounts:
        raise HTTPException(status_code=400, detail="No connected accounts available")
    
    for account in accounts:
        client = telegram_manager.clients.get(account.id)
        if client and client.is_connected():
            try:
                story_service = StoryService(client, db)
                stories = await story_service.download_user_stories(user)
                return {
                    "success": True,
                    "stories_downloaded": len(stories),
                    "stories": stories
                }
            except Exception as e:
                print(f"[DownloadStories] Error with account {account.id}: {e}")
                continue
    
    return {"success": False, "error": "No working account found", "stories": []}


@router.post("/{user_id}/sync-photos")
async def sync_user_profile_photos(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_account import TelegramAccount
    from backend.app.services.telegram_service import telegram_manager
    from backend.app.services.user_enricher import user_enricher
    
    result = await db.execute(select(TelegramUser).where(TelegramUser.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    accounts_result = await db.execute(
        select(TelegramAccount).where(TelegramAccount.is_active == True)
    )
    accounts = accounts_result.scalars().all()
    
    if not accounts:
        raise HTTPException(status_code=400, detail="No connected accounts available")
    
    for account in accounts:
        client = telegram_manager.clients.get(account.id)
        if client and client.is_connected():
            try:
                input_user = await client.get_input_entity(user.telegram_id)
                tg_user = await client.get_entity(input_user)
                
                downloaded = await user_enricher.sync_all_profile_photos(client, db, user, tg_user)
                await db.commit()
                
                photos_result = await db.execute(
                    select(UserProfilePhoto).where(UserProfilePhoto.user_id == user.id).order_by(UserProfilePhoto.created_at.desc())
                )
                photos = photos_result.scalars().all()
                
                return {
                    "success": True,
                    "photos_downloaded": downloaded,
                    "total_photos": len(photos),
                    "photos": [
                        {
                            "id": p.id,
                            "file_path": p.file_path,
                            "is_current": p.is_current,
                            "is_video": p.is_video,
                            "captured_at": p.captured_at.isoformat() if p.captured_at else None,
                            "created_at": p.created_at.isoformat() if p.created_at else None
                        }
                        for p in photos
                    ]
                }
            except Exception as e:
                print(f"[SyncPhotos] Error with account {account.id}: {e}")
                continue
    
    return {"success": False, "error": "No working account found", "photos_downloaded": 0}


@router.post("/bulk-enrich")
async def bulk_enrich_users(
    batch_size: int = Query(default=100, le=500),
    skip_enriched: bool = Query(default=True),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    """
    Queue all users for enrichment in batches.
    This endpoint queues users and returns immediately - enrichment happens in the background.
    """
    import asyncio
    from backend.app.models.telegram_account import TelegramAccount
    from backend.app.services.telegram_service import telegram_manager
    from backend.app.services.enhanced_user_enricher_service import EnhancedUserEnricherService
    from backend.app.core.download_queue_manager import TaskPriority
    
    # Get enricher service
    enricher = EnhancedUserEnricherService()
    if not enricher._initialized:
        await enricher.initialize()
    
    # Start worker if not running
    status = enricher.get_status()
    if not status["running"]:
        await enricher.start_worker()
        await asyncio.sleep(1)
    
    # Get a connected Telegram client
    accounts_result = await db.execute(
        select(TelegramAccount).where(TelegramAccount.is_active == True)
    )
    accounts = accounts_result.scalars().all()
    
    if not accounts:
        raise HTTPException(status_code=400, detail="No active Telegram accounts found")
    
    # Try to get a connected client
    client = None
    for account in accounts:
        test_client = telegram_manager.clients.get(account.id)
        if test_client and test_client.is_connected():
            client = test_client
            break
    
    if not client:
        raise HTTPException(status_code=400, detail="No connected Telegram clients found")
    
    # Query users and queue them
    query = select(TelegramUser).where(
        TelegramUser.telegram_id > 0,
        TelegramUser.access_hash.isnot(None)
    ).order_by(TelegramUser.messages_count.desc())
    
    if skip_enriched:
        # Skip users that already have photos
        query = query.where(TelegramUser.current_photo_path.is_(None))
        # But must have at least a name
        query = query.where(
            (TelegramUser.username.isnot(None)) | (TelegramUser.first_name.isnot(None))
        )
    
    query = query.limit(batch_size)
    
    result = await db.execute(query)
    users = result.scalars().all()
    
    total_queued = 0
    total_skipped = 0
    
    for user in users:
        try:
            # Queue for enrichment
            queued = await enricher.queue_enrichment(
                client=client,
                telegram_id=user.telegram_id,
                group_id=None,
                priority=TaskPriority.NORMAL
            )
            if queued:
                total_queued += 1
            else:
                total_skipped += 1
        except Exception as e:
            print(f"Error queuing user {user.telegram_id}: {e}")
            total_skipped += 1
    
    enricher_status = enricher.get_status()
    
    return {
        "success": True,
        "queued": total_queued,
        "skipped": total_skipped,
        "queue_size": enricher_status["queue_size"],
        "processed_users": enricher_status["processed_users"],
        "worker_running": enricher_status["running"]
    }


@router.get("/combined", response_model=list[dict])
async def list_users_and_channels(
    skip: int = 0,
    limit: int = 100,
    watchlist_only: bool = False,
    favorites_only: bool = False,
    search: str | None = None,
    include_channels: bool = Query(default=True, description="Include channels/groups in results"),
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    """
    Get a combined list of users AND channels/groups.
    Each item has a 'type' field: 'user', 'channel', or 'group'
    """
    results = []
    
    # Get users - fetch more to ensure we have enough with names
    user_query = select(TelegramUser)
    
    if watchlist_only:
        user_query = user_query.where(TelegramUser.is_watchlist == True)
    if favorites_only:
        user_query = user_query.where(TelegramUser.is_favorite == True)
    if search:
        user_query = user_query.where(
            (TelegramUser.username.ilike(f"%{search}%")) |
            (TelegramUser.first_name.ilike(f"%{search}%")) |
            (TelegramUser.last_name.ilike(f"%{search}%"))
        )
    
    # Fetch more users to compensate for those without names
    fetch_limit = limit * 3 if not search else limit
    user_query = user_query.order_by(TelegramUser.messages_count.desc()).limit(fetch_limit)
    user_result = await db.execute(user_query)
    users = user_result.scalars().all()
    
    for user in users:
        # Only include users that have at least a username or first_name
        if not user.username and not user.first_name:
            continue
            
        results.append({
            "id": user.id,
            "telegram_id": user.telegram_id,
            "type": "user",
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "title": None,  # Users don't have titles
            "bio": user.bio,
            "is_premium": user.is_premium,
            "is_verified": user.is_verified,
            "is_bot": user.is_bot,
            "is_watchlist": user.is_watchlist,
            "is_favorite": user.is_favorite,
            "messages_count": user.messages_count,
            "groups_count": user.groups_count,
            "media_count": user.media_count,
            "current_photo_path": user.current_photo_path,
            "has_stories": user.has_stories,
        })
    
    # Get channels/groups if requested
    if include_channels:
        channel_query = select(TelegramGroup)
        
        if search:
            channel_query = channel_query.where(
                (TelegramGroup.title.ilike(f"%{search}%")) |
                (TelegramGroup.username.ilike(f"%{search}%"))
            )
        
        channel_query = channel_query.order_by(TelegramGroup.messages_count.desc()).limit(limit)
        channel_result = await db.execute(channel_query)
        channels = channel_result.scalars().all()
        
        for channel in channels:
            results.append({
                "id": channel.id,
                "telegram_id": channel.telegram_id,
                "type": channel.group_type,  # 'channel', 'group', 'supergroup', 'megagroup'
                "username": channel.username,
                "first_name": None,  # Channels don't have first/last names
                "last_name": None,
                "title": channel.title,
                "bio": channel.description,
                "is_premium": False,
                "is_verified": False,
                "is_bot": False,
                "is_watchlist": False,
                "is_favorite": False,
                "messages_count": channel.messages_count,
                "groups_count": 0,  # Channels don't belong to groups
                "media_count": 0,  # TODO: Could calculate this
                "current_photo_path": channel.photo_path,
                "has_stories": False,
                "member_count": channel.member_count,
            })
    
    # Sort combined results by message count
    results.sort(key=lambda x: x["messages_count"], reverse=True)
    
    return results[:limit]


@router.get("/enrichment/metrics")
async def get_enrichment_metrics(
    current_user: AppUser = Depends(get_current_user)
):
    """
    Get user enrichment metrics and statistics.
    
    Returns:
        - Total enrichments performed
        - Success/failure rates
        - Cache hit/miss rates
        - Average enrichment time
        - Active operations count
    """
    from backend.app.services.enrichment_utils import get_enrichment_metrics
    
    try:
        metrics = await get_enrichment_metrics()
        return {
            "success": True,
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get enrichment metrics: {str(e)}")


@router.get("/enrichment/statistics")
async def get_enrichment_statistics(
    current_user: AppUser = Depends(get_current_user)
):
    """
    Get detailed user enrichment statistics.
    
    Returns:
        - Status counts (pending, in_progress, completed, failed)
        - Recent failures with error messages
        - Comprehensive metrics
    """
    from backend.app.services.enrichment_utils import get_enrichment_statistics
    
    try:
        statistics = await get_enrichment_statistics()
        return {
            "success": True,
            "statistics": statistics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get enrichment statistics: {str(e)}")


@router.get("/passive-enrichment/status")
async def get_passive_enrichment_status(
    current_user: AppUser = Depends(get_current_user)
):
    """
    Get passive enrichment service status and statistics.
    
    Returns:
        - Service running status
        - Enrichment statistics (cycles completed, users enriched, failures)
        - Configuration settings
        - Account error tracking
        - Last cycle time
    """
    from backend.app.services.passive_enrichment_service import passive_enrichment_service
    
    try:
        status = passive_enrichment_service.get_status()
        return {
            "success": True,
            "status": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get passive enrichment status: {str(e)}")
