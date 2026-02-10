from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.telegram_account import TelegramAccount
from backend.app.models.telegram_group import TelegramGroup
from backend.app.services.telegram_service import telegram_manager
from backend.app.services.task_queue import task_queue, TaskPriority
from backend.app.services.websocket_manager import notify_account_status

router = APIRouter()


@router.post("/{account_id}/connect")
async def connect_account(
    account_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.connect_account(account_id, db)
    
    if result.get("success") and result.get("status") == "connected":
        await notify_account_status(account_id, "connected")
    
    return result


@router.post("/{account_id}/verify")
async def verify_code(
    account_id: int,
    code: str,
    password: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.verify_code(account_id, code, password, db)
    
    if result.get("success") and result.get("status") == "connected":
        await notify_account_status(account_id, "connected")
    
    return result


@router.post("/{account_id}/disconnect")
async def disconnect_account(
    account_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.disconnect_account(account_id, db)
    await notify_account_status(account_id, "disconnected")
    return result


@router.get("/{account_id}/dialogs")
async def get_dialogs(
    account_id: int,
    current_user: AppUser = Depends(get_current_user)
):
    dialogs = await telegram_manager.get_dialogs(account_id)
    return {"dialogs": dialogs}


@router.get("/{account_id}/managed-dialogs")
async def get_managed_dialogs(
    account_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_group import TelegramGroup
    
    result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
    account = result.scalar_one_or_none()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    if account_id not in telegram_manager.clients:
        if account.session_string:
            connect_result = await telegram_manager.connect_account(account_id, db)
            if not connect_result.get("success") or connect_result.get("status") != "connected":
                return {"dialogs": [], "error": "Account not connected", "needs_auth": True}
        else:
            return {"dialogs": [], "error": "Account not connected", "needs_auth": True}
    
    dialogs = await telegram_manager.get_dialogs(account_id)
    
    groups_result = await db.execute(select(TelegramGroup))
    groups = {g.telegram_id: g for g in groups_result.scalars().all()}
    
    enriched = []
    for d in dialogs:
        if d["type"] == "user":
            continue
        
        group = groups.get(d["id"])
        is_owned_by_this_account = group is not None and group.assigned_account_id == account_id
        is_owned_by_other = group is not None and group.assigned_account_id is not None and group.assigned_account_id != account_id
        
        enriched.append({
            **d,
            "is_monitored": is_owned_by_this_account and group is not None and group.status == "active",
            "group_id": group.id if group is not None else None,
            "backfill_enabled": group.backfill_enabled if group is not None else True,
            "download_media": group.download_media if group is not None else True,
            "ocr_enabled": group.ocr_enabled if group is not None else False,
            "status": group.status if group is not None else None,
            "owned_by_other_account": is_owned_by_other,
            "assigned_account_id": group.assigned_account_id if group is not None else None
        })
    
    return {"dialogs": enriched, "needs_auth": False}


@router.post("/{account_id}/join")
async def join_group(
    account_id: int,
    invite_link: str,
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.join_group(account_id, invite_link)
    return result


from pydantic import BaseModel

class AddDialogsRequest(BaseModel):
    dialog_ids: list[int]
    auto_backfill: bool = True

@router.post("/{account_id}/add-dialogs")
async def add_dialogs_to_monitor(
    account_id: int,
    request: AddDialogsRequest,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.models.telegram_group import TelegramGroup
    from backend.app.services.task_queue import task_queue, TaskPriority
    
    result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    dialogs = await telegram_manager.get_dialogs(account_id)
    dialogs_map = {d["id"]: d for d in dialogs}
    
    added = []
    skipped = []
    
    for dialog_id in request.dialog_ids:
        dialog = dialogs_map.get(dialog_id)
        if not dialog:
            skipped.append({"id": dialog_id, "reason": "not_found"})
            continue
        
        if dialog["type"] == "user":
            skipped.append({"id": dialog_id, "reason": "is_user"})
            continue
        
        existing = await db.execute(
            select(TelegramGroup).where(TelegramGroup.telegram_id == dialog_id)
        )
        if existing.scalar_one_or_none():
            skipped.append({"id": dialog_id, "reason": "already_exists"})
            continue
        
        group_type = "channel" if dialog["is_broadcast"] else "megagroup" if dialog["is_megagroup"] else "group"
        
        group = TelegramGroup(
            telegram_id=dialog_id,
            title=dialog["name"],
            username=dialog.get("username"),
            group_type=group_type,
            status="active",
            member_count=dialog.get("member_count", 0),
            photo_path=dialog.get("photo_path"),
            assigned_account_id=account_id,
            backfill_enabled=True,
            download_media=True,
            ocr_enabled=True
        )
        db.add(group)
        added.append({"id": dialog_id, "name": dialog["name"]})
    
    await db.commit()
    
    if request.auto_backfill and added:
        for item in added:
            result = await db.execute(
                select(TelegramGroup).where(TelegramGroup.telegram_id == item["id"])
            )
            group = result.scalar_one_or_none()
            
            if group:
                async def backfill_task(group_id=group.id, tg_id=item["id"], acc_id=account_id):
                    await telegram_manager.backfill_service.start_backfill(
                        account_id=acc_id,
                        channel_id=group_id,
                        telegram_id=tg_id,
                        mode="full"
                    )
                    return {"started": True, "group_id": group_id}
                
                await task_queue.enqueue(
                    name=f"Backfill {item['name']}",
                    func=backfill_task,
                    priority=TaskPriority.NORMAL,
                    metadata={"account_id": account_id, "chat_id": item["id"], "group_id": group.id}
                )
    
    return {
        "added": added,
        "skipped": skipped,
        "backfill_queued": request.auto_backfill
    }


@router.get("/{account_id}/messages/{chat_id}")
async def get_messages(
    account_id: int,
    chat_id: int,
    limit: int = 100,
    offset_id: int = 0,
    current_user: AppUser = Depends(get_current_user)
):
    messages = await telegram_manager.fetch_messages(
        account_id, chat_id, limit, offset_id
    )
    return {"messages": messages, "count": len(messages)}


@router.get("/{account_id}/participants/{chat_id}")
async def get_participants(
    account_id: int,
    chat_id: int,
    current_user: AppUser = Depends(get_current_user)
):
    participants = await telegram_manager.fetch_participants(account_id, chat_id)
    return {"participants": participants, "count": len(participants)}


@router.post("/{account_id}/backfill/{chat_id}")
async def start_backfill(
    account_id: int,
    chat_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(
        select(TelegramGroup).where(TelegramGroup.telegram_id == chat_id)
    )
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    await telegram_manager.backfill_service.start_backfill(
        account_id=account_id,
        channel_id=group.id,
        telegram_id=chat_id
    )
    
    return {"status": "started", "group_id": group.id, "telegram_id": chat_id, "mode": "full"}


@router.post("/{account_id}/backfill/{chat_id}/stop")
async def stop_backfill(
    account_id: int,
    chat_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(
        select(TelegramGroup).where(TelegramGroup.telegram_id == chat_id)
    )
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    await telegram_manager.backfill_service.stop_backfill(group.id)
    
    return {"status": "stopped", "group_id": group.id}


@router.post("/{account_id}/monitor/{chat_id}/start")
async def start_live_monitor(
    account_id: int,
    chat_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(
        select(TelegramGroup).where(TelegramGroup.telegram_id == chat_id)
    )
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    await telegram_manager.live_monitor.start_monitor(account_id, group.id, chat_id)
    
    return {"status": "monitoring", "group_id": group.id}


@router.post("/{account_id}/monitor/{chat_id}/stop")
async def stop_live_monitor(
    account_id: int,
    chat_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(
        select(TelegramGroup).where(TelegramGroup.telegram_id == chat_id)
    )
    group = result.scalar_one_or_none()
    
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    await telegram_manager.live_monitor.stop_monitor(group.id)
    
    return {"status": "stopped", "group_id": group.id}


@router.post("/{account_id}/download-media")
async def download_media(
    account_id: int,
    message_id: int,
    chat_id: int,
    current_user: AppUser = Depends(get_current_user)
):
    path = await telegram_manager.download_media(account_id, message_id, chat_id)
    
    if path:
        return {"success": True, "path": path}
    return {"success": False, "error": "Failed to download media"}


@router.post("/{account_id}/download-media-dedup")
async def download_media_with_dedup(
    account_id: int,
    message_id: int,
    chat_id: int,
    force: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.download_media_with_dedup(
        account_id, message_id, chat_id, db, force
    )
    return result


@router.post("/{account_id}/batch-download/{chat_id}")
async def batch_download_media(
    account_id: int,
    chat_id: int,
    message_ids: list[int],
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    async def progress_callback(current: int, total: int):
        pass
    
    result = await telegram_manager.batch_download_media(
        account_id, chat_id, message_ids, db, progress_callback
    )
    return result


@router.get("/{account_id}/profile-photo/{user_id}")
async def download_profile_photo(
    account_id: int,
    user_id: int,
    current_user: AppUser = Depends(get_current_user)
):
    path = await telegram_manager.download_profile_photo(account_id, user_id)
    
    if path:
        return {"success": True, "path": path}
    return {"success": False, "error": "No profile photo available"}


@router.get("/{account_id}/messages-enhanced/{chat_id}")
async def get_messages_enhanced(
    account_id: int,
    chat_id: int,
    limit: int = 100,
    offset_id: int = 0,
    min_id: int = 0,
    current_user: AppUser = Depends(get_current_user)
):
    messages = await telegram_manager.fetch_messages_enhanced(
        account_id, chat_id, limit, offset_id, min_id
    )
    return {"messages": messages, "count": len(messages)}


@router.get("/{account_id}/track-activity")
async def track_user_activity(
    account_id: int,
    user_ids: str,
    current_user: AppUser = Depends(get_current_user)
):
    ids = [int(x.strip()) for x in user_ids.split(",") if x.strip().isdigit()]
    
    if not ids:
        return {"success": False, "error": "No valid user IDs provided"}
    
    result = await telegram_manager.track_user_activity(account_id, ids)
    return result


@router.get("/{account_id}/user-profile/{user_id}")
async def get_full_user_profile(
    account_id: int,
    user_id: int,
    save_to_db: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.get_full_user_profile(account_id, user_id, db)
    
    if result.get("success") and save_to_db:
        db_id = await telegram_manager.save_user_to_db(result, db)
        result["db_id"] = db_id
    
    return result


@router.post("/{account_id}/user-photos/{user_id}")
async def download_all_profile_photos(
    account_id: int,
    user_id: int,
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.download_all_profile_photos(account_id, user_id)
    return result


@router.get("/{account_id}/user-stories/{user_id}")
async def get_user_stories(
    account_id: int,
    user_id: int,
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.get_user_stories(account_id, user_id)
    return result


@router.post("/{account_id}/save-participants/{chat_id}")
async def save_all_participants(
    account_id: int,
    chat_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    participants = await telegram_manager.fetch_participants(account_id, chat_id)
    
    if not participants:
        return {"success": False, "error": "No participants found or no permission"}
    
    saved = 0
    for p in participants:
        profile_data = {
            "success": True,
            "user": {
                "id": p.get("id"),
                "username": p.get("username"),
                "first_name": p.get("first_name"),
                "last_name": p.get("last_name"),
                "phone": p.get("phone"),
                "is_premium": p.get("is_premium", False),
                "is_verified": p.get("is_verified", False),
                "is_bot": p.get("is_bot", False),
                "is_restricted": p.get("is_restricted", False),
            }
        }
        db_id = await telegram_manager.save_user_to_db(profile_data, db)
        if db_id:
            saved += 1
    
    return {
        "success": True,
        "total_participants": len(participants),
        "saved_to_db": saved
    }


@router.post("/{account_id}/scrape-members/{group_id}")
async def scrape_group_members(
    account_id: int,
    group_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.member_scraper import member_scraper
    
    result = await db.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
    group = result.scalar_one_or_none()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    
    client = telegram_manager.clients.get(account_id)
    if not client or not client.is_connected():
        raise HTTPException(status_code=400, detail="Account not connected")
    
    async def run_scrape():
        async with telegram_manager.db_session_maker() as session:
            result = await session.execute(select(TelegramGroup).where(TelegramGroup.id == group_id))
            grp = result.scalar_one_or_none()
            if grp:
                stats = await member_scraper.scrape_group_members(client, grp, session, account_id)
                print(f"[MemberScraper] Group {grp.title}: {stats}")
    
    task_id = await task_queue.enqueue(
        f"scrape_members_{group_id}",
        run_scrape,
        priority=TaskPriority.NORMAL,
        metadata={"account_id": account_id, "group_id": group_id, "type": "member_scrape"}
    )
    
    return {
        "success": True,
        "task_id": task_id,
        "message": f"Member scraping started for {group.title}"
    }


@router.post("/{account_id}/scrape-all-members")
async def scrape_all_group_members(
    account_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    from backend.app.services.member_scraper import member_scraper
    
    client = telegram_manager.clients.get(account_id)
    if not client or not client.is_connected():
        raise HTTPException(status_code=400, detail="Account not connected")
    
    async def run_scrape_all():
        async with telegram_manager.db_session_maker() as session:
            stats = await member_scraper.scrape_all_groups(client, session, account_id)
            print(f"[MemberScraper] All groups: {stats}")
    
    task_id = await task_queue.enqueue(
        f"scrape_all_members_{account_id}",
        run_scrape_all,
        priority=TaskPriority.NORMAL,
        metadata={"account_id": account_id, "type": "member_scrape_all"}
    )
    
    return {
        "success": True,
        "task_id": task_id,
        "message": "Member scraping started for all active groups"
    }
