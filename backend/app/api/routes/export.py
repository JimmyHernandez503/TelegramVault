from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Optional
from io import StringIO, BytesIO
import json

from backend.app.api.deps import get_current_user, get_db
from backend.app.models.user import AppUser
from backend.app.services.telegram_service import telegram_manager
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get("/{account_id}/messages/{chat_id}/csv")
async def export_messages_csv(
    account_id: int,
    chat_id: int,
    limit: int = Query(10000, ge=1, le=100000),
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.export_messages_csv(
        account_id, chat_id, limit
    )
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))
    
    output = StringIO(result["data"])
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=messages_{chat_id}.csv"
        }
    )


@router.get("/{account_id}/messages/{chat_id}/json")
async def export_messages_json(
    account_id: int,
    chat_id: int,
    limit: int = Query(10000, ge=1, le=100000),
    current_user: AppUser = Depends(get_current_user)
):
    result = await telegram_manager.export_messages_json(
        account_id, chat_id, limit
    )
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error", "Export failed"))
    
    return StreamingResponse(
        iter([result["data"]]),
        media_type="application/json",
        headers={
            "Content-Disposition": f"attachment; filename=messages_{chat_id}.json"
        }
    )


@router.get("/{account_id}/participants/{chat_id}/json")
async def export_participants_json(
    account_id: int,
    chat_id: int,
    current_user: AppUser = Depends(get_current_user)
):
    participants = await telegram_manager.fetch_participants(account_id, chat_id)
    
    if not participants:
        raise HTTPException(status_code=404, detail="No participants found")
    
    data = json.dumps(participants, ensure_ascii=False, indent=2)
    
    return StreamingResponse(
        iter([data]),
        media_type="application/json",
        headers={
            "Content-Disposition": f"attachment; filename=participants_{chat_id}.json"
        }
    )
