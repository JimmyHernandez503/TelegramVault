from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from backend.app.api.deps import get_db, get_current_user
from backend.app.models.user import AppUser
from backend.app.models.telegram_account import TelegramAccount
from backend.app.models.telegram_group import TelegramGroup
from backend.app.schemas.telegram import TelegramAccountCreate, TelegramAccountUpdate, TelegramAccountResponse
from backend.app.core.config import settings

router = APIRouter()


@router.get("/", response_model=list[TelegramAccountResponse])
async def list_accounts(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramAccount).order_by(TelegramAccount.created_at.desc()))
    accounts = result.scalars().all()
    return accounts


@router.post("/", response_model=TelegramAccountResponse)
async def create_account(
    account_data: TelegramAccountCreate,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramAccount).where(TelegramAccount.phone == account_data.phone))
    existing = result.scalar_one_or_none()
    
    if existing:
        raise HTTPException(status_code=400, detail="Account with this phone already exists")
    
    data = account_data.model_dump()
    data['api_id'] = data.get('api_id') or settings.TELEGRAM_API_ID
    data['api_hash'] = data.get('api_hash') or settings.TELEGRAM_API_HASH
    
    if not data['api_id'] or not data['api_hash']:
        raise HTTPException(status_code=400, detail="API ID and API Hash are required. Set TELEGRAM_API_ID and TELEGRAM_API_HASH environment variables.")
    
    account = TelegramAccount(**data)
    db.add(account)
    await db.commit()
    await db.refresh(account)
    
    return account


@router.get("/{account_id}", response_model=TelegramAccountResponse)
async def get_account(
    account_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    return account


@router.patch("/{account_id}", response_model=TelegramAccountResponse)
async def update_account(
    account_id: int,
    account_data: TelegramAccountUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    update_data = account_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(account, field, value)
    
    await db.commit()
    await db.refresh(account)
    
    return account


@router.delete("/{account_id}")
async def delete_account(
    account_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    await db.delete(account)
    await db.commit()
    
    return {"message": "Account deleted successfully"}


@router.get("/{account_id}/groups")
async def get_account_groups(
    account_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    result = await db.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))
    account = result.scalar_one_or_none()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    groups_result = await db.execute(
        select(TelegramGroup)
        .where(TelegramGroup.assigned_account_id == account_id)
        .order_by(TelegramGroup.title)
    )
    groups = groups_result.scalars().all()
    
    return {
        "account_id": account_id,
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
                "is_monitoring": g.is_monitoring
            }
            for g in groups
        ]
    }


@router.get("/with-groups")
async def list_accounts_with_groups(
    db: AsyncSession = Depends(get_db),
    current_user: AppUser = Depends(get_current_user)
):
    accounts_result = await db.execute(
        select(TelegramAccount).order_by(TelegramAccount.created_at.desc())
    )
    accounts = accounts_result.scalars().all()
    
    result = []
    for account in accounts:
        groups_result = await db.execute(
            select(TelegramGroup)
            .where(TelegramGroup.assigned_account_id == account.id)
            .order_by(TelegramGroup.title)
        )
        groups = groups_result.scalars().all()
        
        result.append({
            "id": account.id,
            "phone": account.phone,
            "api_id": account.api_id,
            "telegram_id": account.telegram_id,
            "username": account.username,
            "first_name": account.first_name,
            "last_name": account.last_name,
            "status": account.status,
            "is_active": account.is_active,
            "messages_collected": account.messages_collected,
            "errors_count": account.errors_count,
            "proxy_type": account.proxy_type,
            "proxy_host": account.proxy_host,
            "last_activity": account.last_activity.isoformat() if account.last_activity else None,
            "created_at": account.created_at.isoformat() if account.created_at else None,
            "groups": [
                {
                    "id": g.id,
                    "title": g.title,
                    "username": g.username,
                    "group_type": g.group_type,
                    "status": g.status,
                    "is_monitoring": g.is_monitoring
                }
                for g in groups
            ]
        })
    
    return result
