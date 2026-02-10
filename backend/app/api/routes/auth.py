from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from backend.app.api.deps import get_db, get_current_user
from backend.app.core.security import verify_password, get_password_hash, create_access_token
from backend.app.core.config import settings
from backend.app.models.user import AppUser
from backend.app.schemas.auth import Token, LoginRequest, UserCreate, UserResponse

router = APIRouter()


@router.post("/login", response_model=Token)
async def login(request: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AppUser).where(AppUser.username == request.username))
    user = result.scalar_one_or_none()
    
    if not user or not verify_password(request.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    if not user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    
    access_token = create_access_token(
        subject=user.id,
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    
    return Token(access_token=access_token)


# Registration endpoint disabled for security
# @router.post("/register", response_model=UserResponse)
# async def register(request: UserCreate, db: AsyncSession = Depends(get_db)):
#     user_count = await db.execute(select(func.count(AppUser.id)))
#     count = user_count.scalar() or 0
#     
#     if count > 0:
#         raise HTTPException(
#             status_code=status.HTTP_403_FORBIDDEN,
#             detail="Registration closed. Contact administrator."
#         )
#     
#     result = await db.execute(select(AppUser).where(AppUser.username == request.username))
#     existing_user = result.scalar_one_or_none()
#     
#     if existing_user:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Username already registered"
#         )
#     
#     user = AppUser(
#         username=request.username,
#         hashed_password=get_password_hash(request.password),
#         is_superuser=True
#     )
#     
#     db.add(user)
#     await db.commit()
#     await db.refresh(user)
#     
#     return user


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: AppUser = Depends(get_current_user)):
    return current_user
