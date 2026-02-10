from datetime import datetime
from pydantic import BaseModel


class TelegramAccountCreate(BaseModel):
    phone: str
    api_id: int | None = None
    api_hash: str | None = None
    proxy_type: str | None = None
    proxy_host: str | None = None
    proxy_port: int | None = None
    proxy_username: str | None = None
    proxy_password: str | None = None


class TelegramAccountUpdate(BaseModel):
    is_active: bool | None = None
    proxy_type: str | None = None
    proxy_host: str | None = None
    proxy_port: int | None = None
    proxy_username: str | None = None
    proxy_password: str | None = None


class TelegramAccountResponse(BaseModel):
    id: int
    phone: str
    api_id: int
    telegram_id: int | None = None
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    status: str
    is_active: bool
    messages_collected: int
    errors_count: int
    proxy_type: str | None = None
    proxy_host: str | None = None
    proxy_port: int | None = None
    last_activity: datetime | None = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class TelegramGroupCreate(BaseModel):
    telegram_id: int
    title: str
    username: str | None = None
    group_type: str = "group"


class TelegramGroupUpdate(BaseModel):
    backfill_enabled: bool | None = None
    download_media: bool | None = None
    ocr_enabled: bool | None = None
    status: str | None = None
    assigned_account_id: int | None = None


class TelegramGroupResponse(BaseModel):
    id: int
    telegram_id: int
    title: str
    username: str | None
    description: str | None
    group_type: str
    status: str
    member_count: int
    messages_count: int
    is_public: bool
    backfill_enabled: bool
    download_media: bool
    ocr_enabled: bool
    photo_path: str | None
    assigned_account_id: int | None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class TelegramUserResponse(BaseModel):
    id: int
    telegram_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    phone: str | None
    bio: str | None
    is_premium: bool
    is_verified: bool
    is_bot: bool
    is_scam: bool
    is_fake: bool
    is_watchlist: bool
    is_favorite: bool
    messages_count: int
    groups_count: int
    media_count: int = 0
    last_seen: datetime | None
    current_photo_path: str | None
    has_stories: bool = False
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class TelegramMessageResponse(BaseModel):
    id: int
    telegram_id: int
    group_id: int
    sender_id: int | None
    text: str | None
    message_type: str
    date: datetime
    edit_date: datetime | None
    views: int | None
    forwards: int | None
    is_pinned: bool
    is_deleted: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


class InviteLinkCreate(BaseModel):
    link: str


class InviteLinkResponse(BaseModel):
    id: int
    link: str
    invite_hash: str | None = None
    status: str
    retry_count: int
    last_error: str | None
    preview_title: str | None = None
    preview_about: str | None = None
    preview_member_count: int | None = None
    preview_photo_path: str | None = None
    preview_is_channel: bool | None = None
    preview_fetched_at: datetime | None = None
    source_group_id: int | None = None
    source_user_id: int | None = None
    joined_group_id: int | None = None
    created_at: datetime
    
    class Config:
        from_attributes = True
