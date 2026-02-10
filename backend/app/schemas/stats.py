from datetime import datetime
from pydantic import BaseModel


class DashboardStats(BaseModel):
    total_messages: int
    total_users: int
    total_groups: int
    total_media: int
    total_detections: int
    active_accounts: int
    total_accounts: int
    pending_invites: int
    backfills_in_progress: int


class AccountStats(BaseModel):
    id: int
    phone: str
    status: str
    groups_count: int
    messages_collected: int
    errors_count: int


class GroupStats(BaseModel):
    id: int
    title: str
    telegram_id: int
    status: str
    member_count: int
    messages_count: int
    messages_today: int


class ActivityData(BaseModel):
    date: str
    messages: int
    media: int
    users: int
