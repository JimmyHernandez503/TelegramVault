from datetime import datetime
from sqlalchemy import String, Integer, BigInteger, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class InviteLink(Base, TimestampMixin):
    __tablename__ = "invite_links"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    
    link: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    invite_hash: Mapped[str | None] = mapped_column(String(100), unique=True, nullable=True, index=True)
    
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, processing, joined, failed, expired, revoked, already_member, invalid, flood_wait, error
    
    assigned_account_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_accounts.id"), nullable=True)
    joined_group_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_groups.id"), nullable=True)
    source_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_group_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_groups.id"), nullable=True)
    source_user_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_users.id"), nullable=True)
    
    preview_title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    preview_about: Mapped[str | None] = mapped_column(Text, nullable=True)
    preview_member_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    preview_photo_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    preview_is_channel: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    preview_fetched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    preview_retry_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    source_group = relationship("TelegramGroup", foreign_keys=[source_group_id])
    source_user = relationship("TelegramUser", foreign_keys=[source_user_id])
    joined_group = relationship("TelegramGroup", foreign_keys=[joined_group_id])
