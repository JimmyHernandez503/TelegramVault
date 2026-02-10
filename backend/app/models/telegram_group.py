from datetime import datetime
from sqlalchemy import String, Boolean, Integer, BigInteger, Text, DateTime, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin
import enum


class GroupType(enum.Enum):
    GROUP = "group"
    SUPERGROUP = "supergroup"
    CHANNEL = "channel"
    MEGAGROUP = "megagroup"


class GroupStatus(enum.Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    BACKFILLING = "backfilling"
    ERROR = "error"


class TelegramGroup(Base, TimestampMixin):
    __tablename__ = "telegram_groups"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    access_hash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str | None] = mapped_column(String(100), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    group_type: Mapped[str] = mapped_column(String(20), default=GroupType.GROUP.value)
    status: Mapped[str] = mapped_column(String(20), default=GroupStatus.ACTIVE.value)
    
    member_count: Mapped[int] = mapped_column(Integer, default=0)
    messages_count: Mapped[int] = mapped_column(Integer, default=0)
    
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)
    has_protected_content: Mapped[bool] = mapped_column(Boolean, default=False)
    
    photo_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    
    linked_chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    
    backfill_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    download_media: Mapped[bool] = mapped_column(Boolean, default=True)
    ocr_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    
    last_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_backfill_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    backfill_in_progress: Mapped[bool] = mapped_column(Boolean, default=False)
    backfill_done: Mapped[bool] = mapped_column(Boolean, default=False)
    is_monitoring: Mapped[bool] = mapped_column(Boolean, default=False)
    
    last_member_scrape_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    assigned_account_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_accounts.id"), nullable=True)
    assigned_account = relationship("TelegramAccount", back_populates="groups")
    
    messages = relationship("TelegramMessage", back_populates="group")
    members = relationship("GroupMembership", back_populates="group")
