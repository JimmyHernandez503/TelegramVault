from sqlalchemy import String, Boolean, Integer, Text, DateTime, BigInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin
from datetime import datetime
import enum


class AccountStatus(enum.Enum):
    DISCONNECTED = "disconnected"
    CONNECTED = "connected"
    FLOOD_WAIT = "flood_wait"
    BANNED = "banned"
    AUTH_REQUIRED = "auth_required"


class TelegramAccount(Base, TimestampMixin):
    __tablename__ = "telegram_accounts"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    phone: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    api_id: Mapped[int] = mapped_column(Integer, nullable=False)
    api_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    session_string: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    telegram_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    username: Mapped[str | None] = mapped_column(String(100), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_activity: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    proxy_type: Mapped[str | None] = mapped_column(String(10), nullable=True)
    proxy_host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    proxy_port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    proxy_username: Mapped[str | None] = mapped_column(String(100), nullable=True)
    proxy_password: Mapped[str | None] = mapped_column(String(100), nullable=True)
    
    status: Mapped[str] = mapped_column(String(20), default=AccountStatus.DISCONNECTED.value)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    messages_collected: Mapped[int] = mapped_column(Integer, default=0)
    errors_count: Mapped[int] = mapped_column(Integer, default=0)
    
    groups = relationship("TelegramGroup", back_populates="assigned_account")
