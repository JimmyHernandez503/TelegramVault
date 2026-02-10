from datetime import datetime
from sqlalchemy import String, Boolean, Integer, BigInteger, DateTime, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class GroupMembership(Base, TimestampMixin):
    __tablename__ = "group_memberships"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    
    user_id: Mapped[int] = mapped_column(ForeignKey("telegram_users.id"), nullable=False)
    group_id: Mapped[int] = mapped_column(ForeignKey("telegram_groups.id"), nullable=False)
    
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    admin_permissions: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    admin_title: Mapped[str | None] = mapped_column(String(100), nullable=True)
    
    joined_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    left_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    invited_by_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    leave_reason: Mapped[str | None] = mapped_column(String(20), nullable=True)  # left, kicked, banned
    
    user = relationship("TelegramUser", back_populates="memberships")
    group = relationship("TelegramGroup", back_populates="members")
