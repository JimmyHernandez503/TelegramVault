from datetime import datetime
from sqlalchemy import String, Text, DateTime, ForeignKey, JSON, BigInteger, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class UserProfileHistory(Base):
    __tablename__ = "user_profile_history"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("telegram_users.id"), nullable=False)
    
    field_changed: Mapped[str] = mapped_column(String(50), nullable=False)  # username, first_name, last_name, bio, phone, photo
    old_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    changed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    
    user = relationship("TelegramUser", back_populates="profile_changes")


class UserProfilePhoto(Base, TimestampMixin):
    __tablename__ = "user_profile_photos"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("telegram_users.id"), nullable=False)
    
    photo_id: Mapped[str] = mapped_column(String(100), nullable=False)
    telegram_photo_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    file_path: Mapped[str] = mapped_column(String(500), nullable=False)
    file_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_current: Mapped[bool] = mapped_column(default=False)
    is_video: Mapped[bool] = mapped_column(Boolean, default=False)
    captured_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    user = relationship("TelegramUser", back_populates="profile_photos")


class MessageEdit(Base):
    __tablename__ = "message_edits"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    message_id: Mapped[int] = mapped_column(ForeignKey("telegram_messages.id"), nullable=False)
    
    old_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    edited_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    
    message = relationship("TelegramMessage", back_populates="edits")


class UserStory(Base, TimestampMixin):
    __tablename__ = "user_stories"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("telegram_users.id"), nullable=False)
    
    story_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    story_type: Mapped[str] = mapped_column(String(20), nullable=False)  # photo, video
    
    file_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    caption: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    
    views_count: Mapped[int] = mapped_column(Integer, default=0)
    
    posted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    is_pinned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_public: Mapped[bool] = mapped_column(Boolean, default=True)
    
    user = relationship("TelegramUser", back_populates="stories")
