from datetime import datetime
from sqlalchemy import String, Boolean, Integer, BigInteger, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class TelegramUser(Base, TimestampMixin):
    __tablename__ = "telegram_users"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    access_hash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    
    username: Mapped[str | None] = mapped_column(String(100), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(20), nullable=True)
    bio: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    is_premium: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    is_verified: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    is_bot: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    is_scam: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    is_fake: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    is_restricted: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    
    last_seen: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    current_photo_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    has_stories: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    last_photo_scan: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    messages_count: Mapped[int] = mapped_column(Integer, default=0, server_default='0')
    groups_count: Mapped[int] = mapped_column(Integer, default=0, server_default='0')
    media_count: Mapped[int] = mapped_column(Integer, default=0, server_default='0')
    attachments_count: Mapped[int] = mapped_column(Integer, default=0, server_default='0')
    
    is_watchlist: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    is_favorite: Mapped[bool] = mapped_column(Boolean, default=False, server_default='false')
    
    messages = relationship("TelegramMessage", back_populates="sender")
    memberships = relationship("GroupMembership", back_populates="user")
    profile_changes = relationship("UserProfileHistory", back_populates="user")
    profile_photos = relationship("UserProfilePhoto", back_populates="user")
    stories = relationship("UserStory", back_populates="user")
