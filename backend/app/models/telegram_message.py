from datetime import datetime
from sqlalchemy import String, Boolean, Integer, BigInteger, Text, DateTime, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class TelegramMessage(Base, TimestampMixin):
    __tablename__ = "telegram_messages"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    
    group_id: Mapped[int] = mapped_column(ForeignKey("telegram_groups.id"), nullable=False)
    sender_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_users.id"), nullable=True)
    
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_type: Mapped[str] = mapped_column(String(20), default="text")  # text, photo, video, document, etc.
    
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    edit_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    reply_to_msg_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    reply_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    forward_from_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    forward_from_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    forward_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    views: Mapped[int | None] = mapped_column(Integer, nullable=True)
    forwards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    
    mentions: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    reactions: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    
    is_pinned: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    
    grouped_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    
    group = relationship("TelegramGroup", back_populates="messages")
    sender = relationship("TelegramUser", back_populates="messages")
    media = relationship("MediaFile", back_populates="message")
    edits = relationship("MessageEdit", back_populates="message")
    detections = relationship("Detection", back_populates="message")
