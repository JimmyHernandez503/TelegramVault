from datetime import datetime
from sqlalchemy import String, Boolean, Integer, BigInteger, Text, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class MediaFile(Base, TimestampMixin):
    __tablename__ = "media_files"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    
    message_id: Mapped[int] = mapped_column(ForeignKey("telegram_messages.id"), nullable=False, unique=True)
    
    file_type: Mapped[str] = mapped_column(String(20), nullable=False)  # photo, video, document, audio, voice, sticker, gif, video_note
    file_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True)
    
    ocr_status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, processing, completed, error, skipped
    ocr_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    ocr_processed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    qr_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    barcode_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    perceptual_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    
    file_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    unique_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    
    is_self_destructing: Mapped[bool] = mapped_column(Boolean, default=False)
    ttl_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    
    # Enhanced fields for better download tracking
    download_attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_download_attempt: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    download_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    download_error_category: Mapped[str | None] = mapped_column(String(50), nullable=True)
    
    # Validation and integrity fields
    validation_status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, valid, invalid, corrupted
    validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    # Processing status and queue management
    processing_status: Mapped[str] = mapped_column(String(20), default="pending")  # pending, queued, processing, completed, failed
    processing_priority: Mapped[int] = mapped_column(Integer, default=0)
    
    # Enhanced duplicate detection
    is_duplicate: Mapped[bool] = mapped_column(Boolean, default=False)
    original_media_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duplicate_detection_method: Mapped[str | None] = mapped_column(String(20), nullable=True)  # hash, perceptual, manual
    
    message = relationship("TelegramMessage", back_populates="media")
    download_tasks = relationship("DownloadTask", back_populates="media_file", cascade="all, delete-orphan")
