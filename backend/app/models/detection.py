from datetime import datetime
from sqlalchemy import String, Integer, BigInteger, Text, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class Detection(Base, TimestampMixin):
    __tablename__ = "detections"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    
    message_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_messages.id"), nullable=True)
    media_id: Mapped[int | None] = mapped_column(ForeignKey("media_files.id"), nullable=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_users.id"), nullable=True)
    group_id: Mapped[int | None] = mapped_column(ForeignKey("telegram_groups.id"), nullable=True)
    
    detector_id: Mapped[int] = mapped_column(ForeignKey("regex_detectors.id"), nullable=False)
    
    detection_type: Mapped[str] = mapped_column(String(50), nullable=False)  # phone, email, url, crypto, etc.
    matched_text: Mapped[str] = mapped_column(Text, nullable=False)
    context_before: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_after: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    source: Mapped[str] = mapped_column(String(20), default="text")  # text, ocr, qr, barcode
    
    message = relationship("TelegramMessage", back_populates="detections")
    detector = relationship("RegexDetector", back_populates="detections")


class RegexDetector(Base, TimestampMixin):
    __tablename__ = "regex_detectors"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    pattern: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)  # phone, email, url, crypto, custom
    priority: Mapped[int] = mapped_column(Integer, default=5)
    
    is_builtin: Mapped[bool] = mapped_column(default=False)
    is_active: Mapped[bool] = mapped_column(default=True)
    
    detections = relationship("Detection", back_populates="detector")
