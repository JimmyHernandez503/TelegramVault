from datetime import datetime
from sqlalchemy import String, Integer, BigInteger, Text, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class DownloadTask(Base, TimestampMixin):
    """Model for tracking individual download tasks in the queue system."""
    __tablename__ = "download_tasks"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    media_file_id: Mapped[int] = mapped_column(ForeignKey("media_files.id", ondelete="CASCADE"), nullable=False, index=True)
    batch_id: Mapped[int | None] = mapped_column(ForeignKey("batch_processing.id", ondelete="SET NULL"), nullable=True, index=True)
    
    # Task details
    task_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # download, retry, validation, cleanup
    priority: Mapped[int] = mapped_column(Integer, default=0, index=True)
    status: Mapped[str] = mapped_column(String(20), default="queued", index=True)  # queued, assigned, processing, completed, failed, cancelled
    
    # Processing details
    assigned_worker: Mapped[str | None] = mapped_column(String(50), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    # Error handling and retry logic
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_category: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    
    # Task metadata
    task_data: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON data for task-specific parameters
    
    # Relationships
    media_file = relationship("MediaFile", back_populates="download_tasks")
    batch = relationship("BatchProcessing", back_populates="tasks")


class BatchProcessing(Base, TimestampMixin):
    """Model for tracking batch processing operations."""
    __tablename__ = "batch_processing"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    batch_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # retry, validation, cleanup, migration
    
    # Batch configuration
    batch_size: Mapped[int] = mapped_column(Integer, nullable=False)
    max_concurrent: Mapped[int] = mapped_column(Integer, default=5)
    filter_criteria: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON filter criteria
    
    # Progress tracking
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)  # pending, running, paused, completed, failed, cancelled
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    processed_items: Mapped[int] = mapped_column(Integer, default=0)
    successful_items: Mapped[int] = mapped_column(Integer, default=0)
    failed_items: Mapped[int] = mapped_column(Integer, default=0)
    skipped_items: Mapped[int] = mapped_column(Integer, default=0)
    
    # Timing and estimation
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    estimated_completion: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    # Checkpointing for resumability
    last_checkpoint: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    checkpoint_data: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON checkpoint data
    
    # Error tracking
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    # Batch metadata for additional configuration
    batch_metadata: Mapped[str | None] = mapped_column("metadata", Text, nullable=True)  # JSON metadata
    
    # Relationships
    tasks = relationship("DownloadTask", back_populates="batch", cascade="all, delete-orphan")