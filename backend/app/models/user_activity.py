from datetime import datetime
from sqlalchemy import String, Boolean, Integer, BigInteger, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class UserActivity(Base, TimestampMixin):
    __tablename__ = "user_activities"
    
    __table_args__ = (
        Index('idx_user_activity_user_time', 'telegram_user_id', 'timestamp'),
        Index('idx_user_activity_time', 'timestamp'),
    )
    
    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_online: Mapped[bool] = mapped_column(Boolean, default=False)
    was_online: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    
    activity_type: Mapped[str] = mapped_column(String(20), default="status")


class UserCorrelation(Base, TimestampMixin):
    __tablename__ = "user_correlations"
    
    __table_args__ = (
        Index('idx_correlation_users', 'user_a_id', 'user_b_id'),
        Index('idx_correlation_score', 'correlation_score'),
    )
    
    id: Mapped[int] = mapped_column(primary_key=True)
    
    user_a_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_b_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    
    correlation_type: Mapped[str] = mapped_column(String(30), nullable=False)
    correlation_score: Mapped[float] = mapped_column(nullable=False)
    
    lag_hours: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sample_size: Mapped[int] = mapped_column(Integer, default=0)
    
    shared_groups: Mapped[int] = mapped_column(Integer, default=0)
    shared_messages: Mapped[int] = mapped_column(Integer, default=0)
    
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
