from sqlalchemy import String, Boolean, Integer, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class GlobalConfig(Base, TimestampMixin):
    __tablename__ = "global_config"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    value: Mapped[str | None] = mapped_column(Text, nullable=True)
    value_type: Mapped[str] = mapped_column(String(20), default="string")  # string, int, bool, json


class GroupTemplate(Base, TimestampMixin):
    __tablename__ = "group_templates"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)


class DomainWatchlist(Base, TimestampMixin):
    __tablename__ = "domain_watchlist"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    domain: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    mention_count: Mapped[int] = mapped_column(Integer, default=0)
