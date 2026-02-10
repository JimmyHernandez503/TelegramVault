from sqlalchemy import String, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from backend.app.db.database import Base
from backend.app.models.base import TimestampMixin


class AppUser(Base, TimestampMixin):
    __tablename__ = "app_users"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_superuser: Mapped[bool] = mapped_column(Boolean, default=False)
