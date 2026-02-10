from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from backend.app.core.config import settings


def convert_db_url_for_asyncpg(url: str) -> str:
    url = url.replace("postgresql://", "postgresql+asyncpg://")
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    query_params.pop('sslmode', None)
    new_query = urlencode(query_params, doseq=True)
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)


DATABASE_URL = convert_db_url_for_asyncpg(settings.DATABASE_URL)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=300,
)

async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


async def create_tables():
    from backend.app.models import (
        AppUser, TelegramAccount, TelegramGroup, TelegramUser,
        TelegramMessage, MediaFile, Detection, RegexDetector,
        GroupMembership, UserProfileHistory, UserProfilePhoto,
        MessageEdit, InviteLink, GlobalConfig, GroupTemplate, DomainWatchlist,
        UserActivity, UserCorrelation
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
