import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    PROJECT_NAME: str = "TelegramVault"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    DATABASE_URL: str = os.environ.get("DATABASE_URL", "")
    
    SECRET_KEY: str = os.environ.get("SECRET_KEY", "your-secret-key-change-in-production")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days
    
    MEDIA_PATH: str = "./media"
    
    TELEGRAM_API_ID: int = int(os.environ.get("TELEGRAM_API_ID", "0"))
    TELEGRAM_API_HASH: str = os.environ.get("TELEGRAM_API_HASH", "")
    
    class Config:
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
