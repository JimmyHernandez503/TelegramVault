import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)
    
    file_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-25s | %(funcName)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    app_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "app.log"),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    app_handler.setLevel(logging.DEBUG)
    app_handler.setFormatter(file_format)
    root_logger.addHandler(app_handler)
    
    error_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "error.log"),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_format)
    root_logger.addHandler(error_handler)
    
    telegram_logger = logging.getLogger("telegram")
    telegram_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "telegram.log"),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    telegram_handler.setLevel(logging.DEBUG)
    telegram_handler.setFormatter(file_format)
    telegram_logger.addHandler(telegram_handler)
    
    monitor_logger = logging.getLogger("monitor")
    monitor_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "monitor.log"),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    monitor_handler.setLevel(logging.DEBUG)
    monitor_handler.setFormatter(file_format)
    monitor_logger.addHandler(monitor_handler)
    
    backfill_logger = logging.getLogger("backfill")
    backfill_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "backfill.log"),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    backfill_handler.setLevel(logging.DEBUG)
    backfill_handler.setFormatter(file_format)
    backfill_logger.addHandler(backfill_handler)
    
    media_logger = logging.getLogger("media")
    media_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "media.log"),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    media_handler.setLevel(logging.DEBUG)
    media_handler.setFormatter(file_format)
    media_logger.addHandler(media_handler)
    
    detection_logger = logging.getLogger("detection")
    detection_handler = RotatingFileHandler(
        os.path.join(LOGS_DIR, "detection.log"),
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    detection_handler.setLevel(logging.DEBUG)
    detection_handler.setFormatter(file_format)
    detection_logger.addHandler(detection_handler)
    
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    logging.info("Logging system initialized")
    logging.info(f"Log files location: {os.path.abspath(LOGS_DIR)}")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
