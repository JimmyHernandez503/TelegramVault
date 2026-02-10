import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

from backend.app.core.logging_config import setup_logging, get_logger
setup_logging()

from backend.app.core.config import settings as app_settings
from backend.app.core.config_manager import get_config_manager
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem

logger = get_logger("app")

# Initialize ConfigManager and EnhancedLoggingSystem
config_manager = get_config_manager()
enhanced_logger = EnhancedLoggingSystem(log_dir="logs")
from backend.app.db.database import create_tables
from backend.app.api.routes import auth, accounts, groups, stats, users, invites, detections
from backend.app.api.routes import telegram, tasks, websocket, correlation, export
from backend.app.api.routes import settings as settings_routes
from backend.app.api.routes import media as media_routes
from backend.app.api.routes import member_scrape as member_scrape_routes
from backend.app.api.routes import stories as stories_routes
from backend.app.services.task_queue import task_queue
from backend.app.services.telegram_service import telegram_manager
from backend.app.services.detection_service import detection_service
from backend.app.db.database import async_session_maker


async def run_pending_migrations():
    migrations_dir = os.path.join(os.path.dirname(__file__), "db", "migrations")
    if os.path.exists(migrations_dir):
        from sqlalchemy import text
        async with async_session_maker() as db:
            for filename in sorted(os.listdir(migrations_dir)):
                if filename.endswith(".sql"):
                    filepath = os.path.join(migrations_dir, filename)
                    with open(filepath, 'r') as f:
                        lines = f.readlines()
                    
                    sql_lines = [line for line in lines if not line.strip().startswith('--')]
                    sql_content = ''.join(sql_lines)
                    
                    try:
                        for statement in sql_content.split(';'):
                            statement = statement.strip()
                            if statement:
                                await db.execute(text(statement))
                        await db.commit()
                        logger.info(f"Applied migration: {filename}")
                    except Exception as e:
                        logger.debug(f"Migration {filename} skipped (already applied or error): {e}")
                        await db.rollback()


async def log_startup_configurations():
    """
    Log all loaded configurations at system startup.
    
    This function logs all configuration values using the EnhancedLoggingSystem's
    log_metrics() method. Sensitive values (tokens, passwords, API keys) are
    automatically masked by ConfigManager.get_all(hide_sensitive=True).
    
    Validates: Requirements 9.5
    """
    try:
        # Get all configurations with sensitive values masked
        all_configs = config_manager.get_all(hide_sensitive=True)
        
        # Log configurations using log_metrics
        await enhanced_logger.log_metrics(
            service="ConfigManager",
            metrics={
                "event": "startup_configuration_loaded",
                "total_configurations": len(all_configs),
                "configurations": all_configs,
                "config_loaded": config_manager.is_loaded(),
                "validation_errors": config_manager.get_validation_errors()
            }
        )
        
        # Also log a summary message
        await enhanced_logger.log_info(
            component="ConfigManager",
            operation="startup",
            message=f"System started with {len(all_configs)} configurations loaded",
            details={
                "config_loaded": config_manager.is_loaded(),
                "has_validation_errors": len(config_manager.get_validation_errors()) > 0
            }
        )
        
        logger.info(f"Logged {len(all_configs)} configurations at startup (sensitive values masked)")
        
    except Exception as e:
        logger.error(f"Failed to log startup configurations: {e}")
        # Don't fail startup if logging fails
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize enhanced logging system
    await enhanced_logger.initialize()
    
    await create_tables()
    await run_pending_migrations()
    os.makedirs(app_settings.MEDIA_PATH, exist_ok=True)
    await task_queue.start()
    
    async with async_session_maker() as db:
        await detection_service.seed_builtin_detectors(db)
    
    # Log all loaded configurations (with sensitive values masked)
    await log_startup_configurations()
    
    # Store background tasks to keep them alive
    app.state.background_tasks = []
    
    asyncio.create_task(_auto_start_monitors(app))
    
    yield
    
    await telegram_manager.live_monitor.stop_all()
    await task_queue.stop()
    
    # Stop passive enrichment service
    try:
        from backend.app.services.passive_enrichment_service import passive_enrichment_service
        await passive_enrichment_service.stop()
    except:
        pass
    
    # Stop media retry service
    try:
        from backend.app.services.media_retry_service import media_retry_service
        await media_retry_service.stop()
    except:
        pass


async def _auto_start_monitors(app):
    import asyncio as aio
    await aio.sleep(3)
    
    from backend.app.models.telegram_account import TelegramAccount
    from sqlalchemy import select
    
    try:
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramAccount).where(TelegramAccount.status == "active")
            )
            accounts = result.scalars().all()
            
            for account in accounts:
                try:
                    await telegram_manager.connect_account(account.id, db)
                    logger.info(f"Connected account {account.id} ({account.phone})")
                except Exception as e:
                    logger.error(f"Failed to connect account {account.id}: {e}")
        
        await telegram_manager.live_monitor.start_all_enabled()
        
        started = await telegram_manager.backfill_service.start_all_pending_backfills()
        logger.info(f"Auto-started {started} pending backfills")
        
        from backend.app.services.story_monitor import story_monitor
        await story_monitor.start(telegram_manager)
        logger.info("Story monitor started")
        
        from backend.app.services.member_scrape_scheduler import init_member_scrape_scheduler
        member_scheduler = init_member_scrape_scheduler(telegram_manager)
        await member_scheduler.start()
        logger.info("Member scrape scheduler started")
        
        from backend.app.services.autojoin_service import autojoin_service
        await autojoin_service.start(telegram_manager)
        logger.info("AutoJoin service started")
        
        from backend.app.services.profile_photo_scanner import profile_photo_scanner
        await profile_photo_scanner.start(telegram_manager)
        logger.info("Profile photo scanner started")
        
        # Start user enricher worker for automatic user enrichment
        from backend.app.services.user_enricher import user_enricher
        try:
            await user_enricher.start_worker()
            logger.info("User enricher worker started")
            
            # Queue existing users for enrichment (only those without basic info)
            await aio.sleep(5)  # Wait for worker to fully start
            await _queue_existing_users_for_enrichment()
        except Exception as enricher_error:
            logger.error(f"Failed to start user enricher worker: {enricher_error}")
            # Continue with application startup even if enricher fails
        
        # Start passive enrichment service for continuous background enrichment
        passive_enabled = config_manager.get_bool("PASSIVE_ENRICHMENT_ENABLED", True)
        if passive_enabled:
            from backend.app.services.passive_enrichment_service import passive_enrichment_service
            try:
                await aio.sleep(2)  # Wait for enricher to be ready
                started = await passive_enrichment_service.start()
                if started:
                    logger.info("Passive enrichment service started")
                    # Store reference in app state to keep it alive
                    app.state.passive_enrichment_service = passive_enrichment_service
                    app.state.background_tasks.append(passive_enrichment_service._task)
                else:
                    logger.warning("Failed to start passive enrichment service")
            except Exception as passive_error:
                logger.error(f"Failed to start passive enrichment service: {passive_error}")
                import traceback
                traceback.print_exc()
                # Continue with application startup even if passive enrichment fails
        
        # Start media retry service for automatic media download retries
        media_retry_enabled = config_manager.get_bool("MEDIA_RETRY_ENABLED", True)
        if media_retry_enabled:
            from backend.app.services.media_retry_service import media_retry_service
            try:
                await aio.sleep(1)  # Brief wait before starting
                await media_retry_service.start()
                logger.info("Media retry service started")
                # Store reference in app state to keep it alive
                app.state.media_retry_service = media_retry_service
                if media_retry_service._task:
                    app.state.background_tasks.append(media_retry_service._task)
            except Exception as media_retry_error:
                logger.error(f"Failed to start media retry service: {media_retry_error}")
                import traceback
                traceback.print_exc()
                # Continue with application startup even if media retry fails
        
    except Exception as e:
        logger.error(f"Auto-start monitors failed: {e}")


async def _queue_existing_users_for_enrichment():
    """Queue existing users that need enrichment"""
    from backend.app.models.telegram_account import TelegramAccount
    from backend.app.models.telegram_user import TelegramUser
    from backend.app.services.user_enricher import user_enricher
    from sqlalchemy import select
    
    try:
        logger.info("Queueing existing users for enrichment...")
        
        # Get a connected client
        async with async_session_maker() as db:
            result = await db.execute(
                select(TelegramAccount).where(TelegramAccount.is_active == True)
            )
            accounts = result.scalars().all()
            
            if not accounts:
                logger.warning("No active accounts found for bulk enrichment")
                return
            
            client = None
            for account in accounts:
                test_client = telegram_manager.clients.get(account.id)
                if test_client and test_client.is_connected():
                    client = test_client
                    break
            
            if not client:
                logger.warning("No connected clients found for bulk enrichment")
                return
            
            # Query users without basic info in batches
            batch_size = 100
            offset = 0
            total_queued = 0
            
            while True:
                async with async_session_maker() as db:
                    query = (
                        select(TelegramUser)
                        .where(
                            (TelegramUser.username.is_(None)) & 
                            (TelegramUser.first_name.is_(None)) &
                            (TelegramUser.is_deleted == False) &
                            (TelegramUser.access_hash.isnot(None))
                        )
                        .order_by(TelegramUser.id)
                        .offset(offset)
                        .limit(batch_size)
                    )
                    
                    result = await db.execute(query)
                    users = result.scalars().all()
                    
                    if not users:
                        break
                    
                    for user in users:
                        await user_enricher.queue_enrichment(
                            client=client,
                            telegram_id=user.telegram_id,
                            group_id=None,
                            source="startup_bulk"
                        )
                        total_queued += 1
                    
                    offset += batch_size
                    
                    # Log progress every 1000 users
                    if total_queued % 1000 == 0:
                        logger.info(f"Queued {total_queued} users for enrichment...")
                    
                    # Limit to 5000 users per startup to avoid overwhelming the queue
                    if total_queued >= 5000:
                        logger.info(f"Reached limit of 5000 users, stopping bulk queue")
                        break
            
            logger.info(f"Queued {total_queued} existing users for enrichment")
    
    except Exception as e:
        logger.error(f"Error queueing existing users: {e}")


app = FastAPI(
    title=app_settings.PROJECT_NAME,
    version=app_settings.VERSION,
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix=f"{app_settings.API_V1_STR}/auth", tags=["auth"])
app.include_router(accounts.router, prefix=f"{app_settings.API_V1_STR}/accounts", tags=["accounts"])
app.include_router(groups.router, prefix=f"{app_settings.API_V1_STR}/groups", tags=["groups"])
app.include_router(users.router, prefix=f"{app_settings.API_V1_STR}/users", tags=["users"])
app.include_router(invites.router, prefix=f"{app_settings.API_V1_STR}/invites", tags=["invites"])
app.include_router(stats.router, prefix=f"{app_settings.API_V1_STR}/stats", tags=["stats"])
app.include_router(detections.router, prefix=f"{app_settings.API_V1_STR}/detections", tags=["detections"])
app.include_router(telegram.router, prefix=f"{app_settings.API_V1_STR}/telegram", tags=["telegram"])
app.include_router(tasks.router, prefix=f"{app_settings.API_V1_STR}/tasks", tags=["tasks"])
app.include_router(correlation.router, prefix=f"{app_settings.API_V1_STR}/correlation", tags=["correlation"])
app.include_router(export.router, prefix=f"{app_settings.API_V1_STR}/export", tags=["export"])
app.include_router(websocket.router, tags=["websocket"])
app.include_router(settings_routes.router, prefix=f"{app_settings.API_V1_STR}/settings", tags=["settings"])
app.include_router(media_routes.router, prefix=f"{app_settings.API_V1_STR}/media", tags=["media"])
app.include_router(member_scrape_routes.router, prefix=f"{app_settings.API_V1_STR}", tags=["member-scrape"])
app.include_router(stories_routes.router, prefix=f"{app_settings.API_V1_STR}", tags=["stories"])

from backend.app.api.routes import search as search_routes
app.include_router(search_routes.router, prefix=f"{app_settings.API_V1_STR}/search", tags=["search"])

from backend.app.api.routes import profile_photos as profile_photos_routes
app.include_router(profile_photos_routes.router, prefix=f"{app_settings.API_V1_STR}/profile-photos", tags=["profile-photos"])

from backend.app.api.routes import crawler as crawler_routes
app.include_router(crawler_routes.router, prefix=f"{app_settings.API_V1_STR}/crawler", tags=["crawler"])


app.mount("/media", StaticFiles(directory=app_settings.MEDIA_PATH), name="media")


@app.get("/")
async def root():
    return {"message": "TelegramVault API", "version": app_settings.VERSION, "docs": "/api/docs"}


@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "version": app_settings.VERSION}
