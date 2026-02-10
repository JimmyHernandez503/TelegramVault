# TelegramVault Migration Guide

## Overview

This guide helps you migrate existing TelegramVault code to use the new centralized configuration system, enhanced logging, and API retry wrapper. These changes improve code quality, maintainability, and system reliability.

## Table of Contents

1. [Migration Overview](#migration-overview)
2. [Migrating to ConfigManager](#migrating-to-configmanager)
3. [Migrating to EnhancedLoggingSystem](#migrating-to-enhancedloggingsystem)
4. [Migrating to APIRetryWrapper](#migrating-to-apiretryWrapper)
5. [Service-Specific Migrations](#service-specific-migrations)
6. [Testing Your Migration](#testing-your-migration)
7. [Common Issues and Troubleshooting](#common-issues-and-troubleshooting)
8. [Rollback Procedures](#rollback-procedures)

---

## Migration Overview

### What Changed?

**Before:**
- Hardcoded configuration values scattered throughout the codebase
- Inconsistent logging using `print()` statements
- Manual retry logic duplicated across services
- No centralized error handling

**After:**
- Centralized configuration through `ConfigManager`
- Structured JSON logging through `EnhancedLoggingSystem`
- Automatic retry logic through `APIRetryWrapper`
- Consistent error handling and categorization

### Benefits

✅ **Configuration:** Change settings without code modifications  
✅ **Logging:** Structured logs for better analysis and debugging  
✅ **Reliability:** Automatic retries with exponential backoff  
✅ **Maintainability:** Consistent patterns across all services  
✅ **Monitoring:** Better visibility into system behavior  

### Migration Steps

1. Update imports to include new core modules
2. Replace hardcoded values with ConfigManager calls
3. Replace print() statements with structured logging
4. Wrap API calls with APIRetryWrapper
5. Test thoroughly in development environment
6. Deploy to production with monitoring

---

## Migrating to ConfigManager

### Step 1: Import ConfigManager

**Before:**
```python
# No configuration management
MAX_RETRIES = 3
DELAY_BASE = 2
TIMEOUT = 30
```

**After:**
```python
from backend.app.core.config_manager import ConfigManager

# Initialize in service __init__
def __init__(self):
    self.config = ConfigManager()
    self.config.load()
```

### Step 2: Replace Hardcoded Values

**Before:**
```python
class MediaRetryService:
    def __init__(self):
        self.max_retries = 3  # Hardcoded
        self.delay_base = 2   # Hardcoded
        self.timeout = 30     # Hardcoded
    
    async def retry_download(self, media_file):
        for attempt in range(self.max_retries):
            # Retry logic
            pass
```

**After:**
```python
class MediaRetryService:
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        # Load from configuration
        self.max_retries = self.config.get_int("MEDIA_RETRY_MAX_ATTEMPTS")
        self.delay_base = self.config.get_int("MEDIA_RETRY_DELAY_BASE")
        self.timeout = self.config.get_int("MEDIA_DOWNLOAD_TIMEOUT")
    
    async def retry_download(self, media_file):
        for attempt in range(self.max_retries):
            # Retry logic
            pass
```

### Step 3: Use Configuration Methods

ConfigManager provides type-safe methods for retrieving values:

```python
# Integer values
max_attempts = config.get_int("MEDIA_RETRY_MAX_ATTEMPTS", default=3)

# Boolean values
validation_enabled = config.get_bool("MEDIA_VALIDATION_ENABLED", default=True)

# String values
language = config.get("SEARCH_FTS_LANGUAGE", default="spanish")

# Path values
media_dir = config.get_path("MEDIA_DIR", default="media")
```

### Complete Example: Media Retry Service

**Before:**
```python
class MediaRetryService:
    MAX_RETRIES = 3
    DELAY_BASE = 2
    TIMEOUT = 30
    
    def __init__(self):
        pass
    
    async def process_failed_downloads(self):
        print(f"Processing failed downloads with max {self.MAX_RETRIES} retries")
        # Process logic
```

**After:**
```python
from backend.app.core.config_manager import ConfigManager

class MediaRetryService:
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.max_retries = self.config.get_int("MEDIA_RETRY_MAX_ATTEMPTS")
        self.delay_base = self.config.get_int("MEDIA_RETRY_DELAY_BASE")
        self.timeout = self.config.get_int("MEDIA_DOWNLOAD_TIMEOUT")
    
    async def process_failed_downloads(self):
        # Logging handled by EnhancedLoggingSystem (see next section)
        # Process logic
```

---

## Migrating to EnhancedLoggingSystem

### Step 1: Import and Initialize

**Before:**
```python
import logging

logger = logging.getLogger(__name__)
```

**After:**
```python
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem

# Initialize in service __init__
def __init__(self, logger: EnhancedLoggingSystem):
    self.logger = logger
```

### Step 2: Replace print() Statements

**Before:**
```python
def process_batch(self, items):
    print(f"Processing batch of {len(items)} items")
    
    for item in items:
        try:
            result = self.process_item(item)
            print(f"Processed item {item.id}: {result}")
        except Exception as e:
            print(f"Error processing item {item.id}: {e}")
```

**After:**
```python
async def process_batch(self, items):
    await self.logger.log_info(
        "BatchProcessor",
        "process_batch",
        f"Processing batch of {len(items)} items",
        details={"batch_size": len(items)}
    )
    
    for item in items:
        try:
            result = await self.process_item(item)
            await self.logger.log_info(
                "BatchProcessor",
                "process_item",
                f"Processed item successfully",
                details={"item_id": item.id, "result": result}
            )
        except Exception as e:
            await self.logger.log_error(
                "BatchProcessor",
                "process_item",
                f"Error processing item",
                error=e,
                details={"item_id": item.id}
            )
```

### Step 3: Use Structured Logging

The EnhancedLoggingSystem provides several logging methods:

```python
# Info logging
await logger.log_info(
    "ServiceName",
    "operation_name",
    "Message",
    details={"key": "value"}
)

# Error logging with exception
await logger.log_error(
    "ServiceName",
    "operation_name",
    "Error message",
    error=exception,
    details={"context": "data"}
)

# Warning logging
await logger.log_warning(
    "ServiceName",
    "operation_name",
    "Warning message",
    details={"threshold": 100, "actual": 150}
)

# Debug logging
await logger.log_debug(
    "ServiceName",
    "operation_name",
    "Debug message",
    details={"debug_info": "value"}
)
```

### Step 4: Use log_with_context for Simple Cases

**Before:**
```python
print(f"User enrichment completed for user {user_id}")
```

**After:**
```python
await self.logger.log_with_context(
    "INFO",
    "User enrichment completed",
    "EnhancedUserEnricherService",
    context={"user_id": user_id, "duration_ms": 450}
)
```

### Step 5: Track Operations

For long-running operations, use operation tracking:

```python
# Start operation
op_id = await self.logger.log_operation_start(
    "enrich_user_batch",
    "EnhancedUserEnricherService",
    context={"batch_size": 20}
)

try:
    # Perform operation
    result = await self.enrich_batch(users)
    
    # Log success
    await self.logger.log_operation_end(
        op_id,
        success=True,
        context={"processed": 18, "failed": 2}
    )
except Exception as e:
    # Log failure
    await self.logger.log_operation_end(
        op_id,
        success=False,
        error=e
    )
```

### Step 6: Log Metrics

For aggregated metrics, use log_metrics:

```python
await self.logger.log_metrics(
    "MediaRetryService",
    {
        "total_processed": 100,
        "successful": 95,
        "failed": 5,
        "success_rate": 0.95,
        "average_time_ms": 234.5
    }
)
```

### Complete Example: Search Service

**Before:**
```python
class SearchService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def search_messages(self, query: str):
        print(f"Searching for: {query}")
        
        try:
            results = await self._execute_fts_search(query)
            print(f"Found {len(results)} results")
            return results
        except Exception as e:
            print(f"Search failed: {e}")
            print("Falling back to ILIKE search")
            return await self._execute_ilike_search(query)
```

**After:**
```python
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem

class SearchService:
    def __init__(self, logger: EnhancedLoggingSystem):
        self.logger = logger
    
    async def search_messages(self, query: str):
        await self.logger.log_info(
            "SearchService",
            "search_messages",
            "Starting search",
            details={"query": query}
        )
        
        try:
            results = await self._execute_fts_search(query)
            await self.logger.log_info(
                "SearchService",
                "search_messages",
                "Search completed successfully",
                details={"query": query, "result_count": len(results)}
            )
            return results
        except Exception as e:
            await self.logger.log_warning(
                "SearchService",
                "search_messages",
                "FTS search failed, falling back to ILIKE",
                error=e,
                details={"query": query}
            )
            return await self._execute_ilike_search(query)
```

---

## Migrating to APIRetryWrapper

### Step 1: Import and Initialize

**Before:**
```python
async def call_api(self, user_id):
    # Manual retry logic
    for attempt in range(3):
        try:
            return await client.get_entity(user_id)
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                raise
```

**After:**
```python
from backend.app.core.api_retry_wrapper import APIRetryWrapper

def __init__(self, config: ConfigManager, logger: EnhancedLoggingSystem):
    self.retry_wrapper = APIRetryWrapper(config, logger)
```

### Step 2: Wrap API Calls

**Before:**
```python
async def get_user_entity(self, client, user_id):
    max_retries = 3
    delay = 1
    
    for attempt in range(max_retries):
        try:
            return await client.get_entity(user_id)
        except FloodWaitError as e:
            if attempt < max_retries - 1:
                wait_time = e.seconds
                await asyncio.sleep(wait_time)
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(delay * (2 ** attempt))
            else:
                raise
```

**After:**
```python
async def get_user_entity(self, client, user_id):
    result = await self.retry_wrapper.execute_with_retry(
        client.get_entity,
        user_id,
        operation_name="get_user_entity"
    )
    
    if result.success:
        return result.result
    else:
        raise result.error
```

### Step 3: Handle Retry Results

The APIRetryWrapper returns a `RetryResult` object:

```python
result = await self.retry_wrapper.execute_with_retry(
    client.get_entity,
    user_id,
    operation_name="get_user_entity"
)

if result.success:
    print(f"Success after {result.attempts} attempts")
    print(f"Total delay: {result.total_delay_ms}ms")
    return result.result
else:
    print(f"Failed after {result.attempts} attempts")
    print(f"Error: {result.error}")
    # Handle failure
```

### Step 4: Custom Retry Parameters

For specific operations that need different retry behavior:

```python
# Use custom retry parameters
result = await self.retry_wrapper.execute_with_custom_retry(
    client.download_media,
    message,
    max_attempts=10,  # More retries for media
    delay_base=5,     # Longer delays
    operation_name="download_large_media"
)
```

### Step 5: Error Categorization

The wrapper automatically categorizes errors:

```python
# Check if an error is temporary
if self.retry_wrapper.is_temporary_error(exception):
    # Will be retried automatically
    pass
else:
    # Permanent error, won't retry
    pass

# Get error category
category = self.retry_wrapper.categorize_error(exception)
# Returns: ErrorCategory.TEMPORARY, PERMANENT, or RATE_LIMIT
```

### Complete Example: User Enricher Service

**Before:**
```python
class EnhancedUserEnricherService:
    async def download_profile_photo(self, client, user):
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                print(f"Downloading photo for user {user.id}, attempt {attempt + 1}")
                photo_path = await client.download_profile_photo(user)
                print(f"Photo downloaded: {photo_path}")
                return photo_path
            except Exception as e:
                print(f"Error downloading photo: {e}")
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    print(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    print(f"Failed after {max_retries} attempts")
                    return None
```

**After:**
```python
from backend.app.core.api_retry_wrapper import APIRetryWrapper
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem

class EnhancedUserEnricherService:
    def __init__(self, config: ConfigManager, logger: EnhancedLoggingSystem):
        self.config = config
        self.logger = logger
        self.retry_wrapper = APIRetryWrapper(config, logger)
    
    async def download_profile_photo(self, client, user):
        result = await self.retry_wrapper.execute_with_retry(
            client.download_profile_photo,
            user,
            operation_name="download_profile_photo"
        )
        
        if result.success:
            await self.logger.log_info(
                "EnhancedUserEnricherService",
                "download_profile_photo",
                "Photo downloaded successfully",
                details={
                    "user_id": user.id,
                    "attempts": result.attempts,
                    "photo_path": result.result
                }
            )
            return result.result
        else:
            await self.logger.log_error(
                "EnhancedUserEnricherService",
                "download_profile_photo",
                "Failed to download photo",
                error=result.error,
                details={
                    "user_id": user.id,
                    "attempts": result.attempts
                }
            )
            return None
```

---

## Service-Specific Migrations

### Media Retry Service

**Key Changes:**
1. Replace hardcoded retry values with ConfigManager
2. Use EnhancedLoggingSystem for all output
3. Implement proper error categorization

**Before:**
```python
class MediaRetryService:
    MAX_RETRIES = 3
    
    async def retry_download(self, media_file):
        print(f"Retrying download for {media_file.file_path}")
        # Retry logic
```

**After:**
```python
class MediaRetryService:
    def __init__(self, config: ConfigManager, logger: EnhancedLoggingSystem):
        self.config = config
        self.logger = logger
        self.max_retries = config.get_int("MEDIA_RETRY_MAX_ATTEMPTS")
    
    async def retry_download(self, media_file):
        await self.logger.log_info(
            "MediaRetryService",
            "retry_download",
            "Starting download retry",
            details={
                "file_path": media_file.file_path,
                "attempt": media_file.download_attempts + 1,
                "max_retries": self.max_retries
            }
        )
        # Retry logic
```

### Search Service

**Key Changes:**
1. Use ConfigManager for FTS language and fallback settings
2. Implement structured logging for search operations
3. Log search failures with diagnostic information

**Before:**
```python
class SearchService:
    FTS_LANGUAGE = "spanish"
    FALLBACK_ENABLED = True
    
    async def search_all(self, query: str):
        print(f"Searching for: {query}")
        try:
            return await self._fts_search(query)
        except Exception as e:
            print(f"FTS failed: {e}, falling back to ILIKE")
            return await self._ilike_search(query)
```

**After:**
```python
class SearchService:
    def __init__(self, config: ConfigManager, logger: EnhancedLoggingSystem):
        self.config = config
        self.logger = logger
        self.fts_language = config.get("SEARCH_FTS_LANGUAGE")
        self.fallback_enabled = config.get_bool("SEARCH_FALLBACK_TO_ILIKE")
        self.log_failures = config.get_bool("SEARCH_LOG_FAILURES")
    
    async def search_all(self, query: str):
        op_id = await self.logger.log_operation_start(
            "search_all",
            "SearchService",
            context={"query": query, "language": self.fts_language}
        )
        
        try:
            results = await self._fts_search(query)
            await self.logger.log_operation_end(
                op_id,
                success=True,
                context={"result_count": len(results)}
            )
            return results
        except Exception as e:
            if self.log_failures:
                await self.logger.log_warning(
                    "SearchService",
                    "search_all",
                    "FTS search failed",
                    error=e,
                    details={"query": query, "fallback_enabled": self.fallback_enabled}
                )
            
            if self.fallback_enabled:
                results = await self._ilike_search(query)
                await self.logger.log_operation_end(
                    op_id,
                    success=True,
                    context={"result_count": len(results), "used_fallback": True}
                )
                return results
            else:
                await self.logger.log_operation_end(op_id, success=False, error=e)
                raise
```

### Detection Service

**Key Changes:**
1. Use ConfigManager for cache size and validation settings
2. Implement pattern validation before compilation
3. Log compilation errors with pattern details

**Before:**
```python
class DetectionService:
    CACHE_SIZE = 1000
    
    def __init__(self):
        self._pattern_cache = {}
    
    def compile_pattern(self, pattern: str):
        try:
            return re.compile(pattern)
        except Exception as e:
            print(f"Failed to compile pattern: {e}")
            return None
```

**After:**
```python
class DetectionService:
    def __init__(self, config: ConfigManager, logger: EnhancedLoggingSystem):
        self.config = config
        self.logger = logger
        self.cache_size = config.get_int("DETECTION_CACHE_SIZE")
        self.validate_patterns = config.get_bool("DETECTION_VALIDATE_PATTERNS")
        self.log_compilation_errors = config.get_bool("DETECTION_LOG_COMPILATION_ERRORS")
        self._pattern_cache = {}
    
    async def compile_pattern(self, pattern: str):
        # Validate pattern first if enabled
        if self.validate_patterns:
            try:
                re.compile(pattern)  # Test compilation
            except re.error as e:
                if self.log_compilation_errors:
                    await self.logger.log_error(
                        "DetectionService",
                        "compile_pattern",
                        "Invalid regex pattern",
                        error=e,
                        details={"pattern": pattern}
                    )
                return None
        
        # Compile and cache
        try:
            compiled = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            self._pattern_cache[pattern] = compiled
            return compiled
        except re.error as e:
            if self.log_compilation_errors:
                await self.logger.log_error(
                    "DetectionService",
                    "compile_pattern",
                    "Failed to compile pattern",
                    error=e,
                    details={"pattern": pattern}
                )
            return None
```

### User Enrichment Service

**Key Changes:**
1. Use ConfigManager for timeout and retry settings
2. Wrap API calls with APIRetryWrapper
3. Use structured logging for enrichment operations

**Before:**
```python
class EnhancedUserEnricherService:
    TIMEOUT = 30
    MAX_RETRIES = 3
    
    async def enrich_user(self, client, user_id):
        print(f"Enriching user {user_id}")
        
        for attempt in range(self.MAX_RETRIES):
            try:
                user = await client.get_entity(user_id)
                print(f"User enriched: {user.username}")
                return user
            except Exception as e:
                print(f"Error: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
```

**After:**
```python
class EnhancedUserEnricherService:
    def __init__(self, config: ConfigManager, logger: EnhancedLoggingSystem):
        self.config = config
        self.logger = logger
        self.retry_wrapper = APIRetryWrapper(config, logger)
        self.timeout = config.get_int("USER_ENRICHMENT_TIMEOUT")
        self.batch_size = config.get_int("USER_ENRICHMENT_BATCH_SIZE")
    
    async def enrich_user(self, client, user_id):
        op_id = await self.logger.log_operation_start(
            "enrich_user",
            "EnhancedUserEnricherService",
            context={"user_id": user_id}
        )
        
        result = await self.retry_wrapper.execute_with_retry(
            client.get_entity,
            user_id,
            operation_name="get_user_entity"
        )
        
        if result.success:
            user = result.result
            await self.logger.log_operation_end(
                op_id,
                success=True,
                context={
                    "user_id": user_id,
                    "username": user.username,
                    "attempts": result.attempts
                }
            )
            return user
        else:
            await self.logger.log_operation_end(
                op_id,
                success=False,
                error=result.error,
                context={"user_id": user_id, "attempts": result.attempts}
            )
            return None
```

### Media Ingestion Service

**Key Changes:**
1. Use ConfigManager for media directory and validation settings
2. Replace print() with structured logging
3. Implement file validation with proper error handling

**Before:**
```python
class MediaIngestionService:
    MEDIA_DIR = "media"
    VALIDATION_ENABLED = True
    
    async def download_media(self, message):
        print(f"Downloading media from message {message.id}")
        
        try:
            file_path = await self.client.download_media(message, self.MEDIA_DIR)
            print(f"Downloaded to: {file_path}")
            
            if self.VALIDATION_ENABLED:
                if not self.validate_file(file_path):
                    print(f"Validation failed for {file_path}")
                    os.remove(file_path)
                    return None
            
            return file_path
        except Exception as e:
            print(f"Download failed: {e}")
            return None
```

**After:**
```python
class MediaIngestionService:
    def __init__(self, config: ConfigManager, logger: EnhancedLoggingSystem):
        self.config = config
        self.logger = logger
        self.retry_wrapper = APIRetryWrapper(config, logger)
        self.media_dir = config.get_path("MEDIA_DIR")
        self.validation_enabled = config.get_bool("MEDIA_VALIDATION_ENABLED")
    
    async def download_media(self, message):
        await self.logger.log_info(
            "MediaIngestionService",
            "download_media",
            "Starting media download",
            details={"message_id": message.id}
        )
        
        result = await self.retry_wrapper.execute_with_retry(
            self.client.download_media,
            message,
            str(self.media_dir),
            operation_name="download_media"
        )
        
        if not result.success:
            await self.logger.log_error(
                "MediaIngestionService",
                "download_media",
                "Download failed",
                error=result.error,
                details={"message_id": message.id, "attempts": result.attempts}
            )
            return None
        
        file_path = result.result
        
        if self.validation_enabled:
            is_valid = await self.validate_file(file_path)
            if not is_valid:
                await self.logger.log_warning(
                    "MediaIngestionService",
                    "download_media",
                    "File validation failed, removing file",
                    details={"file_path": file_path, "message_id": message.id}
                )
                await aiofiles.os.remove(file_path)
                return None
        
        await self.logger.log_info(
            "MediaIngestionService",
            "download_media",
            "Media downloaded successfully",
            details={
                "message_id": message.id,
                "file_path": file_path,
                "attempts": result.attempts
            }
        )
        
        return file_path
```

---

## Testing Your Migration

### Unit Testing

Test that your migrated code works correctly:

```python
import pytest
from backend.app.core.config_manager import ConfigManager
from backend.app.core.enhanced_logging_system import EnhancedLoggingSystem

@pytest.fixture
async def config_manager():
    config = ConfigManager()
    config.load()
    return config

@pytest.fixture
async def logger():
    logger = EnhancedLoggingSystem()
    await logger.initialize()
    return logger

@pytest.mark.asyncio
async def test_media_retry_service(config_manager, logger):
    service = MediaRetryService(config_manager, logger)
    
    # Test that configuration is loaded
    assert service.max_retries > 0
    assert service.delay_base > 0
    
    # Test retry logic
    # ... your test code
```

### Integration Testing

Test the complete flow in Docker:

```bash
# 1. Build and start containers
docker compose up -d --build

# 2. Check logs for configuration loading
docker compose logs app | grep "Configuration"

# 3. Verify services are using new systems
docker compose logs app | grep "EnhancedLoggingSystem"

# 4. Test a specific service
docker compose exec app python -c "
from backend.app.core.config_manager import ConfigManager
config = ConfigManager()
if config.load():
    print('Configuration loaded successfully')
    print(f'Max retries: {config.get_int(\"MEDIA_RETRY_MAX_ATTEMPTS\")}')
"
```

### Validation Checklist

Before deploying to production, verify:

- [ ] All hardcoded values replaced with ConfigManager calls
- [ ] All print() statements replaced with structured logging
- [ ] All API calls wrapped with APIRetryWrapper (where appropriate)
- [ ] Configuration file (.env) updated with all required values
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Logs are in JSON format (if configured)
- [ ] No configuration validation errors on startup
- [ ] Services start successfully
- [ ] Retry logic works as expected
- [ ] Error handling works correctly

---

## Common Issues and Troubleshooting

### Issue 1: Configuration Not Loading

**Symptom:**
```
CRITICAL CONFIGURATION ERRORS:
  - Required configuration 'TELEGRAM_API_ID' is missing or has default value
```

**Solution:**
1. Check that .env file exists in the project root
2. Verify .env file has correct format (no spaces around =)
3. Ensure required variables are set:
   ```bash
   TELEGRAM_API_ID=12345678
   TELEGRAM_API_HASH=your_hash_here
   DATABASE_URL=postgresql://user:pass@host:5432/db
   SECRET_KEY=your_secret_key
   ```

### Issue 2: Import Errors

**Symptom:**
```
ImportError: cannot import name 'ConfigManager' from 'backend.app.core.config_manager'
```

**Solution:**
1. Ensure you're running from the correct directory
2. Check Python path includes the backend directory
3. In Docker, verify volume mounts are correct:
   ```yaml
   volumes:
     - ./backend:/app/backend
   ```

### Issue 3: Async/Await Errors

**Symptom:**
```
RuntimeWarning: coroutine 'log_info' was never awaited
```

**Solution:**
All logging methods are async and must be awaited:

**Wrong:**
```python
self.logger.log_info("Service", "operation", "message")  # Missing await
```

**Correct:**
```python
await self.logger.log_info("Service", "operation", "message")
```

### Issue 4: Logger Not Initialized

**Symptom:**
```
AttributeError: 'EnhancedLoggingSystem' object has no attribute '_initialized'
```

**Solution:**
Initialize the logger before use:

```python
logger = EnhancedLoggingSystem()
await logger.initialize()  # Don't forget this!
```

### Issue 5: Retry Wrapper Not Retrying

**Symptom:**
Operations fail immediately without retrying

**Solution:**
1. Check that error is categorized as temporary:
   ```python
   category = retry_wrapper.categorize_error(exception)
   print(f"Error category: {category}")
   ```

2. Verify configuration:
   ```python
   max_attempts = config.get_int("TELEGRAM_API_RETRY_MAX_ATTEMPTS")
   print(f"Max attempts: {max_attempts}")
   ```

3. Check logs for retry attempts:
   ```bash
   docker compose logs app | grep "Attempt"
   ```

### Issue 6: Logs Not Appearing

**Symptom:**
No log output or logs missing

**Solution:**
1. Check log level configuration:
   ```bash
   LOG_LEVEL=DEBUG  # Use DEBUG for verbose logging
   ```

2. Verify log directory exists and is writable:
   ```bash
   docker compose exec app ls -la /app/logs
   ```

3. Check log file location:
   ```bash
   docker compose exec app cat /app/logs/info.log
   ```

### Issue 7: JSON Logs Not Formatted

**Symptom:**
Logs are in text format instead of JSON

**Solution:**
Set LOG_FORMAT in .env:
```bash
LOG_FORMAT=json
```

Then restart the service:
```bash
docker compose restart app
```

### Issue 8: High Memory Usage

**Symptom:**
Application uses excessive memory after migration

**Solution:**
1. Reduce cache sizes:
   ```bash
   DETECTION_CACHE_SIZE=100  # Reduce from 1000
   ```

2. Reduce batch sizes:
   ```bash
   USER_ENRICHMENT_BATCH_SIZE=10  # Reduce from 20
   ```

3. Monitor memory usage:
   ```bash
   docker stats app
   ```

### Issue 9: Slow Performance

**Symptom:**
Operations are slower after migration

**Solution:**
1. Check if too many retries are happening:
   ```bash
   docker compose logs app | grep "Attempt" | wc -l
   ```

2. Adjust retry delays:
   ```bash
   TELEGRAM_API_RETRY_DELAY_BASE=1  # Reduce from 2
   ```

3. Disable unnecessary validation:
   ```bash
   DETECTION_VALIDATE_PATTERNS=false
   ```

### Issue 10: Docker Volume Sync Issues

**Symptom:**
Code changes not reflected in running container

**Solution:**
1. Verify volume mounts in docker-compose.yml:
   ```yaml
   volumes:
     - ./backend:/app/backend
   ```

2. Restart container:
   ```bash
   docker compose restart app
   ```

3. Rebuild if necessary:
   ```bash
   docker compose up -d --build
   ```

---

## Rollback Procedures

If you need to rollback the migration:

### Step 1: Identify the Issue

Check logs to understand what's failing:
```bash
docker compose logs app --tail=100
```

### Step 2: Quick Rollback (Git)

If you're using version control:
```bash
# Revert to previous commit
git revert HEAD

# Or reset to specific commit
git reset --hard <commit-hash>

# Rebuild and restart
docker compose up -d --build
```

### Step 3: Partial Rollback

Rollback specific services while keeping others:

1. Identify the problematic service
2. Restore the old version of that service file
3. Restart the container:
   ```bash
   docker compose restart app
   ```

### Step 4: Configuration Rollback

If configuration is the issue:

1. Restore old .env file:
   ```bash
   cp .env.backup .env
   ```

2. Restart services:
   ```bash
   docker compose restart app
   ```

### Step 5: Database Rollback

If database changes were made:

1. Restore from backup:
   ```bash
   docker compose exec db psql -U user -d database < backup.sql
   ```

2. Restart application:
   ```bash
   docker compose restart app
   ```

### Step 6: Verify Rollback

After rollback, verify:

1. Services start successfully:
   ```bash
   docker compose ps
   ```

2. No errors in logs:
   ```bash
   docker compose logs app --tail=50
   ```

3. Basic functionality works:
   ```bash
   # Test API endpoint
   curl http://localhost:8000/health
   ```

---

## Additional Resources

### Documentation

- [Configuration Guide](./CONFIGURATION.md) - Complete configuration reference
- [API Documentation](./API.md) - API endpoint documentation
- [Development Guide](./DEVELOPMENT.md) - Development setup and guidelines

### Code Examples

- `backend/app/services/media_retry_service.py` - Complete media retry implementation
- `backend/app/services/search_service.py` - Search service with FTS and fallback
- `backend/app/services/detection_service.py` - Detection service with pattern caching
- `backend/app/services/enhanced_user_enricher_service.py` - User enrichment with retry

### Testing

- `backend/tests/test_config_manager.py` - ConfigManager tests
- `backend/tests/test_enhanced_logging_system.py` - Logging system tests
- `backend/tests/test_api_retry_wrapper.py` - Retry wrapper tests

### Support

For additional help:
- Check application logs: `docker compose logs app`
- Review configuration: `docker compose exec app env | grep TELEGRAM`
- Test configuration loading: See "Integration Testing" section above
- Open an issue on GitHub with logs and error details

---

## Migration Checklist

Use this checklist to track your migration progress:

### Pre-Migration
- [ ] Backup current codebase
- [ ] Backup database
- [ ] Backup .env file
- [ ] Review all services that need migration
- [ ] Set up test environment

### Configuration Migration
- [ ] Import ConfigManager in all services
- [ ] Replace hardcoded values with config.get() calls
- [ ] Update .env file with all required variables
- [ ] Test configuration loading
- [ ] Verify validation works

### Logging Migration
- [ ] Import EnhancedLoggingSystem in all services
- [ ] Replace all print() statements
- [ ] Add structured logging with context
- [ ] Implement operation tracking for long operations
- [ ] Add metrics logging where appropriate
- [ ] Test log output format

### Retry Logic Migration
- [ ] Import APIRetryWrapper in services with API calls
- [ ] Wrap Telegram API calls with retry wrapper
- [ ] Remove manual retry logic
- [ ] Test retry behavior with temporary failures
- [ ] Verify permanent errors don't retry

### Testing
- [ ] Run unit tests
- [ ] Run integration tests
- [ ] Test in Docker environment
- [ ] Verify logs are structured correctly
- [ ] Test retry logic with simulated failures
- [ ] Check configuration validation

### Deployment
- [ ] Deploy to staging environment
- [ ] Monitor logs for errors
- [ ] Verify all services start correctly
- [ ] Test critical functionality
- [ ] Monitor performance metrics
- [ ] Deploy to production
- [ ] Monitor production logs

### Post-Migration
- [ ] Verify no print() statements remain
- [ ] Verify no hardcoded values remain
- [ ] Check log file sizes and rotation
- [ ] Monitor retry metrics
- [ ] Review error patterns
- [ ] Update documentation

---

**Last Updated:** 2024-01-15  
**Version:** 1.0.0  
**Applies to:** TelegramVault v2.0+
