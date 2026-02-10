# TelegramVault Configuration Guide

This document provides comprehensive documentation for all configuration options in TelegramVault. Each configuration is explained with its purpose, type, valid values, defaults, and examples.

## Table of Contents

1. [Media Configuration](#media-configuration)
2. [Search Configuration](#search-configuration)
3. [Detection Configuration](#detection-configuration)
4. [User Enrichment Configuration](#user-enrichment-configuration)
5. [Telegram API Configuration](#telegram-api-configuration)
6. [Logging Configuration](#logging-configuration)
7. [Database Configuration](#database-configuration)
8. [Security Configuration](#security-configuration)
9. [Configuration Loading](#configuration-loading)
10. [Validation and Error Handling](#validation-and-error-handling)

---

## Media Configuration

Configuration options for media file storage and download retry behavior.

### MEDIA_DIR

**Description:** Directory path for storing downloaded media files (photos, videos, documents).

**Type:** `string`

**Default:** `media`

**Required:** Yes

**Valid Range:** Any valid directory path

**Example:**
```bash
MEDIA_DIR=media
MEDIA_DIR=/app/media
MEDIA_DIR=/var/telegramvault/media
```

**Notes:**
- The directory will be created automatically if it doesn't exist
- Ensure the application has write permissions to this directory
- In Docker environments, this should be mounted as a volume for persistence

---

### MEDIA_RETRY_MAX_ATTEMPTS

**Description:** Maximum number of retry attempts for failed media downloads before marking as permanently failed.

**Type:** `integer`

**Default:** `3`

**Required:** No

**Valid Range:** 1-10

**Example:**
```bash
MEDIA_RETRY_MAX_ATTEMPTS=3  # Retry up to 3 times
MEDIA_RETRY_MAX_ATTEMPTS=5  # More aggressive retry
MEDIA_RETRY_MAX_ATTEMPTS=1  # No retries, fail immediately
```

**Notes:**
- Higher values increase resilience but may delay failure detection
- Each retry uses exponential backoff (see MEDIA_RETRY_DELAY_BASE)
- Recommended: 3-5 for production environments

---

### MEDIA_RETRY_DELAY_BASE

**Description:** Base delay in seconds for exponential backoff between retry attempts.

**Type:** `integer`

**Default:** `2`

**Required:** No

**Valid Range:** 1-60

**Example:**
```bash
MEDIA_RETRY_DELAY_BASE=2   # Wait 2s, 4s, 8s, 16s...
MEDIA_RETRY_DELAY_BASE=5   # Wait 5s, 10s, 20s, 40s...
MEDIA_RETRY_DELAY_BASE=1   # Faster retries: 1s, 2s, 4s, 8s...
```

**Formula:** `delay = base * (2 ^ (attempt - 1))`

**Notes:**
- Exponential backoff helps avoid overwhelming the API during temporary issues
- Lower values retry faster but may hit rate limits
- Higher values are more conservative but slower to recover

---

### MEDIA_DOWNLOAD_TIMEOUT

**Description:** Timeout in seconds for individual media download operations.

**Type:** `integer`

**Default:** `30`

**Required:** No

**Valid Range:** 5-300

**Example:**
```bash
MEDIA_DOWNLOAD_TIMEOUT=30   # 30 seconds timeout
MEDIA_DOWNLOAD_TIMEOUT=60   # Longer timeout for large files
MEDIA_DOWNLOAD_TIMEOUT=10   # Faster timeout for small files
```

**Notes:**
- Adjust based on expected file sizes and network speed
- Too low: large files may timeout prematurely
- Too high: slow connections may block the queue
- Recommended: 30-60 seconds for most use cases

---

### MEDIA_VALIDATION_ENABLED

**Description:** Enable validation of downloaded media files to ensure they are not corrupted.

**Type:** `boolean`

**Default:** `true`

**Required:** No

**Valid Values:** `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

**Example:**
```bash
MEDIA_VALIDATION_ENABLED=true   # Validate all downloads
MEDIA_VALIDATION_ENABLED=false  # Skip validation (faster but risky)
```

**Notes:**
- Validation checks file size, format, and integrity
- Disabling improves performance but may store corrupted files
- Recommended: Keep enabled in production

---

## Search Configuration

Configuration options for full-text search functionality.

### SEARCH_FTS_LANGUAGE

**Description:** Language configuration for PostgreSQL full-text search (FTS). Determines stemming, stop words, and text processing rules.

**Type:** `string`

**Default:** `spanish`

**Required:** No

**Valid Pattern:** Lowercase letters only (`^[a-z]+$`)

**Valid Values:** `spanish`, `english`, `french`, `german`, `italian`, `portuguese`, `russian`, etc.

**Example:**
```bash
SEARCH_FTS_LANGUAGE=spanish   # Spanish text processing
SEARCH_FTS_LANGUAGE=english   # English text processing
SEARCH_FTS_LANGUAGE=french    # French text processing
```

**Notes:**
- Must match the language of your content for best results
- Affects stemming (e.g., "running" → "run")
- Affects stop words (common words ignored in search)
- See PostgreSQL documentation for supported languages

---

### SEARCH_FALLBACK_TO_ILIKE

**Description:** Enable fallback to ILIKE pattern matching when full-text search fails or returns no results.

**Type:** `boolean`

**Default:** `true`

**Required:** No

**Valid Values:** `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

**Example:**
```bash
SEARCH_FALLBACK_TO_ILIKE=true   # Use ILIKE as fallback
SEARCH_FALLBACK_TO_ILIKE=false  # Strict FTS only
```

**Notes:**
- ILIKE is slower but more flexible (substring matching)
- FTS is faster but requires proper text indexing
- Recommended: Keep enabled for better user experience
- Disable for performance-critical applications with large datasets

---

### SEARCH_LOG_FAILURES

**Description:** Log full-text search failures with query details and error messages.

**Type:** `boolean`

**Default:** `true`

**Required:** No

**Valid Values:** `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

**Example:**
```bash
SEARCH_LOG_FAILURES=true   # Log all search failures
SEARCH_LOG_FAILURES=false  # Silent failures
```

**Notes:**
- Helps diagnose search issues and query problems
- Logs include query text, error type, and stack trace
- Disable in high-traffic environments to reduce log volume

---

## Detection Configuration

Configuration options for regex pattern detection and matching.

### DETECTION_CACHE_SIZE

**Description:** Maximum number of compiled regex patterns to cache in memory.

**Type:** `integer`

**Default:** `1000`

**Required:** No

**Valid Range:** 10-10000

**Example:**
```bash
DETECTION_CACHE_SIZE=1000   # Cache up to 1000 patterns
DETECTION_CACHE_SIZE=5000   # Larger cache for many patterns
DETECTION_CACHE_SIZE=100    # Smaller cache for memory-constrained systems
```

**Notes:**
- Compiled patterns are expensive to create but fast to use
- LRU (Least Recently Used) eviction policy
- Higher values use more memory but improve performance
- Recommended: 1000-5000 for production

---

### DETECTION_VALIDATE_PATTERNS

**Description:** Validate regex patterns before compilation to catch syntax errors early.

**Type:** `boolean`

**Default:** `true`

**Required:** No

**Valid Values:** `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

**Example:**
```bash
DETECTION_VALIDATE_PATTERNS=true   # Validate before compiling
DETECTION_VALIDATE_PATTERNS=false  # Skip validation (faster)
```

**Notes:**
- Validation prevents runtime errors from invalid patterns
- Adds minimal overhead during pattern loading
- Recommended: Keep enabled to catch configuration errors

---

### DETECTION_LOG_COMPILATION_ERRORS

**Description:** Log regex pattern compilation errors with pattern details and error messages.

**Type:** `boolean`

**Default:** `true`

**Required:** No

**Valid Values:** `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

**Example:**
```bash
DETECTION_LOG_COMPILATION_ERRORS=true   # Log compilation errors
DETECTION_LOG_COMPILATION_ERRORS=false  # Silent errors
```

**Notes:**
- Helps identify and fix invalid regex patterns
- Logs include pattern text and specific error
- Recommended: Keep enabled during development and testing

---

## User Enrichment Configuration

Configuration options for user profile enrichment operations.

### USER_ENRICHMENT_TIMEOUT

**Description:** Timeout in seconds for individual user enrichment API calls.

**Type:** `integer`

**Default:** `30`

**Required:** No

**Valid Range:** 5-120

**Example:**
```bash
USER_ENRICHMENT_TIMEOUT=30   # 30 seconds timeout
USER_ENRICHMENT_TIMEOUT=60   # Longer timeout for slow connections
USER_ENRICHMENT_TIMEOUT=15   # Faster timeout for quick operations
```

**Notes:**
- Applies to profile photo downloads, bio fetching, etc.
- Too low: may timeout on slow connections
- Too high: may block the enrichment queue
- Recommended: 30-60 seconds

---

### USER_ENRICHMENT_MAX_RETRIES

**Description:** Maximum retry attempts for failed user enrichment operations.

**Type:** `integer`

**Default:** `3`

**Required:** No

**Valid Range:** 1-10

**Example:**
```bash
USER_ENRICHMENT_MAX_RETRIES=3   # Retry up to 3 times
USER_ENRICHMENT_MAX_RETRIES=5   # More aggressive retry
USER_ENRICHMENT_MAX_RETRIES=1   # No retries
```

**Notes:**
- Each retry uses exponential backoff
- Higher values increase resilience but may delay failure detection
- Recommended: 3-5 for production

---

### USER_ENRICHMENT_BATCH_SIZE

**Description:** Number of users to process in a single batch during enrichment operations.

**Type:** `integer`

**Default:** `20`

**Required:** No

**Valid Range:** 1-100

**Example:**
```bash
USER_ENRICHMENT_BATCH_SIZE=20   # Process 20 users per batch
USER_ENRICHMENT_BATCH_SIZE=50   # Larger batches (faster but more memory)
USER_ENRICHMENT_BATCH_SIZE=10   # Smaller batches (slower but safer)
```

**Notes:**
- Larger batches are more efficient but use more memory
- Smaller batches are safer for rate-limited APIs
- Recommended: 20-50 for most use cases

---

## Telegram API Configuration

Configuration options for Telegram API client behavior.

### TELEGRAM_API_RETRY_MAX_ATTEMPTS

**Description:** Maximum retry attempts for failed Telegram API calls.

**Type:** `integer`

**Default:** `5`

**Required:** No

**Valid Range:** 1-10

**Example:**
```bash
TELEGRAM_API_RETRY_MAX_ATTEMPTS=5   # Retry up to 5 times
TELEGRAM_API_RETRY_MAX_ATTEMPTS=10  # Very aggressive retry
TELEGRAM_API_RETRY_MAX_ATTEMPTS=3   # Conservative retry
```

**Notes:**
- Applies to all Telegram API operations
- Each retry uses exponential backoff with jitter
- Higher values increase resilience to temporary failures
- Recommended: 5-7 for production

---

### TELEGRAM_API_RETRY_DELAY_BASE

**Description:** Base delay in seconds for exponential backoff between API retry attempts.

**Type:** `integer`

**Default:** `1`

**Required:** No

**Valid Range:** 1-30

**Example:**
```bash
TELEGRAM_API_RETRY_DELAY_BASE=1   # Wait 1s, 2s, 4s, 8s...
TELEGRAM_API_RETRY_DELAY_BASE=2   # Wait 2s, 4s, 8s, 16s...
TELEGRAM_API_RETRY_DELAY_BASE=5   # More conservative backoff
```

**Formula:** `delay = base * (2 ^ (attempt - 1)) + jitter`

**Notes:**
- Lower values retry faster but may hit rate limits
- Higher values are more conservative
- Jitter is added if TELEGRAM_API_RETRY_JITTER is enabled

---

### TELEGRAM_API_RETRY_JITTER

**Description:** Enable random jitter in retry backoff to avoid thundering herd problem.

**Type:** `boolean`

**Default:** `true`

**Required:** No

**Valid Values:** `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`

**Example:**
```bash
TELEGRAM_API_RETRY_JITTER=true   # Add random jitter
TELEGRAM_API_RETRY_JITTER=false  # Deterministic backoff
```

**Notes:**
- Jitter adds randomness to prevent synchronized retries
- Helps distribute load when multiple clients retry simultaneously
- Recommended: Keep enabled in production

---

### TELEGRAM_API_TIMEOUT

**Description:** Timeout in seconds for individual Telegram API calls.

**Type:** `integer`

**Default:** `30`

**Required:** No

**Valid Range:** 5-120

**Example:**
```bash
TELEGRAM_API_TIMEOUT=30   # 30 seconds timeout
TELEGRAM_API_TIMEOUT=60   # Longer timeout for slow operations
TELEGRAM_API_TIMEOUT=15   # Faster timeout
```

**Notes:**
- Applies to all API operations (messages, media, users, etc.)
- Too low: may timeout on slow connections
- Too high: may block the application
- Recommended: 30-60 seconds

---

### TELEGRAM_API_ID

**Description:** Telegram API ID obtained from https://my.telegram.org

**Type:** `integer`

**Default:** `0`

**Required:** **Yes** (Critical)

**Sensitive:** Yes (masked in logs)

**Example:**
```bash
TELEGRAM_API_ID=12345678
```

**How to obtain:**
1. Visit https://my.telegram.org
2. Log in with your phone number
3. Go to "API development tools"
4. Create a new application
5. Copy the "App api_id"

**Notes:**
- This is a unique identifier for your application
- Keep this value secret and never commit to version control
- Each application should have its own API ID

---

### TELEGRAM_API_HASH

**Description:** Telegram API Hash obtained from https://my.telegram.org

**Type:** `string`

**Default:** `""` (empty)

**Required:** **Yes** (Critical)

**Sensitive:** Yes (masked in logs)

**Example:**
```bash
TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
```

**How to obtain:**
1. Visit https://my.telegram.org
2. Log in with your phone number
3. Go to "API development tools"
4. Create a new application
5. Copy the "App api_hash"

**Notes:**
- This is a secret key for your application
- Keep this value secret and never commit to version control
- Must match the API ID

---

## Logging Configuration

Configuration options for application logging behavior.

### LOG_LEVEL

**Description:** Minimum logging level for log messages.

**Type:** `string`

**Default:** `INFO`

**Required:** No

**Valid Values:** `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Example:**
```bash
LOG_LEVEL=INFO      # Standard logging
LOG_LEVEL=DEBUG     # Verbose logging (development)
LOG_LEVEL=WARNING   # Only warnings and errors
LOG_LEVEL=ERROR     # Only errors and critical
```

**Log Levels Explained:**
- **DEBUG:** Detailed diagnostic information (very verbose)
- **INFO:** General informational messages
- **WARNING:** Warning messages (potential issues)
- **ERROR:** Error messages (failures)
- **CRITICAL:** Critical errors (system failures)

**Notes:**
- DEBUG generates large log volumes
- Recommended: INFO for production, DEBUG for development

---

### LOG_FORMAT

**Description:** Format for log output.

**Type:** `string`

**Default:** `json`

**Required:** No

**Valid Values:** `json`, `text`

**Example:**
```bash
LOG_FORMAT=json   # Structured JSON logs
LOG_FORMAT=text   # Human-readable text logs
```

**JSON Format Example:**
```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "component": "MediaRetryService",
  "operation": "retry_download",
  "message": "Retrying failed download",
  "details": {
    "file_path": "media/photos/123/file.jpg",
    "attempt": 2
  }
}
```

**Text Format Example:**
```
2024-01-15 10:30:45,123 INFO MediaRetryService retry_download: Retrying failed download
```

**Notes:**
- JSON is better for log aggregation and analysis
- Text is more human-readable for development
- Recommended: JSON for production

---

### LOG_FILE

**Description:** Path to the main log file.

**Type:** `string`

**Default:** `logs/app.log`

**Required:** No

**Example:**
```bash
LOG_FILE=logs/app.log
LOG_FILE=/var/log/telegramvault/app.log
LOG_FILE=/app/logs/application.log
```

**Notes:**
- Directory will be created automatically if it doesn't exist
- Ensure the application has write permissions
- In Docker, mount as a volume for persistence

---

### LOG_MAX_SIZE_MB

**Description:** Maximum size of a single log file in megabytes before rotation.

**Type:** `integer`

**Default:** `100`

**Required:** No

**Valid Range:** 1-1000

**Example:**
```bash
LOG_MAX_SIZE_MB=100   # Rotate at 100 MB
LOG_MAX_SIZE_MB=50    # Smaller files, more frequent rotation
LOG_MAX_SIZE_MB=500   # Larger files, less frequent rotation
```

**Notes:**
- When limit is reached, log file is rotated
- Old logs are renamed with .1, .2, .3 suffixes
- Helps prevent disk space issues

---

### LOG_BACKUP_COUNT

**Description:** Number of rotated log files to keep.

**Type:** `integer`

**Default:** `5`

**Required:** No

**Valid Range:** 1-50

**Example:**
```bash
LOG_BACKUP_COUNT=5    # Keep 5 backup files
LOG_BACKUP_COUNT=10   # Keep more history
LOG_BACKUP_COUNT=3    # Keep less history (save disk space)
```

**Notes:**
- Oldest backup is deleted when limit is reached
- Total disk usage = LOG_MAX_SIZE_MB * (LOG_BACKUP_COUNT + 1)
- Example: 100 MB * 6 = 600 MB maximum

---

## Database Configuration

Configuration options for database connectivity.

### DATABASE_URL

**Description:** PostgreSQL database connection URL.

**Type:** `string`

**Default:** `""` (empty)

**Required:** **Yes** (Critical)

**Sensitive:** Yes (masked in logs)

**Format:** `postgresql://[user]:[password]@[host]:[port]/[database]`

**Example:**
```bash
DATABASE_URL=postgresql://telegramvault:password@localhost:5432/telegramvault
DATABASE_URL=postgresql://user:pass@db:5432/mydb
DATABASE_URL=postgresql://user:pass@192.168.1.100:5432/vault
```

**Components:**
- **user:** Database username
- **password:** Database password
- **host:** Database server hostname or IP
- **port:** Database server port (default: 5432)
- **database:** Database name

**Notes:**
- Keep this value secret and never commit to version control
- Ensure PostgreSQL server is accessible from the application
- In Docker, use service name as host (e.g., `db`)

---

## Security Configuration

Configuration options for application security.

### SECRET_KEY

**Description:** Secret key used for JWT token signing and other cryptographic operations.

**Type:** `string`

**Default:** `your-secret-key-change-in-production`

**Required:** **Yes** (Critical)

**Sensitive:** Yes (masked in logs)

**Example:**
```bash
SECRET_KEY=your-secret-key-change-in-production
SECRET_KEY=a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
```

**How to generate:**
```bash
# Python
python -c "import secrets; print(secrets.token_urlsafe(32))"

# OpenSSL
openssl rand -base64 32

# Node.js
node -e "console.log(require('crypto').randomBytes(32).toString('base64'))"
```

**Notes:**
- **CRITICAL:** Change the default value in production!
- Use a strong random string (at least 32 characters)
- Keep this value secret and never commit to version control
- Changing this value will invalidate all existing JWT tokens

---

## Configuration Loading

### Loading Order

Configuration values are loaded in the following order (later sources override earlier ones):

1. **Default values** (defined in ConfigManager)
2. **Configuration file** (if specified)
3. **Environment variables** (highest priority)

### Configuration File

You can optionally load configuration from a JSON file:

```python
from backend.app.core.config_manager import ConfigManager

config = ConfigManager(config_file="config.json")
config.load()
```

**config.json example:**
```json
{
  "MEDIA_RETRY_MAX_ATTEMPTS": 5,
  "SEARCH_FTS_LANGUAGE": "english",
  "LOG_LEVEL": "DEBUG"
}
```

### Environment Variables

Environment variables always take precedence:

```bash
# In .env file
MEDIA_RETRY_MAX_ATTEMPTS=5
LOG_LEVEL=DEBUG

# Or export directly
export MEDIA_RETRY_MAX_ATTEMPTS=5
export LOG_LEVEL=DEBUG
```

### Docker Environment

In Docker Compose, use the `environment` section:

```yaml
services:
  app:
    environment:
      - MEDIA_RETRY_MAX_ATTEMPTS=5
      - LOG_LEVEL=DEBUG
    env_file:
      - .env
```

---

## Validation and Error Handling

### Validation Rules

The ConfigManager validates all configuration values on load:

1. **Type Validation:** Ensures values match expected types
2. **Range Validation:** Checks numeric values are within valid ranges
3. **Pattern Validation:** Validates strings against regex patterns
4. **Required Validation:** Ensures required fields are present

### Error Handling

**Critical Errors** (application won't start):
- Missing required configuration (TELEGRAM_API_ID, DATABASE_URL, etc.)
- Invalid required configuration values

**Warnings** (application starts with defaults):
- Invalid optional configuration values
- Values outside recommended ranges
- Pattern mismatches for optional fields

### Validation Example

```python
from backend.app.core.config_manager import ConfigManager

config = ConfigManager()
if not config.load():
    # Critical errors occurred
    errors = config.get_validation_errors()
    for error in errors:
        print(f"ERROR: {error}")
    exit(1)

# Configuration loaded successfully
max_attempts = config.get_int("MEDIA_RETRY_MAX_ATTEMPTS")
```

### Checking Configuration

You can retrieve all configuration values (with sensitive values masked):

```python
all_config = config.get_all(hide_sensitive=True)
print(json.dumps(all_config, indent=2))
```

**Output example:**
```json
{
  "MEDIA_RETRY_MAX_ATTEMPTS": 3,
  "TELEGRAM_API_ID": "***5678",
  "TELEGRAM_API_HASH": "***7890",
  "DATABASE_URL": "***vault",
  "SECRET_KEY": "***tion"
}
```

---

## Best Practices

### Development

1. **Use .env file:** Keep configuration separate from code
2. **Enable DEBUG logging:** Get detailed diagnostic information
3. **Use text log format:** Easier to read during development
4. **Lower timeouts:** Fail fast during testing

```bash
LOG_LEVEL=DEBUG
LOG_FORMAT=text
MEDIA_DOWNLOAD_TIMEOUT=10
TELEGRAM_API_TIMEOUT=15
```

### Production

1. **Use environment variables:** More secure than files
2. **Enable INFO logging:** Balance between detail and volume
3. **Use JSON log format:** Better for log aggregation
4. **Higher timeouts:** More resilient to network issues
5. **Enable all validation:** Catch errors early

```bash
LOG_LEVEL=INFO
LOG_FORMAT=json
MEDIA_DOWNLOAD_TIMEOUT=60
TELEGRAM_API_TIMEOUT=60
MEDIA_VALIDATION_ENABLED=true
DETECTION_VALIDATE_PATTERNS=true
```

### Security

1. **Never commit secrets:** Use .env files (add to .gitignore)
2. **Rotate secrets regularly:** Change API keys and secret keys periodically
3. **Use strong secret keys:** Generate with cryptographic tools
4. **Restrict file permissions:** Ensure .env is readable only by application user

```bash
# Set proper permissions
chmod 600 .env

# Generate strong secret
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### Monitoring

1. **Enable failure logging:** Track search and detection failures
2. **Monitor log file sizes:** Ensure rotation is working
3. **Review validation warnings:** May indicate configuration issues
4. **Track retry metrics:** Identify persistent problems

```bash
SEARCH_LOG_FAILURES=true
DETECTION_LOG_COMPILATION_ERRORS=true
LOG_MAX_SIZE_MB=100
LOG_BACKUP_COUNT=10
```

---

## Troubleshooting

### Application Won't Start

**Symptom:** Application exits immediately with configuration errors

**Solution:**
1. Check for missing required fields:
   - TELEGRAM_API_ID
   - TELEGRAM_API_HASH
   - DATABASE_URL
   - SECRET_KEY

2. Verify .env file exists and is readable
3. Check environment variable syntax (no spaces around =)

### High Memory Usage

**Symptom:** Application uses excessive memory

**Solution:**
1. Reduce DETECTION_CACHE_SIZE
2. Reduce USER_ENRICHMENT_BATCH_SIZE
3. Enable log rotation (LOG_MAX_SIZE_MB)

### Slow Performance

**Symptom:** Operations are slow or timing out

**Solution:**
1. Increase timeout values:
   - MEDIA_DOWNLOAD_TIMEOUT
   - TELEGRAM_API_TIMEOUT
   - USER_ENRICHMENT_TIMEOUT

2. Increase batch sizes:
   - USER_ENRICHMENT_BATCH_SIZE

3. Reduce retry attempts:
   - MEDIA_RETRY_MAX_ATTEMPTS
   - TELEGRAM_API_RETRY_MAX_ATTEMPTS

### Search Not Working

**Symptom:** Full-text search returns no results

**Solution:**
1. Check SEARCH_FTS_LANGUAGE matches your content language
2. Enable SEARCH_FALLBACK_TO_ILIKE for more flexible matching
3. Enable SEARCH_LOG_FAILURES to see error details
4. Verify database triggers are installed (see migration scripts)

### Detection Not Finding Patterns

**Symptom:** Regex detections not working

**Solution:**
1. Enable DETECTION_LOG_COMPILATION_ERRORS to see pattern errors
2. Enable DETECTION_VALIDATE_PATTERNS to catch syntax errors
3. Check pattern syntax in database
4. Increase DETECTION_CACHE_SIZE if many patterns

---

## Support

For additional help:
- Check application logs (LOG_FILE location)
- Review validation errors on startup
- Consult the main README.md
- Check the migration guide (docs/MIGRATION_GUIDE.md)

---

**Last Updated:** 2024-01-15  
**Version:** 1.0.0
