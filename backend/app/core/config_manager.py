"""
Centralized Configuration Management System for TelegramVault.

This module provides a robust configuration management system that:
- Loads configurations from environment variables and files
- Validates types, ranges, and formats
- Provides reasonable defaults for all configurations
- Hides sensitive values in logs
- Fails fast if required configurations are missing or invalid
"""

import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Type, Union
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
import re


class ConfigValidationError(Exception):
    """Exception raised when configuration validation fails."""
    pass


@dataclass
class ConfigurationSchema:
    """Schema definition for a configuration value."""
    key: str
    description: str
    type: Type
    default: Any
    required: bool = False
    validator: Optional[Callable[[Any], bool]] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    pattern: Optional[str] = None  # Regex pattern for string validation
    sensitive: bool = False  # Whether to hide value in logs


class ConfigManager:
    """
    Centralized configuration manager for TelegramVault.
    
    Features:
    - Load from environment variables and config files
    - Type validation and conversion
    - Range validation for numeric values
    - Pattern validation for strings
    - Default values for all configurations
    - Sensitive value masking in logs
    - Fast failure on missing required configs
    
    Example:
        config = ConfigManager()
        if not config.load():
            raise Exception("Configuration validation failed")
        
        max_attempts = config.get_int("media.retry.max_attempts")
        media_dir = config.get_path("media.dir")
    """
    
    # Configuration schemas with defaults and validation rules
    SCHEMAS = [
        # Media Configuration
        ConfigurationSchema(
            key="MEDIA_DIR",
            description="Directory for storing media files",
            type=str,
            default="media",
            required=True
        ),
        ConfigurationSchema(
            key="MEDIA_RETRY_MAX_ATTEMPTS",
            description="Maximum number of retry attempts for media downloads",
            type=int,
            default=3,
            min_value=1,
            max_value=10
        ),
        ConfigurationSchema(
            key="MEDIA_RETRY_DELAY_BASE",
            description="Base delay in seconds for exponential backoff",
            type=int,
            default=2,
            min_value=1,
            max_value=60
        ),
        ConfigurationSchema(
            key="MEDIA_DOWNLOAD_TIMEOUT",
            description="Timeout in seconds for media downloads",
            type=int,
            default=30,
            min_value=5,
            max_value=300
        ),
        ConfigurationSchema(
            key="MEDIA_VALIDATION_ENABLED",
            description="Enable validation of downloaded media files",
            type=bool,
            default=True
        ),
        
        # Search Configuration
        ConfigurationSchema(
            key="SEARCH_FTS_LANGUAGE",
            description="Language configuration for full-text search",
            type=str,
            default="spanish",
            pattern=r"^[a-z]+$"
        ),
        ConfigurationSchema(
            key="SEARCH_FALLBACK_TO_ILIKE",
            description="Enable fallback to ILIKE when FTS fails",
            type=bool,
            default=True
        ),
        ConfigurationSchema(
            key="SEARCH_LOG_FAILURES",
            description="Log full-text search failures",
            type=bool,
            default=True
        ),
        
        # Detection Configuration
        ConfigurationSchema(
            key="DETECTION_CACHE_SIZE",
            description="Maximum size of regex pattern cache",
            type=int,
            default=1000,
            min_value=10,
            max_value=10000
        ),
        ConfigurationSchema(
            key="DETECTION_VALIDATE_PATTERNS",
            description="Validate regex patterns before compilation",
            type=bool,
            default=True
        ),
        ConfigurationSchema(
            key="DETECTION_LOG_COMPILATION_ERRORS",
            description="Log regex compilation errors",
            type=bool,
            default=True
        ),
        
        # User Enrichment Configuration
        ConfigurationSchema(
            key="USER_ENRICHMENT_TIMEOUT",
            description="Timeout in seconds for user enrichment API calls",
            type=int,
            default=30,
            min_value=5,
            max_value=120
        ),
        ConfigurationSchema(
            key="USER_ENRICHMENT_MAX_RETRIES",
            description="Maximum retry attempts for user enrichment",
            type=int,
            default=3,
            min_value=1,
            max_value=10
        ),
        ConfigurationSchema(
            key="USER_ENRICHMENT_BATCH_SIZE",
            description="Batch size for user enrichment operations",
            type=int,
            default=20,
            min_value=1,
            max_value=100
        ),
        
        # Telegram API Configuration
        ConfigurationSchema(
            key="TELEGRAM_API_RETRY_MAX_ATTEMPTS",
            description="Maximum retry attempts for Telegram API calls",
            type=int,
            default=5,
            min_value=1,
            max_value=10
        ),
        ConfigurationSchema(
            key="TELEGRAM_API_RETRY_DELAY_BASE",
            description="Base delay in seconds for API retry backoff",
            type=int,
            default=1,
            min_value=1,
            max_value=30
        ),
        ConfigurationSchema(
            key="TELEGRAM_API_RETRY_JITTER",
            description="Enable jitter in retry backoff",
            type=bool,
            default=True
        ),
        ConfigurationSchema(
            key="TELEGRAM_API_TIMEOUT",
            description="Timeout in seconds for Telegram API calls",
            type=int,
            default=30,
            min_value=5,
            max_value=120
        ),
        ConfigurationSchema(
            key="TELEGRAM_API_ID",
            description="Telegram API ID",
            type=int,
            default=0,
            required=True,
            sensitive=True
        ),
        ConfigurationSchema(
            key="TELEGRAM_API_HASH",
            description="Telegram API Hash",
            type=str,
            default="",
            required=True,
            sensitive=True
        ),
        
        # Logging Configuration
        ConfigurationSchema(
            key="LOG_LEVEL",
            description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
            type=str,
            default="INFO",
            pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$"
        ),
        ConfigurationSchema(
            key="LOG_FORMAT",
            description="Log format (json or text)",
            type=str,
            default="json",
            pattern=r"^(json|text)$"
        ),
        ConfigurationSchema(
            key="LOG_FILE",
            description="Path to log file",
            type=str,
            default="logs/app.log"
        ),
        ConfigurationSchema(
            key="LOG_MAX_SIZE_MB",
            description="Maximum log file size in MB",
            type=int,
            default=100,
            min_value=1,
            max_value=1000
        ),
        ConfigurationSchema(
            key="LOG_BACKUP_COUNT",
            description="Number of log backup files to keep",
            type=int,
            default=5,
            min_value=1,
            max_value=50
        ),
        
        # Database Configuration
        ConfigurationSchema(
            key="DATABASE_URL",
            description="Database connection URL",
            type=str,
            default="",
            required=True,
            sensitive=True
        ),
        
        # Security Configuration
        ConfigurationSchema(
            key="SECRET_KEY",
            description="Secret key for JWT tokens",
            type=str,
            default="your-secret-key-change-in-production",
            required=True,
            sensitive=True
        ),
    ]
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Optional path to a JSON configuration file
        """
        self.config_file = config_file
        self._config: Dict[str, Any] = {}
        self._schema_map: Dict[str, ConfigurationSchema] = {
            schema.key: schema for schema in self.SCHEMAS
        }
        self._loaded = False
        self._validation_errors: List[str] = []
    
    def load(self) -> bool:
        """
        Load and validate all configurations.
        
        Returns:
            bool: True if load successful, False if there are critical errors
        """
        try:
            # Load from environment variables
            self._load_from_env()
            
            # Load from config file if provided
            if self.config_file:
                self._load_from_file()
            
            # Apply defaults for missing values
            self._apply_defaults()
            
            # Validate all configurations
            validation_errors = self.validate()
            
            if validation_errors:
                self._validation_errors = validation_errors
                # Check if any required configs are missing
                has_critical_errors = any(
                    "required" in error.lower() for error in validation_errors
                )
                
                if has_critical_errors:
                    print("CRITICAL CONFIGURATION ERRORS:")
                    for error in validation_errors:
                        print(f"  - {error}")
                    return False
                else:
                    print("CONFIGURATION WARNINGS:")
                    for error in validation_errors:
                        print(f"  - {error}")
            
            self._loaded = True
            return True
            
        except Exception as e:
            print(f"Failed to load configuration: {e}")
            return False
    
    def _load_from_env(self) -> None:
        """Load configuration values from environment variables."""
        for schema in self.SCHEMAS:
            env_value = os.environ.get(schema.key)
            if env_value is not None:
                try:
                    # Convert to appropriate type
                    converted_value = self._convert_type(env_value, schema.type)
                    self._config[schema.key] = converted_value
                except Exception as e:
                    print(f"Warning: Failed to convert {schema.key}={env_value} to {schema.type.__name__}: {e}")
    
    def _load_from_file(self) -> None:
        """Load configuration values from a JSON file."""
        try:
            if not self.config_file:
                return
            
            config_path = Path(self.config_file)
            if not config_path.exists():
                print(f"Warning: Config file {self.config_file} not found")
                return
            
            with open(config_path, 'r') as f:
                file_config = json.load(f)
            
            # Merge file config with existing config (env vars take precedence)
            for key, value in file_config.items():
                if key not in self._config and key in self._schema_map:
                    schema = self._schema_map[key]
                    try:
                        converted_value = self._convert_type(value, schema.type)
                        self._config[key] = converted_value
                    except Exception as e:
                        print(f"Warning: Failed to convert {key}={value} from file: {e}")
                        
        except Exception as e:
            print(f"Warning: Failed to load config file {self.config_file}: {e}")
    
    def _apply_defaults(self) -> None:
        """Apply default values for missing configurations."""
        for schema in self.SCHEMAS:
            if schema.key not in self._config:
                self._config[schema.key] = schema.default
    
    def _convert_type(self, value: Any, target_type: Type) -> Any:
        """
        Convert a value to the target type.
        
        Args:
            value: Value to convert
            target_type: Target type
            
        Returns:
            Converted value
        """
        if target_type == bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        elif target_type == int:
            return int(value)
        elif target_type == float:
            return float(value)
        elif target_type == str:
            return str(value)
        else:
            return value
    
    def validate(self) -> List[str]:
        """
        Validate all loaded configurations.
        
        Returns:
            List of validation error messages (empty if all valid)
        """
        errors = []
        
        for schema in self.SCHEMAS:
            key = schema.key
            value = self._config.get(key)
            
            # Check required fields
            if schema.required and (value is None or value == schema.default and schema.default in ("", 0)):
                errors.append(f"Required configuration '{key}' is missing or has default value")
                continue
            
            if value is None:
                continue
            
            # Type validation
            if not isinstance(value, schema.type):
                errors.append(f"Configuration '{key}' has invalid type: expected {schema.type.__name__}, got {type(value).__name__}")
                continue
            
            # Range validation for numeric types
            if schema.type in (int, float):
                if schema.min_value is not None and value < schema.min_value:
                    errors.append(f"Configuration '{key}' value {value} is below minimum {schema.min_value}")
                if schema.max_value is not None and value > schema.max_value:
                    errors.append(f"Configuration '{key}' value {value} is above maximum {schema.max_value}")
            
            # Pattern validation for strings
            if schema.type == str and schema.pattern:
                if not re.match(schema.pattern, value):
                    errors.append(f"Configuration '{key}' value '{value}' does not match pattern {schema.pattern}")
            
            # Custom validator
            if schema.validator:
                try:
                    if not schema.validator(value):
                        errors.append(f"Configuration '{key}' failed custom validation")
                except Exception as e:
                    errors.append(f"Configuration '{key}' validation error: {e}")
        
        return errors
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key (can use dot notation like "media.retry.max_attempts"
                 or environment variable style like "MEDIA_RETRY_MAX_ATTEMPTS")
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        # Convert dot notation to env var style
        if '.' in key:
            key = key.upper().replace('.', '_')
        
        return self._config.get(key, default)
    
    def get_int(self, key: str, default: int = 0) -> int:
        """
        Get an integer configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Integer value
        """
        value = self.get(key, default)
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def get_float(self, key: str, default: float = 0.0) -> float:
        """
        Get a float configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Float value
        """
        value = self.get(key, default)
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """
        Get a boolean configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Boolean value
        """
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)
    
    def get_path(self, key: str, default: str = "") -> Path:
        """
        Get a path configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Path object
        """
        value = self.get(key, default)
        return Path(value)
    
    def get_all(self, hide_sensitive: bool = True) -> Dict[str, Any]:
        """
        Get all configuration values.
        
        Args:
            hide_sensitive: Whether to mask sensitive values
            
        Returns:
            Dictionary of all configuration values
        """
        if not hide_sensitive:
            return self._config.copy()
        
        # Mask sensitive values
        masked_config = {}
        for key, value in self._config.items():
            schema = self._schema_map.get(key)
            if schema and schema.sensitive:
                # Mask sensitive values
                if isinstance(value, str) and value:
                    masked_config[key] = "***" + value[-4:] if len(value) > 4 else "***"
                else:
                    masked_config[key] = "***"
            else:
                masked_config[key] = value
        
        return masked_config
    
    def is_loaded(self) -> bool:
        """Check if configuration has been loaded."""
        return self._loaded
    
    def get_validation_errors(self) -> List[str]:
        """Get validation errors from last load attempt."""
        return self._validation_errors.copy()
    
    def reload(self) -> bool:
        """
        Reload configuration from sources.
        
        Returns:
            bool: True if reload successful
        """
        self._config.clear()
        self._validation_errors.clear()
        self._loaded = False
        return self.load()


# Global singleton instance
@lru_cache()
def get_config_manager() -> ConfigManager:
    """
    Get the global ConfigManager instance.
    
    Returns:
        ConfigManager: Global configuration manager
    """
    config = ConfigManager()
    config.load()
    return config


# Convenience alias
config_manager = get_config_manager()
