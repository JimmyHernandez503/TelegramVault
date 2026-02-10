"""
Unit tests for ConfigManager.

Tests cover:
- Loading from environment variables
- Default values
- Type validation
- Range validation
- Pattern validation
- Sensitive value masking
- Required field validation
"""

import os
import pytest
import tempfile
import json
from pathlib import Path
from backend.app.core.config_manager import ConfigManager, ConfigValidationError


class TestConfigManager:
    """Test suite for ConfigManager."""
    
    def setup_method(self):
        """Setup test environment."""
        # Save original environment
        self.original_env = os.environ.copy()
        
        # Clear test-related env vars
        test_keys = [
            'MEDIA_DIR', 'MEDIA_RETRY_MAX_ATTEMPTS', 'LOG_LEVEL',
            'TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'DATABASE_URL', 'SECRET_KEY'
        ]
        for key in test_keys:
            if key in os.environ:
                del os.environ[key]
    
    def teardown_method(self):
        """Restore original environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
    
    def test_load_with_defaults(self):
        """Test that ConfigManager loads with default values for non-required fields."""
        # Set required fields
        os.environ['TELEGRAM_API_ID'] = '12345'
        os.environ['TELEGRAM_API_HASH'] = 'test_hash'
        os.environ['DATABASE_URL'] = 'postgresql://test:test@localhost/test'
        
        config = ConfigManager()
        
        # Should load successfully with required fields set
        assert config.load() == True
        assert config.is_loaded() == True
        
        # Check some default values for non-required fields
        assert config.get('MEDIA_DIR') == 'media'
        assert config.get_int('MEDIA_RETRY_MAX_ATTEMPTS') == 3
        assert config.get_bool('MEDIA_VALIDATION_ENABLED') == True
        assert config.get('LOG_LEVEL') == 'INFO'
    
    def test_load_from_environment(self):
        """Test loading configuration from environment variables."""
        # Set environment variables
        os.environ['MEDIA_DIR'] = '/custom/media'
        os.environ['MEDIA_RETRY_MAX_ATTEMPTS'] = '5'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['MEDIA_VALIDATION_ENABLED'] = 'false'
        
        config = ConfigManager()
        config.load()
        
        # Verify values loaded from environment
        assert config.get('MEDIA_DIR') == '/custom/media'
        assert config.get_int('MEDIA_RETRY_MAX_ATTEMPTS') == 5
        assert config.get('LOG_LEVEL') == 'DEBUG'
        assert config.get_bool('MEDIA_VALIDATION_ENABLED') == False
    
    def test_load_from_file(self):
        """Test loading configuration from JSON file."""
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_data = {
                'MEDIA_DIR': '/file/media',
                'MEDIA_RETRY_MAX_ATTEMPTS': 7,
                'LOG_LEVEL': 'WARNING'
            }
            json.dump(config_data, f)
            config_file = f.name
        
        try:
            config = ConfigManager(config_file=config_file)
            config.load()
            
            # Verify values loaded from file
            assert config.get('MEDIA_DIR') == '/file/media'
            assert config.get_int('MEDIA_RETRY_MAX_ATTEMPTS') == 7
            assert config.get('LOG_LEVEL') == 'WARNING'
        finally:
            os.unlink(config_file)
    
    def test_environment_overrides_file(self):
        """Test that environment variables override file values."""
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config_data = {
                'MEDIA_DIR': '/file/media',
                'MEDIA_RETRY_MAX_ATTEMPTS': 7
            }
            json.dump(config_data, f)
            config_file = f.name
        
        try:
            # Set environment variable
            os.environ['MEDIA_DIR'] = '/env/media'
            
            config = ConfigManager(config_file=config_file)
            config.load()
            
            # Environment should override file
            assert config.get('MEDIA_DIR') == '/env/media'
            # File value should be used for non-overridden keys
            assert config.get_int('MEDIA_RETRY_MAX_ATTEMPTS') == 7
        finally:
            os.unlink(config_file)
    
    def test_type_conversion(self):
        """Test type conversion for different value types."""
        os.environ['MEDIA_RETRY_MAX_ATTEMPTS'] = '10'
        os.environ['MEDIA_VALIDATION_ENABLED'] = 'true'
        os.environ['MEDIA_RETRY_DELAY_BASE'] = '5'
        
        config = ConfigManager()
        config.load()
        
        # Test integer conversion
        assert config.get_int('MEDIA_RETRY_MAX_ATTEMPTS') == 10
        assert isinstance(config.get_int('MEDIA_RETRY_MAX_ATTEMPTS'), int)
        
        # Test boolean conversion
        assert config.get_bool('MEDIA_VALIDATION_ENABLED') == True
        assert isinstance(config.get_bool('MEDIA_VALIDATION_ENABLED'), bool)
        
        # Test float conversion
        assert config.get_float('MEDIA_RETRY_DELAY_BASE') == 5.0
        assert isinstance(config.get_float('MEDIA_RETRY_DELAY_BASE'), float)
    
    def test_boolean_conversion_variants(self):
        """Test various boolean string representations."""
        test_cases = [
            ('true', True),
            ('True', True),
            ('TRUE', True),
            ('1', True),
            ('yes', True),
            ('on', True),
            ('false', False),
            ('False', False),
            ('0', False),
            ('no', False),
            ('off', False),
        ]
        
        for str_value, expected in test_cases:
            os.environ['MEDIA_VALIDATION_ENABLED'] = str_value
            config = ConfigManager()
            config.load()
            assert config.get_bool('MEDIA_VALIDATION_ENABLED') == expected, f"Failed for {str_value}"
    
    def test_range_validation(self):
        """Test range validation for numeric values."""
        # Test value below minimum
        os.environ['MEDIA_RETRY_MAX_ATTEMPTS'] = '0'
        config = ConfigManager()
        config.load()
        
        errors = config.get_validation_errors()
        assert any('below minimum' in error for error in errors)
        
        # Test value above maximum
        os.environ['MEDIA_RETRY_MAX_ATTEMPTS'] = '100'
        config = ConfigManager()
        config.load()
        
        errors = config.get_validation_errors()
        assert any('above maximum' in error for error in errors)
    
    def test_pattern_validation(self):
        """Test pattern validation for string values."""
        # Test invalid log level
        os.environ['LOG_LEVEL'] = 'INVALID'
        config = ConfigManager()
        config.load()
        
        errors = config.get_validation_errors()
        assert any('does not match pattern' in error for error in errors)
        
        # Test valid log level
        os.environ['LOG_LEVEL'] = 'DEBUG'
        config = ConfigManager()
        config.load()
        
        errors = config.get_validation_errors()
        # Should not have pattern error for LOG_LEVEL
        assert not any('LOG_LEVEL' in error and 'pattern' in error for error in errors)
    
    def test_required_field_validation(self):
        """Test validation of required fields."""
        # Don't set required fields
        config = ConfigManager()
        result = config.load()
        
        # Should fail because required fields are missing
        assert result == False
        
        errors = config.get_validation_errors()
        # Should have errors for required fields
        assert any('TELEGRAM_API_ID' in error for error in errors)
        assert any('TELEGRAM_API_HASH' in error for error in errors)
        assert any('DATABASE_URL' in error for error in errors)
    
    def test_sensitive_value_masking(self):
        """Test that sensitive values are masked in get_all()."""
        os.environ['TELEGRAM_API_HASH'] = 'secret_hash_12345'
        os.environ['SECRET_KEY'] = 'super_secret_key'
        os.environ['DATABASE_URL'] = 'postgresql://user:pass@localhost/db'
        os.environ['TELEGRAM_API_ID'] = '12345'
        
        config = ConfigManager()
        config.load()
        
        # Get all with masking (default)
        all_config = config.get_all(hide_sensitive=True)
        
        # Sensitive values should be masked
        assert '***' in all_config['TELEGRAM_API_HASH']
        assert 'secret_hash' not in all_config['TELEGRAM_API_HASH']
        assert '***' in all_config['SECRET_KEY']
        assert 'super_secret' not in all_config['SECRET_KEY']
        
        # Get all without masking
        all_config_unmasked = config.get_all(hide_sensitive=False)
        
        # Should have actual values
        assert all_config_unmasked['TELEGRAM_API_HASH'] == 'secret_hash_12345'
        assert all_config_unmasked['SECRET_KEY'] == 'super_secret_key'
    
    def test_get_with_dot_notation(self):
        """Test getting values with dot notation."""
        os.environ['MEDIA_RETRY_MAX_ATTEMPTS'] = '5'
        
        config = ConfigManager()
        config.load()
        
        # Should work with dot notation
        assert config.get('media.retry.max_attempts') == 5
        assert config.get_int('media.retry.max_attempts') == 5
    
    def test_get_path(self):
        """Test getting path values."""
        os.environ['MEDIA_DIR'] = '/custom/media/path'
        
        config = ConfigManager()
        config.load()
        
        path = config.get_path('MEDIA_DIR')
        assert isinstance(path, Path)
        assert str(path) == '/custom/media/path'
    
    def test_reload(self):
        """Test reloading configuration."""
        os.environ['MEDIA_DIR'] = '/initial/path'
        
        config = ConfigManager()
        config.load()
        assert config.get('MEDIA_DIR') == '/initial/path'
        
        # Change environment
        os.environ['MEDIA_DIR'] = '/new/path'
        
        # Reload
        config.reload()
        assert config.get('MEDIA_DIR') == '/new/path'
    
    def test_get_with_default(self):
        """Test getting non-existent keys with default values."""
        config = ConfigManager()
        config.load()
        
        # Non-existent key should return default
        assert config.get('NON_EXISTENT_KEY', 'default_value') == 'default_value'
        assert config.get_int('NON_EXISTENT_INT', 42) == 42
        assert config.get_bool('NON_EXISTENT_BOOL', True) == True
    
    def test_validation_with_all_required_fields(self):
        """Test that validation passes when all required fields are set."""
        # Set all required fields
        os.environ['TELEGRAM_API_ID'] = '12345'
        os.environ['TELEGRAM_API_HASH'] = 'test_hash'
        os.environ['DATABASE_URL'] = 'postgresql://localhost/test'
        os.environ['SECRET_KEY'] = 'test_secret_key'
        
        config = ConfigManager()
        result = config.load()
        
        # Should succeed
        assert result == True
        errors = config.get_validation_errors()
        # Should not have critical errors
        assert not any('required' in error.lower() for error in errors)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
