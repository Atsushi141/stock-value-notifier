"""
Tests for ErrorHandlingConfig module

Tests the configuration management for error handling modes and settings.
"""

import os
import pytest
from unittest.mock import patch

from src.error_handling_config import (
    ErrorHandlingConfig,
    ErrorHandlingConfigManager,
    ErrorHandlingMode,
    AlertLevel,
    create_strict_config,
    create_tolerant_config,
    create_debug_config,
    load_config_from_environment,
)
from src.retry_manager import RetryStrategy


class TestErrorHandlingConfig:
    """Test ErrorHandlingConfig class functionality"""

    def test_default_configuration(self):
        """Test default configuration values"""
        config = ErrorHandlingConfig()

        assert config.mode == ErrorHandlingMode.TOLERANT
        assert config.continue_on_individual_error is True
        assert config.continue_on_batch_error is True
        assert config.max_consecutive_errors == 20  # Tolerant mode default
        assert config.max_error_rate == 0.8  # Tolerant mode default
        assert config.treat_data_not_found_as_warning is True
        assert config.retry_config.max_retries == 5  # Tolerant mode default
        assert config.alert_config.enabled is True

    def test_strict_mode_configuration(self):
        """Test strict mode applies correct settings"""
        config = ErrorHandlingConfig()
        config.mode = ErrorHandlingMode.STRICT
        config._apply_mode_settings()

        assert config.continue_on_batch_error is False
        assert config.max_consecutive_errors == 3
        assert config.max_error_rate == 0.1
        assert config.treat_data_not_found_as_warning is False
        assert config.retry_config.max_retries == 2
        assert config.alert_config.error_threshold == 0.05
        assert config.include_stack_traces is True

    def test_tolerant_mode_configuration(self):
        """Test tolerant mode applies correct settings"""
        config = ErrorHandlingConfig()
        config.mode = ErrorHandlingMode.TOLERANT
        config._apply_mode_settings()

        assert config.continue_on_batch_error is True
        assert config.max_consecutive_errors == 20
        assert config.max_error_rate == 0.8
        assert config.treat_data_not_found_as_warning is True
        assert config.retry_config.max_retries == 5
        assert config.alert_config.error_threshold == 0.3
        assert config.include_stack_traces is False

    def test_debug_mode_configuration(self):
        """Test debug mode applies correct settings"""
        config = ErrorHandlingConfig()
        config.mode = ErrorHandlingMode.DEBUG
        config._apply_mode_settings()

        assert config.continue_on_batch_error is True
        assert config.max_consecutive_errors == 50
        assert config.max_error_rate == 0.95
        assert config.treat_data_not_found_as_warning is True
        assert config.retry_config.max_retries == 3
        assert config.alert_config.error_threshold == 0.9
        assert config.include_stack_traces is True
        assert config.detailed_error_context is True

    def test_configuration_validation(self):
        """Test configuration validation with invalid values"""
        config = ErrorHandlingConfig()

        # Test invalid values that should be corrected
        config.max_consecutive_errors = -1
        config.max_error_rate = 1.5
        config.alert_config.error_threshold = -0.1
        config.retry_config.max_retries = -1

        config._validate_configuration()

        # Should be corrected to valid defaults
        assert config.max_consecutive_errors == 10
        assert config.max_error_rate == 0.5
        assert config.alert_config.error_threshold == 0.1
        assert config.retry_config.max_retries == 3

    def test_to_processing_config(self):
        """Test conversion to ProcessingConfig"""
        config = ErrorHandlingConfig()
        config.mode = ErrorHandlingMode.STRICT
        config._apply_mode_settings()

        processing_config = config.to_processing_config()

        assert (
            processing_config.continue_on_individual_error
            == config.continue_on_individual_error
        )
        assert (
            processing_config.continue_on_batch_error == config.continue_on_batch_error
        )
        assert processing_config.max_consecutive_errors == config.max_consecutive_errors
        assert processing_config.max_error_rate == config.max_error_rate
        assert (
            processing_config.treat_data_not_found_as_warning
            == config.treat_data_not_found_as_warning
        )
        assert processing_config.max_retries_per_item == config.retry_config.max_retries

    def test_configuration_summary(self):
        """Test configuration summary generation"""
        config = ErrorHandlingConfig()
        summary = config.get_configuration_summary()

        assert "mode" in summary
        assert "processing" in summary
        assert "error_treatment" in summary
        assert "retry" in summary
        assert "alerts" in summary
        assert "logging" in summary

        assert summary["mode"] == "tolerant"
        assert summary["processing"]["max_consecutive_errors"] == 20
        assert summary["retry"]["max_retries"] == 5


class TestErrorHandlingConfigManager:
    """Test ErrorHandlingConfigManager functionality"""

    def test_create_config_for_mode(self):
        """Test creating configuration for specific modes"""
        manager = ErrorHandlingConfigManager()

        strict_config = manager.create_config_for_mode(ErrorHandlingMode.STRICT)
        assert strict_config.mode == ErrorHandlingMode.STRICT
        assert strict_config.max_consecutive_errors == 3

        tolerant_config = manager.create_config_for_mode(ErrorHandlingMode.TOLERANT)
        assert tolerant_config.mode == ErrorHandlingMode.TOLERANT
        assert tolerant_config.max_consecutive_errors == 20

        debug_config = manager.create_config_for_mode(ErrorHandlingMode.DEBUG)
        assert debug_config.mode == ErrorHandlingMode.DEBUG
        assert debug_config.max_consecutive_errors == 50

    @patch.dict(
        os.environ,
        {
            "ERROR_HANDLING_MODE": "strict",
            "MAX_CONSECUTIVE_ERRORS": "5",
            "MAX_ERROR_RATE": "0.2",
            "RETRY_MAX_ATTEMPTS": "3",
            "RETRY_BASE_DELAY": "1.5",
            "ALERT_ERROR_THRESHOLD": "0.1",
            "ENABLE_DETAILED_LOGGING": "true",
        },
    )
    def test_load_config_from_env(self):
        """Test loading configuration from environment variables"""
        manager = ErrorHandlingConfigManager()
        config = manager.load_config_from_env()

        assert config.mode == ErrorHandlingMode.STRICT
        assert config.max_consecutive_errors == 5
        assert config.max_error_rate == 0.2
        assert config.retry_config.max_retries == 3
        assert config.retry_config.base_delay == 1.5
        assert config.alert_config.error_threshold == 0.1
        assert config.include_stack_traces is True

    @patch.dict(
        os.environ,
        {
            "ERROR_HANDLING_MODE": "invalid_mode",
            "MAX_CONSECUTIVE_ERRORS": "invalid",
            "MAX_ERROR_RATE": "2.0",
        },
    )
    def test_load_config_from_env_with_invalid_values(self):
        """Test loading configuration with invalid environment values"""
        manager = ErrorHandlingConfigManager()
        config = manager.load_config_from_env()

        # Should fall back to defaults for invalid values
        assert config.mode == ErrorHandlingMode.TOLERANT  # Invalid mode falls back
        # Invalid values should be corrected during validation

    def test_validate_config(self):
        """Test configuration validation"""
        manager = ErrorHandlingConfigManager()

        # Valid configuration
        valid_config = ErrorHandlingConfig()
        assert manager.validate_config(valid_config) is True

        # Invalid configuration
        invalid_config = ErrorHandlingConfig()
        invalid_config.max_consecutive_errors = -1
        invalid_config.max_error_rate = 2.0

        assert manager.validate_config(invalid_config) is False


class TestConvenienceFunctions:
    """Test convenience functions for creating configurations"""

    def test_create_strict_config(self):
        """Test create_strict_config function"""
        config = create_strict_config()
        assert config.mode == ErrorHandlingMode.STRICT
        assert config.max_consecutive_errors == 3
        assert config.continue_on_batch_error is False

    def test_create_tolerant_config(self):
        """Test create_tolerant_config function"""
        config = create_tolerant_config()
        assert config.mode == ErrorHandlingMode.TOLERANT
        assert config.max_consecutive_errors == 20
        assert config.continue_on_batch_error is True

    def test_create_debug_config(self):
        """Test create_debug_config function"""
        config = create_debug_config()
        assert config.mode == ErrorHandlingMode.DEBUG
        assert config.max_consecutive_errors == 50
        assert config.include_stack_traces is True
        assert config.detailed_error_context is True

    @patch.dict(os.environ, {"ERROR_HANDLING_MODE": "debug"})
    def test_load_config_from_environment(self):
        """Test load_config_from_environment function"""
        config = load_config_from_environment()
        assert config.mode == ErrorHandlingMode.DEBUG


if __name__ == "__main__":
    pytest.main([__file__])
