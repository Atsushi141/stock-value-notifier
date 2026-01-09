"""
Error Handling Configuration module for configurable error processing modes

This module provides comprehensive configuration for error handling behavior:
- Strict mode/tolerant mode/debug mode settings
- Retry settings and error threshold configuration
- Default values and fallback processing
- Integration with existing error handling components
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Type
from enum import Enum

from .retry_manager import RetryConfig, RetryStrategy
from .error_handler import ProcessingConfig


class ErrorHandlingMode(Enum):
    """Error handling operation modes"""

    STRICT = "strict"  # Stop on minor errors
    TOLERANT = "tolerant"  # Continue processing as much as possible
    DEBUG = "debug"  # Detailed logging and continue processing


class AlertLevel(Enum):
    """Alert severity levels"""

    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AlertConfig:
    """Configuration for alert notifications"""

    enabled: bool = True
    error_threshold: float = 0.1  # 10% error rate triggers alert
    consecutive_error_threshold: int = 5
    alert_level: AlertLevel = AlertLevel.MEDIUM

    # Alert channels
    slack_alerts: bool = True
    log_alerts: bool = True

    # Alert frequency control
    min_alert_interval_minutes: int = 30  # Minimum time between alerts
    max_alerts_per_hour: int = 10


@dataclass
class ErrorHandlingConfig:
    """
    Comprehensive configuration for error handling behavior

    Implements requirements 7.1, 7.5 for configurable error processing
    """

    # Operation mode
    mode: ErrorHandlingMode = ErrorHandlingMode.TOLERANT

    # Processing continuation settings
    continue_on_individual_error: bool = True
    continue_on_batch_error: bool = True
    max_consecutive_errors: int = 10
    max_error_rate: float = 0.5  # Stop if error rate exceeds 50%

    # Error classification settings
    treat_data_not_found_as_warning: bool = True
    treat_rate_limit_as_retryable: bool = True
    treat_network_errors_as_retryable: bool = True
    treat_timezone_errors_as_recoverable: bool = True
    treat_validation_errors_as_skippable: bool = True

    # Retry configuration
    retry_config: RetryConfig = field(default_factory=RetryConfig)

    # Alert configuration
    alert_config: AlertConfig = field(default_factory=AlertConfig)

    # Logging settings
    log_all_errors: bool = True
    log_skipped_items: bool = True
    log_processing_summary: bool = True
    include_stack_traces: bool = False
    detailed_error_context: bool = False

    # Performance settings
    enable_error_metrics: bool = True
    enable_performance_tracking: bool = True
    cache_validation_results: bool = True

    # Fallback behavior
    use_fallback_on_config_error: bool = True
    fallback_to_tolerant_mode: bool = True

    def __post_init__(self):
        """Apply mode-specific configurations after initialization"""
        self._apply_mode_settings()
        self._validate_configuration()

    def _apply_mode_settings(self) -> None:
        """Apply settings based on the selected mode"""
        if self.mode == ErrorHandlingMode.STRICT:
            self._apply_strict_mode()
        elif self.mode == ErrorHandlingMode.TOLERANT:
            self._apply_tolerant_mode()
        elif self.mode == ErrorHandlingMode.DEBUG:
            self._apply_debug_mode()

    def _apply_strict_mode(self) -> None:
        """Apply strict mode settings - stop on minor errors"""
        self.continue_on_individual_error = (
            True  # Continue per item but be strict about errors
        )
        self.continue_on_batch_error = False  # Stop batch on errors
        self.max_consecutive_errors = 3  # Low tolerance for consecutive errors
        self.max_error_rate = 0.1  # Stop at 10% error rate

        # Treat more errors as critical
        self.treat_data_not_found_as_warning = False
        self.treat_timezone_errors_as_recoverable = False
        self.treat_validation_errors_as_skippable = False

        # Conservative retry settings
        self.retry_config.max_retries = 2
        self.retry_config.base_delay = 2.0

        # Sensitive alerting
        self.alert_config.error_threshold = 0.05  # 5% error rate
        self.alert_config.consecutive_error_threshold = 2
        self.alert_config.alert_level = AlertLevel.HIGH

        # Detailed logging for analysis
        self.log_all_errors = True
        self.log_skipped_items = True
        self.include_stack_traces = True
        self.detailed_error_context = True

    def _apply_tolerant_mode(self) -> None:
        """Apply tolerant mode settings - continue processing as much as possible"""
        self.continue_on_individual_error = True
        self.continue_on_batch_error = True
        self.max_consecutive_errors = 20
        self.max_error_rate = 0.8  # Very high tolerance

        # Treat errors as recoverable when possible
        self.treat_data_not_found_as_warning = True
        self.treat_rate_limit_as_retryable = True
        self.treat_network_errors_as_retryable = True
        self.treat_timezone_errors_as_recoverable = True
        self.treat_validation_errors_as_skippable = True

        # Aggressive retry settings
        self.retry_config.max_retries = 5
        self.retry_config.base_delay = 1.0
        self.retry_config.strategy = RetryStrategy.EXPONENTIAL_BACKOFF

        # Moderate alerting
        self.alert_config.error_threshold = 0.3  # 30% error rate
        self.alert_config.consecutive_error_threshold = 10
        self.alert_config.alert_level = AlertLevel.MEDIUM

        # Standard logging
        self.log_all_errors = True
        self.log_skipped_items = False  # Reduce noise
        self.include_stack_traces = False
        self.detailed_error_context = False

    def _apply_debug_mode(self) -> None:
        """Apply debug mode settings - detailed logging and continue processing"""
        self.continue_on_individual_error = True
        self.continue_on_batch_error = True
        self.max_consecutive_errors = 50  # Very high for debugging
        self.max_error_rate = 0.95  # Almost never stop

        # Treat most errors as recoverable for debugging
        self.treat_data_not_found_as_warning = True
        self.treat_rate_limit_as_retryable = True
        self.treat_network_errors_as_retryable = True
        self.treat_timezone_errors_as_recoverable = True
        self.treat_validation_errors_as_skippable = True

        # Conservative retry for debugging
        self.retry_config.max_retries = 3
        self.retry_config.base_delay = 0.5
        self.retry_config.log_retries = True
        self.retry_config.log_failures = True

        # Minimal alerting (focus on logging)
        self.alert_config.error_threshold = 0.9  # 90% error rate
        self.alert_config.consecutive_error_threshold = 25
        self.alert_config.alert_level = AlertLevel.LOW

        # Maximum logging detail
        self.log_all_errors = True
        self.log_skipped_items = True
        self.log_processing_summary = True
        self.include_stack_traces = True
        self.detailed_error_context = True

        # Enable all tracking for debugging
        self.enable_error_metrics = True
        self.enable_performance_tracking = True

    def _validate_configuration(self) -> None:
        """Validate configuration values and apply fallbacks if needed"""
        logger = logging.getLogger(__name__)

        # Validate numeric ranges
        if self.max_consecutive_errors <= 0:
            logger.warning(
                f"Invalid max_consecutive_errors: {self.max_consecutive_errors}, using default: 10"
            )
            self.max_consecutive_errors = 10

        if not 0.0 <= self.max_error_rate <= 1.0:
            logger.warning(
                f"Invalid max_error_rate: {self.max_error_rate}, using default: 0.5"
            )
            self.max_error_rate = 0.5

        if not 0.0 <= self.alert_config.error_threshold <= 1.0:
            logger.warning(
                f"Invalid alert error_threshold: {self.alert_config.error_threshold}, using default: 0.1"
            )
            self.alert_config.error_threshold = 0.1

        if self.alert_config.consecutive_error_threshold <= 0:
            logger.warning(
                f"Invalid consecutive_error_threshold: {self.alert_config.consecutive_error_threshold}, using default: 5"
            )
            self.alert_config.consecutive_error_threshold = 5

        # Validate retry config
        if self.retry_config.max_retries < 0:
            logger.warning(
                f"Invalid max_retries: {self.retry_config.max_retries}, using default: 3"
            )
            self.retry_config.max_retries = 3

        if self.retry_config.base_delay < 0:
            logger.warning(
                f"Invalid base_delay: {self.retry_config.base_delay}, using default: 1.0"
            )
            self.retry_config.base_delay = 1.0

    def to_processing_config(self) -> ProcessingConfig:
        """
        Convert to ProcessingConfig for compatibility with existing error handler

        Returns:
            ProcessingConfig compatible with EnhancedErrorHandler
        """
        return ProcessingConfig(
            continue_on_individual_error=self.continue_on_individual_error,
            continue_on_batch_error=self.continue_on_batch_error,
            max_consecutive_errors=self.max_consecutive_errors,
            max_error_rate=self.max_error_rate,
            treat_data_not_found_as_warning=self.treat_data_not_found_as_warning,
            treat_rate_limit_as_retryable=self.treat_rate_limit_as_retryable,
            treat_network_errors_as_retryable=self.treat_network_errors_as_retryable,
            enable_retries=True,
            max_retries_per_item=self.retry_config.max_retries,
            retry_delay=self.retry_config.base_delay,
            log_all_errors=self.log_all_errors,
            log_skipped_items=self.log_skipped_items,
            log_processing_summary=self.log_processing_summary,
            include_stack_traces=self.include_stack_traces,
        )

    def get_configuration_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration settings"""
        return {
            "mode": self.mode.value,
            "processing": {
                "continue_on_individual_error": self.continue_on_individual_error,
                "continue_on_batch_error": self.continue_on_batch_error,
                "max_consecutive_errors": self.max_consecutive_errors,
                "max_error_rate": self.max_error_rate,
            },
            "error_treatment": {
                "data_not_found_as_warning": self.treat_data_not_found_as_warning,
                "rate_limit_as_retryable": self.treat_rate_limit_as_retryable,
                "network_errors_as_retryable": self.treat_network_errors_as_retryable,
                "timezone_errors_as_recoverable": self.treat_timezone_errors_as_recoverable,
                "validation_errors_as_skippable": self.treat_validation_errors_as_skippable,
            },
            "retry": {
                "max_retries": self.retry_config.max_retries,
                "base_delay": self.retry_config.base_delay,
                "max_delay": self.retry_config.max_delay,
                "strategy": self.retry_config.strategy.value,
            },
            "alerts": {
                "enabled": self.alert_config.enabled,
                "error_threshold": self.alert_config.error_threshold,
                "consecutive_error_threshold": self.alert_config.consecutive_error_threshold,
                "alert_level": self.alert_config.alert_level.value,
            },
            "logging": {
                "log_all_errors": self.log_all_errors,
                "log_skipped_items": self.log_skipped_items,
                "include_stack_traces": self.include_stack_traces,
                "detailed_error_context": self.detailed_error_context,
            },
        }


class ErrorHandlingConfigManager:
    """
    Manager for loading and validating error handling configuration

    Implements requirements 7.1, 7.5 for configuration management
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def load_config_from_env(self) -> ErrorHandlingConfig:
        """
        Load error handling configuration from environment variables

        Returns:
            ErrorHandlingConfig with values from environment or defaults

        Environment Variables:
            ERROR_HANDLING_MODE: strict, tolerant, or debug
            MAX_CONSECUTIVE_ERRORS: Maximum consecutive errors before stopping
            MAX_ERROR_RATE: Maximum error rate (0.0-1.0) before stopping
            RETRY_MAX_ATTEMPTS: Maximum retry attempts
            RETRY_BASE_DELAY: Base delay between retries (seconds)
            ALERT_ERROR_THRESHOLD: Error rate threshold for alerts
            ENABLE_DETAILED_LOGGING: Enable detailed error logging (true/false)
        """
        config = ErrorHandlingConfig()

        try:
            # Load operation mode first
            mode_str = os.getenv("ERROR_HANDLING_MODE", "tolerant").lower()
            if mode_str in ["strict", "tolerant", "debug"]:
                config.mode = ErrorHandlingMode(mode_str)
                self.logger.info(f"Error handling mode set to: {mode_str}")
            else:
                self.logger.warning(
                    f"Invalid ERROR_HANDLING_MODE: {mode_str}, using default: tolerant"
                )
                config.mode = ErrorHandlingMode.TOLERANT

            # Apply mode-specific settings first
            config._apply_mode_settings()

            # Then override with environment variables (environment takes precedence)
            config.max_consecutive_errors = self._load_int_env(
                "MAX_CONSECUTIVE_ERRORS",
                config.max_consecutive_errors,
                min_value=1,
                max_value=100,
            )

            config.max_error_rate = self._load_float_env(
                "MAX_ERROR_RATE", config.max_error_rate, min_value=0.0, max_value=1.0
            )

            # Load retry settings
            config.retry_config.max_retries = self._load_int_env(
                "RETRY_MAX_ATTEMPTS",
                config.retry_config.max_retries,
                min_value=0,
                max_value=10,
            )

            config.retry_config.base_delay = self._load_float_env(
                "RETRY_BASE_DELAY",
                config.retry_config.base_delay,
                min_value=0.1,
                max_value=60.0,
            )

            # Load alert settings
            config.alert_config.error_threshold = self._load_float_env(
                "ALERT_ERROR_THRESHOLD",
                config.alert_config.error_threshold,
                min_value=0.0,
                max_value=1.0,
            )

            # Load logging settings
            config.include_stack_traces = self._load_bool_env(
                "ENABLE_DETAILED_LOGGING", config.include_stack_traces
            )

            config.detailed_error_context = self._load_bool_env(
                "ENABLE_ERROR_CONTEXT", config.detailed_error_context
            )

            # Load feature flags
            config.enable_error_metrics = self._load_bool_env(
                "ENABLE_ERROR_METRICS", config.enable_error_metrics
            )

            config.cache_validation_results = self._load_bool_env(
                "CACHE_VALIDATION_RESULTS", config.cache_validation_results
            )

            self.logger.info("Error handling configuration loaded from environment")
            return config

        except Exception as e:
            self.logger.error(f"Error loading configuration from environment: {e}")
            if config.use_fallback_on_config_error:
                self.logger.info("Using fallback configuration")
                return self._create_fallback_config()
            else:
                raise

    def _load_int_env(
        self, env_var: str, default: int, min_value: int = None, max_value: int = None
    ) -> int:
        """Load and validate integer environment variable"""
        try:
            value = int(os.getenv(env_var, str(default)))

            if min_value is not None and value < min_value:
                self.logger.warning(
                    f"{env_var} value {value} below minimum {min_value}, using minimum"
                )
                return min_value

            if max_value is not None and value > max_value:
                self.logger.warning(
                    f"{env_var} value {value} above maximum {max_value}, using maximum"
                )
                return max_value

            return value

        except (ValueError, TypeError):
            self.logger.warning(f"Invalid {env_var} format, using default: {default}")
            return default

    def _load_float_env(
        self,
        env_var: str,
        default: float,
        min_value: float = None,
        max_value: float = None,
    ) -> float:
        """Load and validate float environment variable"""
        try:
            value = float(os.getenv(env_var, str(default)))

            if min_value is not None and value < min_value:
                self.logger.warning(
                    f"{env_var} value {value} below minimum {min_value}, using minimum"
                )
                return min_value

            if max_value is not None and value > max_value:
                self.logger.warning(
                    f"{env_var} value {value} above maximum {max_value}, using maximum"
                )
                return max_value

            return value

        except (ValueError, TypeError):
            self.logger.warning(f"Invalid {env_var} format, using default: {default}")
            return default

    def _load_bool_env(self, env_var: str, default: bool) -> bool:
        """Load and validate boolean environment variable"""
        value = os.getenv(env_var, str(default)).lower()

        if value in ["true", "1", "yes", "on", "enabled"]:
            return True
        elif value in ["false", "0", "no", "off", "disabled"]:
            return False
        else:
            self.logger.warning(
                f"Invalid {env_var} format: {value}, using default: {default}"
            )
            return default

    def _create_fallback_config(self) -> ErrorHandlingConfig:
        """Create a safe fallback configuration"""
        config = ErrorHandlingConfig()
        config.mode = ErrorHandlingMode.TOLERANT
        config._apply_mode_settings()

        self.logger.info("Created fallback error handling configuration")
        return config

    def validate_config(self, config: ErrorHandlingConfig) -> bool:
        """
        Validate error handling configuration

        Args:
            config: Configuration to validate

        Returns:
            True if configuration is valid
        """
        try:
            # Validate mode
            if not isinstance(config.mode, ErrorHandlingMode):
                self.logger.error(f"Invalid error handling mode: {config.mode}")
                return False

            # Validate numeric ranges
            if config.max_consecutive_errors <= 0:
                self.logger.error(
                    f"Invalid max_consecutive_errors: {config.max_consecutive_errors}"
                )
                return False

            if not 0.0 <= config.max_error_rate <= 1.0:
                self.logger.error(f"Invalid max_error_rate: {config.max_error_rate}")
                return False

            if config.retry_config.max_retries < 0:
                self.logger.error(
                    f"Invalid max_retries: {config.retry_config.max_retries}"
                )
                return False

            if config.retry_config.base_delay < 0:
                self.logger.error(
                    f"Invalid base_delay: {config.retry_config.base_delay}"
                )
                return False

            if not 0.0 <= config.alert_config.error_threshold <= 1.0:
                self.logger.error(
                    f"Invalid alert error_threshold: {config.alert_config.error_threshold}"
                )
                return False

            return True

        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False

    def create_config_for_mode(self, mode: ErrorHandlingMode) -> ErrorHandlingConfig:
        """
        Create configuration for a specific mode

        Args:
            mode: Error handling mode

        Returns:
            ErrorHandlingConfig configured for the specified mode
        """
        config = ErrorHandlingConfig()
        config.mode = mode
        config._apply_mode_settings()

        self.logger.info(f"Created error handling configuration for mode: {mode.value}")
        return config


# Convenience functions for common configurations


def create_strict_config() -> ErrorHandlingConfig:
    """Create strict error handling configuration"""
    return ErrorHandlingConfigManager().create_config_for_mode(ErrorHandlingMode.STRICT)


def create_tolerant_config() -> ErrorHandlingConfig:
    """Create tolerant error handling configuration"""
    return ErrorHandlingConfigManager().create_config_for_mode(
        ErrorHandlingMode.TOLERANT
    )


def create_debug_config() -> ErrorHandlingConfig:
    """Create debug error handling configuration"""
    return ErrorHandlingConfigManager().create_config_for_mode(ErrorHandlingMode.DEBUG)


def load_config_from_environment() -> ErrorHandlingConfig:
    """Load error handling configuration from environment variables"""
    return ErrorHandlingConfigManager().load_config_from_env()
