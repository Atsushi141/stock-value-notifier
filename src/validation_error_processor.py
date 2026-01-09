"""
Validation error processing module for stock value notifier system.

This module provides comprehensive error processing for data validation including:
- Invalid data skipping with detailed logging
- Processing continuation after validation errors
- Error aggregation and reporting
- Integration with DataValidator for seamless error handling

Implements requirements 3.3, 3.4 for validation error processing.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Callable
from collections import Counter, defaultdict

try:
    # Try relative imports first (when used as a module)
    from .data_validator import (
        DataValidator,
        DataValidationResult,
        ValidationStatus,
        ValidationConfig,
    )
    from .exceptions import DataNotFoundError
except ImportError:
    # Fall back to absolute imports (when used standalone)
    from data_validator import (
        DataValidator,
        DataValidationResult,
        ValidationStatus,
        ValidationConfig,
    )
    from exceptions import DataNotFoundError


@dataclass
class ValidationErrorSummary:
    """Summary of validation errors for reporting."""

    total_processed: int = 0
    successful_validations: int = 0
    validation_warnings: int = 0
    validation_errors: int = 0
    skipped_items: int = 0

    # Detailed error breakdown
    error_types: Dict[str, int] = field(default_factory=lambda: Counter())
    warning_types: Dict[str, int] = field(default_factory=lambda: Counter())
    skipped_symbols: List[str] = field(default_factory=list)

    # Processing metrics
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    processing_duration: Optional[float] = None

    def finalize(self) -> None:
        """Finalize the summary by calculating duration."""
        self.end_time = datetime.now()
        self.processing_duration = (self.end_time - self.start_time).total_seconds()

    def get_success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_processed == 0:
            return 0.0
        return (self.successful_validations / self.total_processed) * 100

    def get_error_rate(self) -> float:
        """Calculate error rate as percentage."""
        if self.total_processed == 0:
            return 0.0
        return (
            (self.validation_errors + self.skipped_items) / self.total_processed
        ) * 100


@dataclass
class ProcessingConfig:
    """Configuration for validation error processing."""

    # Error handling behavior
    continue_on_validation_error: bool = True
    continue_on_processing_error: bool = True
    max_consecutive_errors: int = 10  # Stop if too many consecutive errors
    max_error_rate: float = 0.5  # Stop if error rate exceeds 50%

    # Logging configuration
    log_all_validations: bool = False
    log_validation_warnings: bool = True
    log_validation_errors: bool = True
    log_skipped_items: bool = True
    log_summary: bool = True

    # Error aggregation
    aggregate_similar_errors: bool = True
    max_error_details: int = 100  # Maximum number of detailed errors to keep

    # Processing limits
    batch_size: Optional[int] = None  # Process in batches if specified
    timeout_seconds: Optional[float] = None  # Overall processing timeout


class ValidationErrorProcessor:
    """
    Processes validation errors and manages data processing continuation.

    Provides functionality for:
    - Skipping invalid data with detailed logging
    - Continuing processing after validation errors
    - Aggregating and reporting validation errors
    - Integration with DataValidator

    Implements requirements 3.3, 3.4 for validation error processing.
    """

    def __init__(
        self,
        data_validator: Optional[DataValidator] = None,
        config: Optional[ProcessingConfig] = None,
    ):
        """
        Initialize ValidationErrorProcessor.

        Args:
            data_validator: DataValidator instance, creates default if None
            config: Processing configuration, uses defaults if None
        """
        self.data_validator = data_validator or DataValidator()
        self.config = config or ProcessingConfig()
        self.logger = logging.getLogger(__name__)

        # Processing state
        self.current_summary = ValidationErrorSummary()
        self.consecutive_errors = 0
        self.processing_active = False

    def process_financial_data_batch(
        self, data_batch: List[Tuple[str, Dict[str, Any]]]
    ) -> Tuple[List[Tuple[str, Dict[str, Any]]], ValidationErrorSummary]:
        """
        Process a batch of financial data with validation and error handling.

        Args:
            data_batch: List of (symbol, financial_data) tuples

        Returns:
            Tuple of (valid_data_list, error_summary)

        Implements requirement 3.3 for invalid data skipping.
        """
        self._start_processing()
        valid_data = []

        try:
            for symbol, financial_data in data_batch:
                self.current_summary.total_processed += 1

                try:
                    # Validate financial data
                    validation_result = self.data_validator.validate_financial_data(
                        symbol, financial_data
                    )

                    # Process validation result
                    if self._process_validation_result(validation_result, symbol):
                        valid_data.append((symbol, financial_data))
                        self.consecutive_errors = 0  # Reset consecutive error counter
                    else:
                        self._handle_skipped_item(
                            symbol, "financial_data", validation_result
                        )

                except Exception as e:
                    self._handle_processing_error(symbol, "financial_data", e)
                    if not self.config.continue_on_processing_error:
                        break

                # Check if we should stop processing
                if self._should_stop_processing():
                    self.logger.warning("Stopping processing due to error thresholds")
                    break

        finally:
            self._finalize_processing()

        return valid_data, self.current_summary

    def process_price_data_batch(
        self,
        data_batch: List[
            Tuple[str, Any]
        ],  # Any to accommodate DataFrame or other formats
    ) -> Tuple[List[Tuple[str, Any]], ValidationErrorSummary]:
        """
        Process a batch of price data with validation and error handling.

        Args:
            data_batch: List of (symbol, price_data) tuples

        Returns:
            Tuple of (valid_data_list, error_summary)

        Implements requirement 3.3 for invalid data skipping.
        """
        self._start_processing()
        valid_data = []

        try:
            for symbol, price_data in data_batch:
                self.current_summary.total_processed += 1

                try:
                    # Validate price data
                    validation_result = self.data_validator.validate_price_data(
                        symbol, price_data
                    )

                    # Process validation result
                    if self._process_validation_result(validation_result, symbol):
                        valid_data.append((symbol, price_data))
                        self.consecutive_errors = 0
                    else:
                        self._handle_skipped_item(
                            symbol, "price_data", validation_result
                        )

                except Exception as e:
                    self._handle_processing_error(symbol, "price_data", e)
                    if not self.config.continue_on_processing_error:
                        break

                if self._should_stop_processing():
                    self.logger.warning("Stopping processing due to error thresholds")
                    break

        finally:
            self._finalize_processing()

        return valid_data, self.current_summary

    def process_dividend_data_batch(
        self, data_batch: List[Tuple[str, Any]]
    ) -> Tuple[List[Tuple[str, Any]], ValidationErrorSummary]:
        """
        Process a batch of dividend data with validation and error handling.

        Args:
            data_batch: List of (symbol, dividend_data) tuples

        Returns:
            Tuple of (valid_data_list, error_summary)

        Implements requirement 3.3 for invalid data skipping.
        """
        self._start_processing()
        valid_data = []

        try:
            for symbol, dividend_data in data_batch:
                self.current_summary.total_processed += 1

                try:
                    # Validate dividend data
                    validation_result = self.data_validator.validate_dividend_data(
                        symbol, dividend_data
                    )

                    # Process validation result
                    if self._process_validation_result(validation_result, symbol):
                        valid_data.append((symbol, dividend_data))
                        self.consecutive_errors = 0
                    else:
                        self._handle_skipped_item(
                            symbol, "dividend_data", validation_result
                        )

                except Exception as e:
                    self._handle_processing_error(symbol, "dividend_data", e)
                    if not self.config.continue_on_processing_error:
                        break

                if self._should_stop_processing():
                    self.logger.warning("Stopping processing due to error thresholds")
                    break

        finally:
            self._finalize_processing()

        return valid_data, self.current_summary

    def process_with_custom_validator(
        self,
        data_batch: List[Tuple[str, Any]],
        validator_func: Callable[[str, Any], DataValidationResult],
        data_type: str = "custom",
    ) -> Tuple[List[Tuple[str, Any]], ValidationErrorSummary]:
        """
        Process data batch with a custom validation function.

        Args:
            data_batch: List of (symbol, data) tuples
            validator_func: Custom validation function
            data_type: Type description for logging

        Returns:
            Tuple of (valid_data_list, error_summary)
        """
        self._start_processing()
        valid_data = []

        try:
            for symbol, data in data_batch:
                self.current_summary.total_processed += 1

                try:
                    # Use custom validator
                    validation_result = validator_func(symbol, data)

                    # Process validation result
                    if self._process_validation_result(validation_result, symbol):
                        valid_data.append((symbol, data))
                        self.consecutive_errors = 0
                    else:
                        self._handle_skipped_item(symbol, data_type, validation_result)

                except Exception as e:
                    self._handle_processing_error(symbol, data_type, e)
                    if not self.config.continue_on_processing_error:
                        break

                if self._should_stop_processing():
                    self.logger.warning("Stopping processing due to error thresholds")
                    break

        finally:
            self._finalize_processing()

        return valid_data, self.current_summary

    def validate_and_filter_data(
        self,
        symbol: str,
        financial_data: Optional[Dict[str, Any]] = None,
        price_data: Optional[Any] = None,
        dividend_data: Optional[Any] = None,
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Validate multiple data types for a single symbol and return filtering decision.

        Args:
            symbol: Stock symbol
            financial_data: Financial information dictionary
            price_data: Price data (DataFrame or similar)
            dividend_data: Dividend data (DataFrame or similar)

        Returns:
            Tuple of (should_include, warnings, errors)

        Implements requirement 3.4 for processing continuation.
        """
        all_warnings = []
        all_errors = []
        has_valid_data = False

        # Validate financial data if provided
        if financial_data is not None:
            result = self.data_validator.validate_financial_data(symbol, financial_data)
            all_warnings.extend(result.warnings)
            all_errors.extend(result.errors)
            if result.is_valid:
                has_valid_data = True

        # Validate price data if provided
        if price_data is not None:
            result = self.data_validator.validate_price_data(symbol, price_data)
            all_warnings.extend(result.warnings)
            all_errors.extend(result.errors)
            if result.is_valid:
                has_valid_data = True

        # Validate dividend data if provided
        if dividend_data is not None:
            result = self.data_validator.validate_dividend_data(symbol, dividend_data)
            all_warnings.extend(result.warnings)
            all_errors.extend(result.errors)
            if result.is_valid:
                has_valid_data = True

        # Decision logic: include if at least one data type is valid
        should_include = has_valid_data and len(all_errors) == 0

        # Log the decision
        if should_include:
            if all_warnings:
                self.logger.warning(
                    f"Including {symbol} with warnings: {'; '.join(all_warnings)}"
                )
        else:
            self.logger.warning(
                f"Excluding {symbol} due to validation issues - "
                f"Errors: {len(all_errors)}, Warnings: {len(all_warnings)}"
            )

        return should_include, all_warnings, all_errors

    def get_error_summary(self) -> ValidationErrorSummary:
        """Get current error summary."""
        return self.current_summary

    def reset_summary(self) -> None:
        """Reset error summary for new processing batch."""
        self.current_summary = ValidationErrorSummary()
        self.consecutive_errors = 0

    def _start_processing(self) -> None:
        """Initialize processing state."""
        if not self.processing_active:
            self.reset_summary()
            self.processing_active = True

    def _finalize_processing(self) -> None:
        """Finalize processing and log summary."""
        if self.processing_active:
            self.current_summary.finalize()
            self.processing_active = False

            if self.config.log_summary:
                self._log_processing_summary()

    def _process_validation_result(
        self, result: DataValidationResult, symbol: str
    ) -> bool:
        """
        Process a validation result and update statistics.

        Args:
            result: Validation result
            symbol: Stock symbol

        Returns:
            True if data should be included, False if should be skipped
        """
        if result.status == ValidationStatus.VALID:
            self.current_summary.successful_validations += 1
            if self.config.log_all_validations:
                self.logger.debug(f"Validation passed for {symbol}")
            return True

        elif result.status == ValidationStatus.WARNING:
            self.current_summary.validation_warnings += 1
            self.current_summary.successful_validations += (
                1  # Still valid with warnings
            )

            # Aggregate warning types
            for warning in result.warnings:
                self.current_summary.warning_types[warning] += 1

            if self.config.log_validation_warnings:
                self.logger.warning(
                    f"Validation warnings for {symbol} ({result.data_type}): "
                    f"{'; '.join(result.warnings)}"
                )
            return True

        else:  # ERROR or INVALID
            self.current_summary.validation_errors += 1
            self.consecutive_errors += 1

            # Aggregate error types
            for error in result.errors:
                self.current_summary.error_types[error] += 1

            if self.config.log_validation_errors:
                self.logger.error(
                    f"Validation failed for {symbol} ({result.data_type}): "
                    f"Status: {result.status.value}, Errors: {'; '.join(result.errors)}"
                )
            return False

    def _handle_skipped_item(
        self, symbol: str, data_type: str, validation_result: DataValidationResult
    ) -> None:
        """Handle a skipped item due to validation failure."""
        self.current_summary.skipped_items += 1
        self.current_summary.skipped_symbols.append(symbol)

        if self.config.log_skipped_items:
            self.logger.warning(
                f"Skipping {symbol} ({data_type}) due to validation failure - "
                f"Status: {validation_result.status.value}, "
                f"Quality Score: {validation_result.quality_score:.2f}"
            )

    def _handle_processing_error(
        self, symbol: str, data_type: str, error: Exception
    ) -> None:
        """Handle a processing error (not validation error)."""
        self.current_summary.validation_errors += 1
        self.consecutive_errors += 1

        error_msg = f"Processing error for {symbol} ({data_type}): {str(error)}"
        self.current_summary.error_types[error_msg] += 1

        self.logger.error(error_msg)

    def _should_stop_processing(self) -> bool:
        """Check if processing should be stopped due to error thresholds."""
        # Check consecutive errors
        if self.consecutive_errors >= self.config.max_consecutive_errors:
            self.logger.error(
                f"Stopping due to {self.consecutive_errors} consecutive errors "
                f"(threshold: {self.config.max_consecutive_errors})"
            )
            return True

        # Check overall error rate
        if (
            self.current_summary.total_processed > 10
        ):  # Only check after some processing
            error_rate = self.current_summary.get_error_rate() / 100
            if error_rate > self.config.max_error_rate:
                self.logger.error(
                    f"Stopping due to high error rate: {error_rate*100:.1f}% "
                    f"(threshold: {self.config.max_error_rate*100:.1f}%)"
                )
                return True

        return False

    def _log_processing_summary(self) -> None:
        """Log detailed processing summary."""
        summary = self.current_summary

        self.logger.info(
            f"Validation processing completed - "
            f"Total: {summary.total_processed}, "
            f"Successful: {summary.successful_validations}, "
            f"Warnings: {summary.validation_warnings}, "
            f"Errors: {summary.validation_errors}, "
            f"Skipped: {summary.skipped_items}, "
            f"Success Rate: {summary.get_success_rate():.1f}%, "
            f"Duration: {summary.processing_duration:.2f}s"
        )

        # Log most common errors if any
        if summary.error_types:
            top_errors = summary.error_types.most_common(5)
            self.logger.warning(
                f"Top validation errors: {', '.join([f'{error}: {count}' for error, count in top_errors])}"
            )

        # Log most common warnings if any
        if summary.warning_types:
            top_warnings = summary.warning_types.most_common(3)
            self.logger.info(
                f"Top validation warnings: {', '.join([f'{warning}: {count}' for warning, count in top_warnings])}"
            )

        # Log skipped symbols if not too many
        if summary.skipped_symbols and len(summary.skipped_symbols) <= 20:
            self.logger.warning(
                f"Skipped symbols: {', '.join(summary.skipped_symbols)}"
            )
        elif len(summary.skipped_symbols) > 20:
            self.logger.warning(
                f"Skipped {len(summary.skipped_symbols)} symbols "
                f"(first 10: {', '.join(summary.skipped_symbols[:10])})"
            )


# Utility functions for integration with existing code


def create_validation_error_processor(
    strict_mode: bool = False, continue_on_error: bool = True, log_details: bool = True
) -> ValidationErrorProcessor:
    """
    Create a ValidationErrorProcessor with common configuration.

    Args:
        strict_mode: If True, warnings become errors
        continue_on_error: If True, continue processing after errors
        log_details: If True, log detailed validation information

    Returns:
        Configured ValidationErrorProcessor instance
    """
    validation_config = ValidationConfig(
        strict_mode=strict_mode, log_validation_details=log_details
    )

    processing_config = ProcessingConfig(
        continue_on_validation_error=continue_on_error,
        continue_on_processing_error=continue_on_error,
        log_validation_warnings=log_details,
        log_validation_errors=True,
        log_skipped_items=log_details,
        log_summary=True,
    )

    data_validator = DataValidator(validation_config)
    return ValidationErrorProcessor(data_validator, processing_config)


def validate_and_skip_invalid_data(
    data_items: List[Tuple[str, Any]],
    validation_func: Callable[[str, Any], DataValidationResult],
    data_type: str = "data",
) -> Tuple[List[Tuple[str, Any]], ValidationErrorSummary]:
    """
    Utility function to validate data and skip invalid items.

    Args:
        data_items: List of (symbol, data) tuples to validate
        validation_func: Function to validate each item
        data_type: Description of data type for logging

    Returns:
        Tuple of (valid_items, error_summary)

    Implements requirements 3.3, 3.4 for validation error processing.
    """
    processor = create_validation_error_processor()
    return processor.process_with_custom_validator(
        data_items, validation_func, data_type
    )
