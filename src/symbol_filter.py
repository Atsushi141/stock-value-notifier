"""
Symbol filtering module for comprehensive symbol list management.

This module provides advanced symbol filtering functionality including:
- Pre-filtering of invalid symbols before processing
- Symbol list update logging and tracking
- Empty list detection and alert functionality
- Integration with SymbolValidator and ErrorMetrics
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

from .symbol_validator import SymbolValidator, ValidationResult, ValidationStatus
from .error_metrics import ErrorMetrics, ErrorType, AlertLevel


class FilteringMode(Enum):
    """Symbol filtering modes."""

    STRICT = "strict"  # Filter out any symbol with validation issues
    TOLERANT = "tolerant"  # Only filter out clearly invalid/delisted symbols
    PERMISSIVE = "permissive"  # Minimal filtering, only obvious failures


@dataclass
class FilteringResult:
    """Result of symbol filtering operation."""

    original_symbols: List[str]
    valid_symbols: List[str]
    filtered_symbols: List[str]
    delisted_symbols: List[str]
    invalid_symbols: List[str]
    error_symbols: List[str]
    filtering_mode: FilteringMode
    processing_time: float
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def filter_rate(self) -> float:
        """Calculate the percentage of symbols filtered out."""
        if not self.original_symbols:
            return 0.0
        return len(self.filtered_symbols) / len(self.original_symbols)

    @property
    def success_rate(self) -> float:
        """Calculate the percentage of symbols that passed filtering."""
        if not self.original_symbols:
            return 0.0
        return len(self.valid_symbols) / len(self.original_symbols)


@dataclass
class FilteringStats:
    """Statistics for symbol filtering operations."""

    total_filtering_operations: int = 0
    total_symbols_processed: int = 0
    total_symbols_filtered: int = 0
    total_valid_symbols: int = 0

    # Breakdown by reason
    delisted_count: int = 0
    invalid_count: int = 0
    error_count: int = 0

    # Performance metrics
    total_processing_time: float = 0.0
    average_processing_time: float = 0.0

    # Alert tracking
    empty_list_alerts: int = 0
    high_filter_rate_alerts: int = 0

    def update_from_result(self, result: FilteringResult) -> None:
        """Update statistics from a filtering result."""
        self.total_filtering_operations += 1
        self.total_symbols_processed += len(result.original_symbols)
        self.total_symbols_filtered += len(result.filtered_symbols)
        self.total_valid_symbols += len(result.valid_symbols)

        self.delisted_count += len(result.delisted_symbols)
        self.invalid_count += len(result.invalid_symbols)
        self.error_count += len(result.error_symbols)

        self.total_processing_time += result.processing_time
        self.average_processing_time = (
            self.total_processing_time / self.total_filtering_operations
        )


class SymbolFilter:
    """
    Advanced symbol filtering system with comprehensive logging and alerting.

    Provides functionality for:
    - Pre-filtering invalid symbols before processing
    - Symbol list update logging and tracking
    - Empty list detection and alert functionality
    - Configurable filtering modes and thresholds
    - Integration with existing validation and error tracking systems

    Implements requirements 6.1, 6.2, 6.3, 6.5 for symbol filtering and management.
    """

    def __init__(
        self,
        symbol_validator: Optional[SymbolValidator] = None,
        error_metrics: Optional[ErrorMetrics] = None,
        filtering_mode: FilteringMode = FilteringMode.TOLERANT,
        high_filter_rate_threshold: float = 0.3,  # 30% filter rate triggers alert
        empty_list_alert: bool = True,
        cache_duration: timedelta = timedelta(hours=1),
    ):
        """
        Initialize SymbolFilter with configurable behavior.

        Args:
            symbol_validator: SymbolValidator instance (creates new if None)
            error_metrics: ErrorMetrics instance (creates new if None)
            filtering_mode: Default filtering mode
            high_filter_rate_threshold: Threshold for high filter rate alerts
            empty_list_alert: Whether to alert on empty filtered lists
            cache_duration: Duration to cache filtering results
        """
        self.logger = logging.getLogger(__name__)

        # Initialize dependencies
        self.symbol_validator = symbol_validator or SymbolValidator()
        self.error_metrics = error_metrics or ErrorMetrics()

        # Configuration
        self.filtering_mode = filtering_mode
        self.high_filter_rate_threshold = high_filter_rate_threshold
        self.empty_list_alert = empty_list_alert
        self.cache_duration = cache_duration

        # Statistics and tracking
        self.stats = FilteringStats()
        self.filtering_history: List[FilteringResult] = []

        # Caching for performance
        self.filtering_cache: Dict[str, FilteringResult] = {}

        # Alert state tracking
        self.last_empty_list_alert: Optional[datetime] = None
        self.last_high_filter_rate_alert: Optional[datetime] = None
        self.alert_cooldown = timedelta(minutes=30)

        self.logger.info(
            f"SymbolFilter initialized - Mode: {filtering_mode.value}, "
            f"High filter rate threshold: {high_filter_rate_threshold*100:.1f}%, "
            f"Empty list alerts: {empty_list_alert}"
        )

    def filter_symbols(
        self,
        symbols: List[str],
        filtering_mode: Optional[FilteringMode] = None,
        operation_name: str = "symbol_filtering",
        log_details: bool = True,
    ) -> FilteringResult:
        """
        Filter a list of symbols with comprehensive logging and alerting.

        Args:
            symbols: List of symbols to filter
            filtering_mode: Override default filtering mode
            operation_name: Name of the operation for logging
            log_details: Whether to log detailed filtering information

        Returns:
            FilteringResult with detailed filtering information

        Implements requirements 6.1, 6.2, 6.3 for symbol filtering and logging
        """
        start_time = datetime.now()
        mode = filtering_mode or self.filtering_mode

        self.logger.info(
            f"Starting symbol filtering - Operation: {operation_name}, "
            f"Symbols: {len(symbols)}, Mode: {mode.value}"
        )

        # Check cache first
        cache_key = f"{hash(tuple(sorted(symbols)))}_{mode.value}"
        cached_result = self._get_cached_result(cache_key)
        if cached_result:
            self.logger.debug(
                f"Using cached filtering result for {len(symbols)} symbols"
            )
            return cached_result

        # Perform validation
        validation_results = self.symbol_validator.batch_validate_symbols(symbols)

        # Categorize symbols based on validation results and filtering mode
        valid_symbols = []
        delisted_symbols = []
        invalid_symbols = []
        error_symbols = []

        for symbol, result in validation_results.items():
            if self._should_include_symbol(result, mode):
                valid_symbols.append(symbol)
            else:
                if result.status == ValidationStatus.DELISTED:
                    delisted_symbols.append(symbol)
                elif result.status in [
                    ValidationStatus.INVALID,
                    ValidationStatus.NOT_FOUND,
                ]:
                    invalid_symbols.append(symbol)
                else:  # ValidationStatus.ERROR
                    error_symbols.append(symbol)

        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()

        # Create filtering result
        filtered_symbols = delisted_symbols + invalid_symbols + error_symbols
        result = FilteringResult(
            original_symbols=symbols.copy(),
            valid_symbols=valid_symbols,
            filtered_symbols=filtered_symbols,
            delisted_symbols=delisted_symbols,
            invalid_symbols=invalid_symbols,
            error_symbols=error_symbols,
            filtering_mode=mode,
            processing_time=processing_time,
        )

        # Cache the result
        self.filtering_cache[cache_key] = result

        # Update statistics
        self.stats.update_from_result(result)
        self.filtering_history.append(result)

        # Log filtering results
        if log_details:
            self._log_filtering_results(result, operation_name)

        # Check for alert conditions
        self._check_alert_conditions(result, operation_name)

        # Record metrics
        self._record_filtering_metrics(result, operation_name)

        return result

    def pre_filter_symbol_list(
        self,
        symbols: List[str],
        operation_name: str = "pre_filtering",
        update_log: bool = True,
    ) -> List[str]:
        """
        Pre-filter a symbol list before processing with update logging.

        Args:
            symbols: Original symbol list
            operation_name: Name of the operation for logging
            update_log: Whether to log symbol list updates

        Returns:
            Filtered list of valid symbols

        Implements requirements 6.1, 6.2 for pre-filtering and update logging
        """
        self.logger.info(
            f"Pre-filtering symbol list - Operation: {operation_name}, "
            f"Original count: {len(symbols)}"
        )

        # Perform filtering
        result = self.filter_symbols(
            symbols=symbols,
            operation_name=operation_name,
            log_details=True,
        )

        # Log symbol list updates if requested
        if update_log:
            self._log_symbol_list_update(result, operation_name)

        return result.valid_symbols

    def validate_and_alert_empty_list(
        self,
        symbols: List[str],
        operation_name: str = "empty_list_check",
        force_alert: bool = False,
    ) -> bool:
        """
        Check for empty symbol list and send alert if configured.

        Args:
            symbols: Symbol list to check
            operation_name: Name of the operation for logging
            force_alert: Force alert even if in cooldown period

        Returns:
            True if list is empty, False otherwise

        Implements requirement 6.5 for empty list detection and alerting
        """
        is_empty = len(symbols) == 0

        if is_empty and self.empty_list_alert:
            # Check alert cooldown unless forced
            should_alert = force_alert or (
                self.last_empty_list_alert is None
                or datetime.now() - self.last_empty_list_alert > self.alert_cooldown
            )

            if should_alert:
                self._send_empty_list_alert(operation_name)
                self.last_empty_list_alert = datetime.now()
                self.stats.empty_list_alerts += 1

        return is_empty

    def get_filtering_statistics(self) -> Dict[str, any]:
        """
        Get comprehensive filtering statistics.

        Returns:
            Dictionary with detailed filtering statistics
        """
        recent_results = self._get_recent_results(hours=24)

        return {
            "overall_stats": {
                "total_operations": self.stats.total_filtering_operations,
                "total_symbols_processed": self.stats.total_symbols_processed,
                "total_symbols_filtered": self.stats.total_symbols_filtered,
                "total_valid_symbols": self.stats.total_valid_symbols,
                "overall_filter_rate": (
                    self.stats.total_symbols_filtered
                    / max(self.stats.total_symbols_processed, 1)
                ),
                "overall_success_rate": (
                    self.stats.total_valid_symbols
                    / max(self.stats.total_symbols_processed, 1)
                ),
                "average_processing_time": self.stats.average_processing_time,
            },
            "breakdown_stats": {
                "delisted_count": self.stats.delisted_count,
                "invalid_count": self.stats.invalid_count,
                "error_count": self.stats.error_count,
            },
            "recent_stats": {
                "recent_operations": len(recent_results),
                "recent_filter_rate": (
                    sum(r.filter_rate for r in recent_results)
                    / max(len(recent_results), 1)
                ),
                "recent_success_rate": (
                    sum(r.success_rate for r in recent_results)
                    / max(len(recent_results), 1)
                ),
            },
            "alert_stats": {
                "empty_list_alerts": self.stats.empty_list_alerts,
                "high_filter_rate_alerts": self.stats.high_filter_rate_alerts,
                "last_empty_list_alert": (
                    self.last_empty_list_alert.isoformat()
                    if self.last_empty_list_alert
                    else None
                ),
                "last_high_filter_rate_alert": (
                    self.last_high_filter_rate_alert.isoformat()
                    if self.last_high_filter_rate_alert
                    else None
                ),
            },
            "configuration": {
                "filtering_mode": self.filtering_mode.value,
                "high_filter_rate_threshold": self.high_filter_rate_threshold,
                "empty_list_alert": self.empty_list_alert,
                "cache_duration_hours": self.cache_duration.total_seconds() / 3600,
            },
        }

    def configure_filtering(
        self,
        filtering_mode: Optional[FilteringMode] = None,
        high_filter_rate_threshold: Optional[float] = None,
        empty_list_alert: Optional[bool] = None,
    ) -> None:
        """
        Configure filtering behavior.

        Args:
            filtering_mode: New filtering mode
            high_filter_rate_threshold: New threshold for high filter rate alerts
            empty_list_alert: Whether to enable empty list alerts
        """
        if filtering_mode is not None:
            self.filtering_mode = filtering_mode
            self.logger.info(f"Filtering mode updated to: {filtering_mode.value}")

        if high_filter_rate_threshold is not None:
            self.high_filter_rate_threshold = high_filter_rate_threshold
            self.logger.info(
                f"High filter rate threshold updated to: {high_filter_rate_threshold*100:.1f}%"
            )

        if empty_list_alert is not None:
            self.empty_list_alert = empty_list_alert
            self.logger.info(
                f"Empty list alerts {'enabled' if empty_list_alert else 'disabled'}"
            )

    def clear_cache(self) -> None:
        """Clear filtering cache."""
        cache_size = len(self.filtering_cache)
        self.filtering_cache.clear()
        self.logger.info(f"Filtering cache cleared - {cache_size} entries removed")

    def _should_include_symbol(
        self, result: ValidationResult, mode: FilteringMode
    ) -> bool:
        """
        Determine if a symbol should be included based on validation result and mode.

        Args:
            result: Validation result for the symbol
            mode: Filtering mode to apply

        Returns:
            True if symbol should be included, False otherwise
        """
        if mode == FilteringMode.STRICT:
            # Only include clearly valid symbols
            return result.status == ValidationStatus.VALID

        elif mode == FilteringMode.TOLERANT:
            # Include valid symbols and exclude clearly invalid/delisted ones
            return result.status not in [
                ValidationStatus.DELISTED,
                ValidationStatus.INVALID,
            ]

        elif mode == FilteringMode.PERMISSIVE:
            # Only exclude clearly delisted symbols
            return result.status != ValidationStatus.DELISTED

        else:
            # Default to tolerant mode
            return result.status not in [
                ValidationStatus.DELISTED,
                ValidationStatus.INVALID,
            ]

    def _get_cached_result(self, cache_key: str) -> Optional[FilteringResult]:
        """Get cached filtering result if still valid."""
        if cache_key not in self.filtering_cache:
            return None

        cached_result = self.filtering_cache[cache_key]

        # Check if cache is still valid
        if datetime.now() - cached_result.timestamp > self.cache_duration:
            del self.filtering_cache[cache_key]
            return None

        return cached_result

    def _log_filtering_results(
        self, result: FilteringResult, operation_name: str
    ) -> None:
        """Log detailed filtering results."""
        filter_rate = result.filter_rate * 100
        success_rate = result.success_rate * 100

        self.logger.info(
            f"Symbol filtering completed - Operation: {operation_name}, "
            f"Original: {len(result.original_symbols)}, Valid: {len(result.valid_symbols)}, "
            f"Filtered: {len(result.filtered_symbols)} ({filter_rate:.1f}%), "
            f"Success rate: {success_rate:.1f}%, "
            f"Processing time: {result.processing_time:.2f}s"
        )

        if result.filtered_symbols:
            self.logger.info(
                f"Filtering breakdown - Delisted: {len(result.delisted_symbols)}, "
                f"Invalid: {len(result.invalid_symbols)}, "
                f"Errors: {len(result.error_symbols)}"
            )

            # Log sample of filtered symbols for debugging
            if result.delisted_symbols:
                sample_delisted = result.delisted_symbols[:5]
                self.logger.debug(
                    f"Sample delisted symbols: {sample_delisted}"
                    + ("..." if len(result.delisted_symbols) > 5 else "")
                )

            if result.invalid_symbols:
                sample_invalid = result.invalid_symbols[:5]
                self.logger.debug(
                    f"Sample invalid symbols: {sample_invalid}"
                    + ("..." if len(result.invalid_symbols) > 5 else "")
                )

    def _log_symbol_list_update(
        self, result: FilteringResult, operation_name: str
    ) -> None:
        """Log symbol list update information."""
        if result.filtered_symbols:
            self.logger.warning(
                f"Symbol list updated - Operation: {operation_name}, "
                f"Removed {len(result.filtered_symbols)} symbols: "
                f"Delisted: {len(result.delisted_symbols)}, "
                f"Invalid: {len(result.invalid_symbols)}, "
                f"Errors: {len(result.error_symbols)}"
            )

            # Log specific symbols that were removed
            if result.delisted_symbols:
                self.logger.warning(
                    f"Delisted symbols removed: {result.delisted_symbols}"
                )
            if result.invalid_symbols:
                self.logger.warning(
                    f"Invalid symbols removed: {result.invalid_symbols}"
                )
            if result.error_symbols:
                self.logger.error(f"Error symbols removed: {result.error_symbols}")
        else:
            self.logger.info(f"Symbol list unchanged - Operation: {operation_name}")

    def _check_alert_conditions(
        self, result: FilteringResult, operation_name: str
    ) -> None:
        """Check for alert conditions and send alerts if necessary."""
        # Check for empty list
        if not result.valid_symbols:
            self.validate_and_alert_empty_list(result.valid_symbols, operation_name)

        # Check for high filter rate
        if result.filter_rate > self.high_filter_rate_threshold:
            should_alert = (
                self.last_high_filter_rate_alert is None
                or datetime.now() - self.last_high_filter_rate_alert
                > self.alert_cooldown
            )

            if should_alert:
                self._send_high_filter_rate_alert(result, operation_name)
                self.last_high_filter_rate_alert = datetime.now()
                self.stats.high_filter_rate_alerts += 1

    def _send_empty_list_alert(self, operation_name: str) -> None:
        """Send alert for empty symbol list."""
        self.logger.error(
            f"CRITICAL ALERT: Empty symbol list detected - Operation: {operation_name}. "
            f"All symbols were filtered out! This may indicate a data source issue "
            f"or all symbols are delisted/invalid."
        )

        # Record in error metrics
        self.error_metrics.record_error(
            error_type=ErrorType.DATA_VALIDATION,
            symbol="ALL_SYMBOLS",
            operation=operation_name,
            details="Empty symbol list after filtering - all symbols removed",
            severity=AlertLevel.CRITICAL,
            additional_info={
                "alert_type": "empty_symbol_list",
                "filtering_mode": self.filtering_mode.value,
                "timestamp": datetime.now().isoformat(),
            },
        )

    def _send_high_filter_rate_alert(
        self, result: FilteringResult, operation_name: str
    ) -> None:
        """Send alert for high filter rate."""
        filter_rate = result.filter_rate * 100

        self.logger.warning(
            f"HIGH FILTER RATE ALERT: {filter_rate:.1f}% of symbols filtered - "
            f"Operation: {operation_name}, Threshold: {self.high_filter_rate_threshold*100:.1f}%. "
            f"Original: {len(result.original_symbols)}, Valid: {len(result.valid_symbols)}, "
            f"Filtered: {len(result.filtered_symbols)}"
        )

        # Record in error metrics
        self.error_metrics.record_error(
            error_type=ErrorType.DATA_VALIDATION,
            symbol="SYMBOL_LIST",
            operation=operation_name,
            details=f"High filter rate: {filter_rate:.1f}% of symbols filtered",
            severity=AlertLevel.WARNING,
            additional_info={
                "alert_type": "high_filter_rate",
                "filter_rate": result.filter_rate,
                "threshold": self.high_filter_rate_threshold,
                "original_count": len(result.original_symbols),
                "valid_count": len(result.valid_symbols),
                "filtered_count": len(result.filtered_symbols),
                "delisted_count": len(result.delisted_symbols),
                "invalid_count": len(result.invalid_symbols),
                "error_count": len(result.error_symbols),
                "filtering_mode": result.filtering_mode.value,
                "timestamp": datetime.now().isoformat(),
            },
        )

    def _record_filtering_metrics(
        self, result: FilteringResult, operation_name: str
    ) -> None:
        """Record filtering metrics for monitoring."""
        # Record success for valid symbols
        for symbol in result.valid_symbols:
            self.error_metrics.record_success(
                symbol=symbol,
                operation=f"{operation_name}_filtering",
                duration=result.processing_time / len(result.original_symbols),
            )

        # Record errors for filtered symbols
        for symbol in result.delisted_symbols:
            self.error_metrics.record_error(
                error_type=ErrorType.DELISTED_STOCK,
                symbol=symbol,
                operation=f"{operation_name}_filtering",
                details="Symbol filtered due to delisted status",
                severity=AlertLevel.WARNING,
            )

        for symbol in result.invalid_symbols:
            self.error_metrics.record_error(
                error_type=ErrorType.DATA_NOT_FOUND,
                symbol=symbol,
                operation=f"{operation_name}_filtering",
                details="Symbol filtered due to invalid status",
                severity=AlertLevel.WARNING,
            )

        for symbol in result.error_symbols:
            self.error_metrics.record_error(
                error_type=ErrorType.UNKNOWN,
                symbol=symbol,
                operation=f"{operation_name}_filtering",
                details="Symbol filtered due to validation error",
                severity=AlertLevel.ERROR,
            )

    def _get_recent_results(self, hours: int = 24) -> List[FilteringResult]:
        """Get recent filtering results within the specified time window."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            result
            for result in self.filtering_history
            if result.timestamp >= cutoff_time
        ]
