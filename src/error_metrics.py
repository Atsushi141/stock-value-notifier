"""
Error metrics collection and analysis module for Stock Value Notifier.

This module provides comprehensive error tracking, statistics collection,
and alert determination functionality to monitor system health and reliability.
"""

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
from enum import Enum


class ErrorType(Enum):
    """Enumeration of error types for categorization."""

    DELISTED_STOCK = "delisted_stock"
    TIMEZONE_ERROR = "timezone_error"
    DATA_VALIDATION = "data_validation"
    NETWORK_ERROR = "network_error"
    API_RATE_LIMIT = "api_rate_limit"
    DATA_NOT_FOUND = "data_not_found"
    AUTHENTICATION = "authentication"
    UNKNOWN = "unknown"

    @classmethod
    def from_exception(cls, error: Exception) -> "ErrorType":
        """
        Determine ErrorType from exception instance.

        Args:
            error: Exception instance to classify

        Returns:
            Appropriate ErrorType for the exception
        """
        error_name = error.__class__.__name__.lower()
        error_message = str(error).lower()

        # Check for specific error patterns
        if "delisted" in error_message or "possibly delisted" in error_message:
            return cls.DELISTED_STOCK
        elif "timezone" in error_message or "tz" in error_message:
            return cls.TIMEZONE_ERROR
        elif "validation" in error_message or "invalid" in error_message:
            return cls.DATA_VALIDATION
        elif "not found" in error_message or "404" in error_message:
            return cls.DATA_NOT_FOUND
        elif "rate limit" in error_message or "429" in error_message:
            return cls.API_RATE_LIMIT
        elif any(
            net_error in error_name
            for net_error in ["connection", "network", "timeout", "http"]
        ):
            return cls.NETWORK_ERROR
        elif any(
            auth_error in error_name
            for auth_error in ["auth", "permission", "unauthorized", "403"]
        ):
            return cls.AUTHENTICATION
        else:
            return cls.UNKNOWN


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ErrorRecord:
    """Individual error record with detailed information."""

    timestamp: datetime
    error_type: ErrorType
    symbol: str
    operation: str
    details: str
    severity: AlertLevel = AlertLevel.WARNING
    additional_info: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationRecord:
    """Record of successful operations for success rate calculation."""

    timestamp: datetime
    symbol: str
    operation: str
    duration: Optional[float] = None
    additional_info: Dict[str, Any] = field(default_factory=dict)


class ErrorMetrics:
    """
    Comprehensive error metrics collection and analysis system.

    Provides functionality for:
    - Error statistics collection and categorization
    - Error rate calculation and monitoring
    - Alert threshold determination
    - Performance metrics tracking
    - Historical error analysis

    Implements requirements 5.4, 5.5 for error statistics and alerting.
    """

    def __init__(
        self,
        error_threshold: float = 0.1,  # 10% error rate threshold
        alert_window_minutes: int = 60,  # Alert evaluation window
        max_history_hours: int = 24,
    ):  # Maximum history retention
        """
        Initialize ErrorMetrics with configurable thresholds.

        Args:
            error_threshold: Error rate threshold for alerts (0.0-1.0)
            alert_window_minutes: Time window for alert evaluation
            max_history_hours: Maximum hours to retain error history
        """
        self.logger = logging.getLogger(__name__)

        # Configuration
        self.error_threshold = error_threshold
        self.alert_window = timedelta(minutes=alert_window_minutes)
        self.max_history = timedelta(hours=max_history_hours)

        # Error tracking
        self.error_records: List[ErrorRecord] = []
        self.operation_records: List[OperationRecord] = []

        # Statistics counters
        self.error_counts: Counter = Counter()
        self.error_by_type: Dict[ErrorType, List[ErrorRecord]] = defaultdict(list)
        self.error_by_symbol: Dict[str, List[ErrorRecord]] = defaultdict(list)
        self.error_by_operation: Dict[str, List[ErrorRecord]] = defaultdict(list)

        # Success tracking
        self.success_counts: Counter = Counter()
        self.operation_counts: Counter = Counter()

        # Session tracking
        self.session_start: datetime = datetime.now()
        self.last_cleanup: datetime = datetime.now()

        # Alert state
        self.last_alert_time: Optional[datetime] = None
        self.alert_cooldown = timedelta(minutes=30)  # Prevent alert spam

        self.logger.info(
            f"ErrorMetrics initialized - Threshold: {error_threshold*100:.1f}%, "
            f"Alert window: {alert_window_minutes}min, History: {max_history_hours}h"
        )

    def record_error(
        self,
        error_type: ErrorType,
        symbol: str,
        operation: str,
        details: str,
        severity: AlertLevel = AlertLevel.WARNING,
        additional_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record an error occurrence with detailed information.

        Args:
            error_type: Type of error that occurred
            symbol: Stock symbol associated with the error
            operation: Operation that failed
            details: Detailed error description
            severity: Severity level of the error
            additional_info: Additional context information

        Implements requirement 5.4 for error statistics collection.
        """
        timestamp = datetime.now()

        error_record = ErrorRecord(
            timestamp=timestamp,
            error_type=error_type,
            symbol=symbol,
            operation=operation,
            details=details,
            severity=severity,
            additional_info=additional_info or {},
        )

        # Store the error record
        self.error_records.append(error_record)

        # Update counters and categorizations
        self.error_counts[error_type.value] += 1
        self.error_by_type[error_type].append(error_record)
        self.error_by_symbol[symbol].append(error_record)
        self.error_by_operation[operation].append(error_record)

        # Log the error with appropriate level
        log_message = (
            f"Error recorded - Type: {error_type.value}, Symbol: {symbol}, "
            f"Operation: {operation}, Details: {details}"
        )

        if severity == AlertLevel.CRITICAL:
            self.logger.error(f"CRITICAL: {log_message}")
        elif severity == AlertLevel.ERROR:
            self.logger.error(log_message)
        elif severity == AlertLevel.WARNING:
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)

        # Perform periodic cleanup
        self._cleanup_old_records()

    def record_success(
        self,
        symbol: str,
        operation: str,
        duration: Optional[float] = None,
        additional_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a successful operation for success rate calculation.

        Args:
            symbol: Stock symbol for the successful operation
            operation: Operation that succeeded
            duration: Operation duration in seconds
            additional_info: Additional context information

        Implements requirement 5.5 for success rate tracking.
        """
        timestamp = datetime.now()

        operation_record = OperationRecord(
            timestamp=timestamp,
            symbol=symbol,
            operation=operation,
            duration=duration,
            additional_info=additional_info or {},
        )

        # Store the operation record
        self.operation_records.append(operation_record)

        # Update success counters
        self.success_counts[operation] += 1
        self.operation_counts[operation] += 1

        # Also count this operation for error rate calculation
        self.operation_counts[f"{operation}_total"] += 1

        # Log success at debug level to avoid noise
        self.logger.debug(
            f"Success recorded - Symbol: {symbol}, Operation: {operation}"
            + (f", Duration: {duration:.2f}s" if duration else "")
        )

        # Perform periodic cleanup
        self._cleanup_old_records()

    def get_error_rate(
        self, operation: Optional[str] = None, time_window: Optional[timedelta] = None
    ) -> float:
        """
        Calculate error rate for operations within a time window.

        Args:
            operation: Specific operation to calculate rate for (None for all)
            time_window: Time window for calculation (None for session)

        Returns:
            Error rate as a float between 0.0 and 1.0

        Implements requirement 5.4 for error rate calculation.
        """
        if time_window is None:
            time_window = datetime.now() - self.session_start

        cutoff_time = datetime.now() - time_window

        # Count recent errors
        recent_errors = [
            record
            for record in self.error_records
            if record.timestamp >= cutoff_time
            and (operation is None or record.operation == operation)
        ]

        # Count recent operations (successes + errors)
        recent_operations = [
            record
            for record in self.operation_records
            if record.timestamp >= cutoff_time
            and (operation is None or record.operation == operation)
        ]

        total_operations = len(recent_operations) + len(recent_errors)

        if total_operations == 0:
            return 0.0

        error_rate = len(recent_errors) / total_operations

        self.logger.debug(
            f"Error rate calculated - Operation: {operation or 'all'}, "
            f"Window: {time_window}, Errors: {len(recent_errors)}, "
            f"Total: {total_operations}, Rate: {error_rate*100:.2f}%"
        )

        return error_rate

    def get_error_summary(
        self, time_window: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive error statistics summary.

        Args:
            time_window: Time window for summary (None for session)

        Returns:
            Dictionary with detailed error statistics

        Implements requirement 5.4 for error statistics reporting.
        """
        if time_window is None:
            time_window = datetime.now() - self.session_start

        cutoff_time = datetime.now() - time_window

        # Filter recent records
        recent_errors = [
            record for record in self.error_records if record.timestamp >= cutoff_time
        ]

        recent_operations = [
            record
            for record in self.operation_records
            if record.timestamp >= cutoff_time
        ]

        # Calculate statistics
        total_operations = len(recent_operations) + len(recent_errors)
        error_count = len(recent_errors)
        success_count = len(recent_operations)

        # Error breakdown by type
        error_by_type = Counter()
        for error in recent_errors:
            error_by_type[error.error_type.value] += 1

        # Error breakdown by severity
        error_by_severity = Counter()
        for error in recent_errors:
            error_by_severity[error.severity.value] += 1

        # Most problematic symbols
        symbol_errors = Counter()
        for error in recent_errors:
            symbol_errors[error.symbol] += 1

        # Most problematic operations
        operation_errors = Counter()
        for error in recent_errors:
            operation_errors[error.operation] += 1

        # Calculate rates
        error_rate = error_count / total_operations if total_operations > 0 else 0.0
        success_rate = success_count / total_operations if total_operations > 0 else 0.0

        # Performance metrics
        operation_durations = [
            op.duration for op in recent_operations if op.duration is not None
        ]

        avg_duration = (
            sum(operation_durations) / len(operation_durations)
            if operation_durations
            else None
        )

        summary = {
            "time_window_hours": time_window.total_seconds() / 3600,
            "total_operations": total_operations,
            "successful_operations": success_count,
            "failed_operations": error_count,
            "error_rate": error_rate,
            "success_rate": success_rate,
            "error_by_type": dict(error_by_type.most_common()),
            "error_by_severity": dict(error_by_severity.most_common()),
            "top_problematic_symbols": dict(symbol_errors.most_common(10)),
            "top_problematic_operations": dict(operation_errors.most_common(10)),
            "average_operation_duration": avg_duration,
            "session_duration_hours": (
                datetime.now() - self.session_start
            ).total_seconds()
            / 3600,
            "last_error_time": (
                recent_errors[-1].timestamp.isoformat() if recent_errors else None
            ),
            "alert_threshold": self.error_threshold,
            "should_alert": self.should_alert(time_window),
        }

        return summary

    def should_alert(self, time_window: Optional[timedelta] = None) -> bool:
        """
        Determine if an alert should be sent based on error thresholds.

        Args:
            time_window: Time window for alert evaluation (None for default)

        Returns:
            True if alert should be sent, False otherwise

        Implements requirement 5.5 for alert determination.
        """
        if time_window is None:
            time_window = self.alert_window

        # Check if we're in alert cooldown period
        if (
            self.last_alert_time
            and datetime.now() - self.last_alert_time < self.alert_cooldown
        ):
            return False

        # Calculate current error rate
        current_error_rate = self.get_error_rate(time_window=time_window)

        # Check if error rate exceeds threshold
        should_alert = current_error_rate > self.error_threshold

        if should_alert:
            self.logger.warning(
                f"Alert threshold exceeded - Current rate: {current_error_rate*100:.2f}%, "
                f"Threshold: {self.error_threshold*100:.2f}%"
            )
            self.last_alert_time = datetime.now()

        return should_alert

    def get_recent_errors(
        self,
        count: int = 10,
        error_type: Optional[ErrorType] = None,
        symbol: Optional[str] = None,
    ) -> List[ErrorRecord]:
        """
        Get recent error records with optional filtering.

        Args:
            count: Maximum number of errors to return
            error_type: Filter by specific error type
            symbol: Filter by specific symbol

        Returns:
            List of recent error records
        """
        filtered_errors = self.error_records

        # Apply filters
        if error_type:
            filtered_errors = [
                error for error in filtered_errors if error.error_type == error_type
            ]

        if symbol:
            filtered_errors = [
                error for error in filtered_errors if error.symbol == symbol
            ]

        # Sort by timestamp (most recent first) and limit
        recent_errors = sorted(
            filtered_errors, key=lambda x: x.timestamp, reverse=True
        )[:count]

        return recent_errors

    def get_error_trends(
        self, hours: int = 24, bucket_size_minutes: int = 60
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get error trends over time with bucketed statistics.

        Args:
            hours: Number of hours to analyze
            bucket_size_minutes: Size of time buckets in minutes

        Returns:
            Dictionary with trend data by error type
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        bucket_size = timedelta(minutes=bucket_size_minutes)

        # Create time buckets
        buckets = []
        current_time = cutoff_time
        while current_time < datetime.now():
            buckets.append(current_time)
            current_time += bucket_size

        # Initialize trend data
        trends = {}
        for error_type in ErrorType:
            trends[error_type.value] = []

        # Fill buckets with error counts
        for i, bucket_start in enumerate(buckets):
            bucket_end = bucket_start + bucket_size

            for error_type in ErrorType:
                bucket_errors = [
                    error
                    for error in self.error_by_type[error_type]
                    if bucket_start <= error.timestamp < bucket_end
                ]

                trends[error_type.value].append(
                    {
                        "timestamp": bucket_start.isoformat(),
                        "count": len(bucket_errors),
                        "bucket_start": bucket_start.isoformat(),
                        "bucket_end": bucket_end.isoformat(),
                    }
                )

        return trends

    def reset_metrics(self) -> None:
        """
        Reset all metrics and start fresh session.

        Useful for testing or starting new monitoring periods.
        """
        self.logger.info("Resetting error metrics")

        # Clear all records
        self.error_records.clear()
        self.operation_records.clear()

        # Reset counters
        self.error_counts.clear()
        self.error_by_type.clear()
        self.error_by_symbol.clear()
        self.error_by_operation.clear()
        self.success_counts.clear()
        self.operation_counts.clear()

        # Reset session tracking
        self.session_start = datetime.now()
        self.last_cleanup = datetime.now()
        self.last_alert_time = None

    def _cleanup_old_records(self) -> None:
        """
        Clean up old records to prevent memory bloat.

        Called periodically during record operations.
        """
        now = datetime.now()

        # Only cleanup every 10 minutes to avoid overhead
        if now - self.last_cleanup < timedelta(minutes=10):
            return

        cutoff_time = now - self.max_history
        initial_error_count = len(self.error_records)
        initial_operation_count = len(self.operation_records)

        # Remove old error records
        self.error_records = [
            record for record in self.error_records if record.timestamp >= cutoff_time
        ]

        # Remove old operation records
        self.operation_records = [
            record
            for record in self.operation_records
            if record.timestamp >= cutoff_time
        ]

        # Rebuild categorization dictionaries
        self.error_by_type.clear()
        self.error_by_symbol.clear()
        self.error_by_operation.clear()

        for error in self.error_records:
            self.error_by_type[error.error_type].append(error)
            self.error_by_symbol[error.symbol].append(error)
            self.error_by_operation[error.operation].append(error)

        # Update cleanup timestamp
        self.last_cleanup = now

        # Log cleanup results if significant
        removed_errors = initial_error_count - len(self.error_records)
        removed_operations = initial_operation_count - len(self.operation_records)

        if removed_errors > 0 or removed_operations > 0:
            self.logger.debug(
                f"Cleaned up old records - Errors: {removed_errors}, "
                f"Operations: {removed_operations}, "
                f"Retention: {self.max_history.total_seconds()/3600:.1f}h"
            )

    def export_metrics(self, include_records: bool = False) -> Dict[str, Any]:
        """
        Export all metrics data for external analysis or backup.

        Args:
            include_records: Whether to include individual error records

        Returns:
            Dictionary with all metrics data
        """
        export_data = {
            "configuration": {
                "error_threshold": self.error_threshold,
                "alert_window_minutes": self.alert_window.total_seconds() / 60,
                "max_history_hours": self.max_history.total_seconds() / 3600,
            },
            "session_info": {
                "session_start": self.session_start.isoformat(),
                "export_time": datetime.now().isoformat(),
                "session_duration_hours": (
                    datetime.now() - self.session_start
                ).total_seconds()
                / 3600,
            },
            "summary": self.get_error_summary(),
            "counters": {
                "error_counts": dict(self.error_counts),
                "success_counts": dict(self.success_counts),
                "operation_counts": dict(self.operation_counts),
            },
        }

        if include_records:
            export_data["records"] = {
                "error_records": [
                    {
                        "timestamp": record.timestamp.isoformat(),
                        "error_type": record.error_type.value,
                        "symbol": record.symbol,
                        "operation": record.operation,
                        "details": record.details,
                        "severity": record.severity.value,
                        "additional_info": record.additional_info,
                    }
                    for record in self.error_records
                ],
                "operation_records": [
                    {
                        "timestamp": record.timestamp.isoformat(),
                        "symbol": record.symbol,
                        "operation": record.operation,
                        "duration": record.duration,
                        "additional_info": record.additional_info,
                    }
                    for record in self.operation_records
                ],
            }

        return export_data
