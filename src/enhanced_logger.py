"""
Enhanced logging module for Stock Value Notifier error handling.

This module provides detailed logging functionality specifically for:
- Delisted stock error logging (requirement 5.1)
- Timezone error logging (requirement 5.2)
- Data validation error logging (requirement 5.3)

Integrates with ErrorMetrics for comprehensive error tracking and analysis.
"""

import logging
import logging.handlers
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import json
import traceback

from .error_metrics import ErrorMetrics, ErrorType, AlertLevel


class EnhancedLogger:
    """
    Enhanced logging system with detailed error categorization and metrics integration.

    Provides specialized logging methods for different error types with structured
    logging format and automatic metrics collection.

    Implements requirements 5.1, 5.2, 5.3 for detailed error logging.
    """

    def __init__(
        self,
        logger_name: str = __name__,
        log_dir: str = "logs",
        error_metrics: Optional[ErrorMetrics] = None,
    ):
        """
        Initialize EnhancedLogger with structured logging and metrics integration.

        Args:
            logger_name: Name for the logger instance
            log_dir: Directory for log files
            error_metrics: ErrorMetrics instance for automatic metrics collection
        """
        self.logger = logging.getLogger(logger_name)
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        # Initialize or use provided ErrorMetrics
        self.error_metrics = error_metrics or ErrorMetrics()

        # Setup specialized log files
        self._setup_specialized_loggers()

        self.logger.info("EnhancedLogger initialized with detailed error tracking")

    def _setup_specialized_loggers(self) -> None:
        """Setup specialized loggers for different error types."""

        # Delisted stock errors log
        self.delisted_log_file = self.log_dir / "delisted_stocks.log"
        self.delisted_handler = logging.handlers.RotatingFileHandler(
            self.delisted_log_file, maxBytes=5 * 1024 * 1024, backupCount=3
        )
        self.delisted_handler.setLevel(logging.WARNING)

        # Timezone errors log
        self.timezone_log_file = self.log_dir / "timezone_errors.log"
        self.timezone_handler = logging.handlers.RotatingFileHandler(
            self.timezone_log_file, maxBytes=2 * 1024 * 1024, backupCount=2
        )
        self.timezone_handler.setLevel(logging.WARNING)

        # Data validation errors log
        self.validation_log_file = self.log_dir / "validation_errors.log"
        self.validation_handler = logging.handlers.RotatingFileHandler(
            self.validation_log_file, maxBytes=5 * 1024 * 1024, backupCount=3
        )
        self.validation_handler.setLevel(logging.WARNING)

        # Create structured formatter for detailed logging
        detailed_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d\n"
            "%(message)s\n"
            "---"
        )

        # Apply formatter to all handlers
        self.delisted_handler.setFormatter(detailed_formatter)
        self.timezone_handler.setFormatter(detailed_formatter)
        self.validation_handler.setFormatter(detailed_formatter)

        # Create specialized loggers
        self.delisted_logger = logging.getLogger("delisted_stocks")
        self.delisted_logger.addHandler(self.delisted_handler)
        self.delisted_logger.setLevel(logging.WARNING)

        self.timezone_logger = logging.getLogger("timezone_errors")
        self.timezone_logger.addHandler(self.timezone_handler)
        self.timezone_logger.setLevel(logging.WARNING)

        self.validation_logger = logging.getLogger("validation_errors")
        self.validation_logger.addHandler(self.validation_handler)
        self.validation_logger.setLevel(logging.WARNING)

    def log_delisted_stock_error(
        self,
        symbol: str,
        operation: str,
        error: Exception,
        error_indicators: Optional[List[str]] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log detailed information about delisted stock errors.

        Args:
            symbol: Stock symbol that appears to be delisted
            operation: Operation that failed (e.g., "get_financial_info")
            error: Original exception that occurred
            error_indicators: List of error indicators that suggested delisting
            additional_context: Additional context information

        Implements requirement 5.1 for delisted stock error logging.
        """
        timestamp = datetime.now()

        # Prepare detailed error information
        error_details = {
            "timestamp": timestamp.isoformat(),
            "symbol": symbol,
            "operation": operation,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_indicators": error_indicators or [],
            "additional_context": additional_context or {},
            "stack_trace": (
                traceback.format_exc() if hasattr(error, "__traceback__") else None
            ),
        }

        # Create structured log message
        log_message = self._format_delisted_error_message(error_details)

        # Log to specialized delisted stocks log
        self.delisted_logger.warning(log_message)

        # Also log to main logger with summary
        self.logger.warning(
            f"DELISTED STOCK DETECTED - Symbol: {symbol}, Operation: {operation}, "
            f"Error: {str(error)}, Indicators: {len(error_indicators or [])}"
        )

        # Record in error metrics
        self.error_metrics.record_error(
            error_type=ErrorType.DELISTED_STOCK,
            symbol=symbol,
            operation=operation,
            details=str(error),
            severity=AlertLevel.WARNING,
            additional_info={
                "error_indicators": error_indicators,
                "original_error_type": type(error).__name__,
                **(additional_context or {}),
            },
        )

    def log_timezone_error(
        self,
        symbol: str,
        operation: str,
        error: Exception,
        timezone_info: Optional[Dict[str, Any]] = None,
        fallback_action: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log detailed information about timezone processing errors.

        Args:
            symbol: Stock symbol where timezone error occurred
            operation: Operation that failed (e.g., "get_dividend_history")
            error: Original timezone-related exception
            timezone_info: Information about timezone states
            fallback_action: Description of fallback action taken
            additional_context: Additional context information

        Implements requirement 5.2 for timezone error logging.
        """
        timestamp = datetime.now()

        # Prepare detailed timezone error information
        error_details = {
            "timestamp": timestamp.isoformat(),
            "symbol": symbol,
            "operation": operation,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timezone_info": timezone_info or {},
            "fallback_action": fallback_action,
            "additional_context": additional_context or {},
            "stack_trace": (
                traceback.format_exc() if hasattr(error, "__traceback__") else None
            ),
        }

        # Create structured log message
        log_message = self._format_timezone_error_message(error_details)

        # Log to specialized timezone errors log
        self.timezone_logger.warning(log_message)

        # Also log to main logger with summary
        self.logger.warning(
            f"TIMEZONE ERROR - Symbol: {symbol}, Operation: {operation}, "
            f"Error: {str(error)}, Fallback: {fallback_action or 'None'}"
        )

        # Record in error metrics
        self.error_metrics.record_error(
            error_type=ErrorType.TIMEZONE_ERROR,
            symbol=symbol,
            operation=operation,
            details=str(error),
            severity=AlertLevel.WARNING,
            additional_info={
                "timezone_info": timezone_info,
                "fallback_action": fallback_action,
                "original_error_type": type(error).__name__,
                **(additional_context or {}),
            },
        )

    def log_data_validation_error(
        self,
        symbol: str,
        data_type: str,
        validation_errors: List[str],
        validation_warnings: Optional[List[str]] = None,
        data_summary: Optional[Dict[str, Any]] = None,
        action_taken: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log detailed information about data validation errors.

        Args:
            symbol: Stock symbol where validation failed
            data_type: Type of data that failed validation (e.g., "financial", "price")
            validation_errors: List of validation error messages
            validation_warnings: List of validation warning messages
            data_summary: Summary of the data that failed validation
            action_taken: Description of action taken (e.g., "skipped", "used_fallback")
            additional_context: Additional context information

        Implements requirement 5.3 for data validation error logging.
        """
        timestamp = datetime.now()

        # Prepare detailed validation error information
        error_details = {
            "timestamp": timestamp.isoformat(),
            "symbol": symbol,
            "data_type": data_type,
            "validation_errors": validation_errors,
            "validation_warnings": validation_warnings or [],
            "error_count": len(validation_errors),
            "warning_count": len(validation_warnings or []),
            "data_summary": data_summary or {},
            "action_taken": action_taken,
            "additional_context": additional_context or {},
        }

        # Create structured log message
        log_message = self._format_validation_error_message(error_details)

        # Log to specialized validation errors log
        self.validation_logger.warning(log_message)

        # Also log to main logger with summary
        self.logger.warning(
            f"DATA VALIDATION FAILED - Symbol: {symbol}, Type: {data_type}, "
            f"Errors: {len(validation_errors)}, Warnings: {len(validation_warnings or [])}, "
            f"Action: {action_taken or 'None'}"
        )

        # Record in error metrics
        self.error_metrics.record_error(
            error_type=ErrorType.DATA_VALIDATION,
            symbol=symbol,
            operation=f"validate_{data_type}_data",
            details=f"{len(validation_errors)} validation errors",
            severity=AlertLevel.WARNING,
            additional_info={
                "data_type": data_type,
                "validation_errors": validation_errors,
                "validation_warnings": validation_warnings,
                "action_taken": action_taken,
                **(additional_context or {}),
            },
        )

    def log_error_summary(self, time_window_hours: int = 1) -> None:
        """
        Log a comprehensive error summary for the specified time window.

        Args:
            time_window_hours: Hours to look back for error summary

        Provides periodic error summaries for monitoring and alerting.
        """
        from datetime import timedelta

        summary = self.error_metrics.get_error_summary(
            time_window=timedelta(hours=time_window_hours)
        )

        # Create summary log message
        summary_message = self._format_error_summary(summary, time_window_hours)

        # Log summary based on severity
        if summary["should_alert"]:
            self.logger.error(f"ERROR THRESHOLD EXCEEDED\n{summary_message}")
        elif summary["failed_operations"] > 0:
            self.logger.warning(f"ERROR SUMMARY\n{summary_message}")
        else:
            self.logger.info(f"ERROR SUMMARY\n{summary_message}")

    def _format_delisted_error_message(self, error_details: Dict[str, Any]) -> str:
        """Format delisted stock error message with structured information."""

        message_parts = [
            f"DELISTED STOCK ERROR DETAILS:",
            f"Symbol: {error_details['symbol']}",
            f"Operation: {error_details['operation']}",
            f"Timestamp: {error_details['timestamp']}",
            f"Error Type: {error_details['error_type']}",
            f"Error Message: {error_details['error_message']}",
        ]

        if error_details["error_indicators"]:
            message_parts.append(
                f"Delisting Indicators: {', '.join(error_details['error_indicators'])}"
            )

        if error_details["additional_context"]:
            message_parts.append(
                f"Additional Context: {json.dumps(error_details['additional_context'], indent=2)}"
            )

        if error_details["stack_trace"]:
            message_parts.append(f"Stack Trace:\n{error_details['stack_trace']}")

        return "\n".join(message_parts)

    def _format_timezone_error_message(self, error_details: Dict[str, Any]) -> str:
        """Format timezone error message with structured information."""

        message_parts = [
            f"TIMEZONE ERROR DETAILS:",
            f"Symbol: {error_details['symbol']}",
            f"Operation: {error_details['operation']}",
            f"Timestamp: {error_details['timestamp']}",
            f"Error Type: {error_details['error_type']}",
            f"Error Message: {error_details['error_message']}",
        ]

        if error_details["timezone_info"]:
            message_parts.append(
                f"Timezone Information: {json.dumps(error_details['timezone_info'], indent=2)}"
            )

        if error_details["fallback_action"]:
            message_parts.append(f"Fallback Action: {error_details['fallback_action']}")

        if error_details["additional_context"]:
            message_parts.append(
                f"Additional Context: {json.dumps(error_details['additional_context'], indent=2)}"
            )

        if error_details["stack_trace"]:
            message_parts.append(f"Stack Trace:\n{error_details['stack_trace']}")

        return "\n".join(message_parts)

    def _format_validation_error_message(self, error_details: Dict[str, Any]) -> str:
        """Format data validation error message with structured information."""

        message_parts = [
            f"DATA VALIDATION ERROR DETAILS:",
            f"Symbol: {error_details['symbol']}",
            f"Data Type: {error_details['data_type']}",
            f"Timestamp: {error_details['timestamp']}",
            f"Error Count: {error_details['error_count']}",
            f"Warning Count: {error_details['warning_count']}",
        ]

        if error_details["validation_errors"]:
            message_parts.append("Validation Errors:")
            for i, error in enumerate(error_details["validation_errors"], 1):
                message_parts.append(f"  {i}. {error}")

        if error_details["validation_warnings"]:
            message_parts.append("Validation Warnings:")
            for i, warning in enumerate(error_details["validation_warnings"], 1):
                message_parts.append(f"  {i}. {warning}")

        if error_details["data_summary"]:
            message_parts.append(
                f"Data Summary: {json.dumps(error_details['data_summary'], indent=2)}"
            )

        if error_details["action_taken"]:
            message_parts.append(f"Action Taken: {error_details['action_taken']}")

        if error_details["additional_context"]:
            message_parts.append(
                f"Additional Context: {json.dumps(error_details['additional_context'], indent=2)}"
            )

        return "\n".join(message_parts)

    def _format_error_summary(
        self, summary: Dict[str, Any], time_window_hours: int
    ) -> str:
        """Format comprehensive error summary message."""

        message_parts = [
            f"ERROR SUMMARY ({time_window_hours}h window):",
            f"Total Operations: {summary['total_operations']}",
            f"Successful: {summary['successful_operations']} ({summary['success_rate']*100:.1f}%)",
            f"Failed: {summary['failed_operations']} ({summary['error_rate']*100:.1f}%)",
            f"Alert Threshold: {summary['alert_threshold']*100:.1f}%",
            f"Should Alert: {summary['should_alert']}",
        ]

        if summary["error_by_type"]:
            message_parts.append("Errors by Type:")
            for error_type, count in summary["error_by_type"].items():
                message_parts.append(f"  {error_type}: {count}")

        if summary["error_by_severity"]:
            message_parts.append("Errors by Severity:")
            for severity, count in summary["error_by_severity"].items():
                message_parts.append(f"  {severity}: {count}")

        if summary["top_problematic_symbols"]:
            message_parts.append("Top Problematic Symbols:")
            for symbol, count in list(summary["top_problematic_symbols"].items())[:5]:
                message_parts.append(f"  {symbol}: {count} errors")

        if summary["top_problematic_operations"]:
            message_parts.append("Top Problematic Operations:")
            for operation, count in list(summary["top_problematic_operations"].items())[
                :5
            ]:
                message_parts.append(f"  {operation}: {count} errors")

        if summary["average_operation_duration"]:
            message_parts.append(
                f"Average Operation Duration: {summary['average_operation_duration']:.2f}s"
            )

        if summary["last_error_time"]:
            message_parts.append(f"Last Error: {summary['last_error_time']}")

        return "\n".join(message_parts)

    def get_error_metrics(self) -> ErrorMetrics:
        """
        Get the ErrorMetrics instance for external access.

        Returns:
            ErrorMetrics instance used by this logger
        """
        return self.error_metrics

    def export_error_logs(self, output_dir: str = "error_exports") -> Dict[str, str]:
        """
        Export all error logs to a specified directory for analysis.

        Args:
            output_dir: Directory to export logs to

        Returns:
            Dictionary mapping log types to exported file paths
        """
        export_dir = Path(output_dir)
        export_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        exported_files = {}

        # Export specialized logs
        log_files = {
            "delisted_stocks": self.delisted_log_file,
            "timezone_errors": self.timezone_log_file,
            "validation_errors": self.validation_log_file,
        }

        for log_type, log_file in log_files.items():
            if log_file.exists():
                export_file = export_dir / f"{log_type}_{timestamp}.log"
                try:
                    import shutil

                    shutil.copy2(log_file, export_file)
                    exported_files[log_type] = str(export_file)
                    self.logger.info(f"Exported {log_type} log to {export_file}")
                except Exception as e:
                    self.logger.error(f"Failed to export {log_type} log: {e}")

        # Export error metrics
        metrics_file = export_dir / f"error_metrics_{timestamp}.json"
        try:
            metrics_data = self.error_metrics.export_metrics(include_records=True)
            with open(metrics_file, "w") as f:
                json.dump(metrics_data, f, indent=2)
            exported_files["error_metrics"] = str(metrics_file)
            self.logger.info(f"Exported error metrics to {metrics_file}")
        except Exception as e:
            self.logger.error(f"Failed to export error metrics: {e}")

        return exported_files

    def cleanup_old_logs(self, days_to_keep: int = 30) -> None:
        """
        Clean up old log files to prevent disk space issues.

        Args:
            days_to_keep: Number of days of logs to retain
        """
        from datetime import timedelta
        import glob

        cutoff_date = datetime.now() - timedelta(days=days_to_keep)

        # Find all log files in the log directory
        log_patterns = [
            str(self.log_dir / "*.log"),
            str(self.log_dir / "*.log.*"),  # Rotated logs
        ]

        cleaned_count = 0
        for pattern in log_patterns:
            for log_file in glob.glob(pattern):
                try:
                    file_path = Path(log_file)
                    if file_path.stat().st_mtime < cutoff_date.timestamp():
                        file_path.unlink()
                        cleaned_count += 1
                        self.logger.debug(f"Cleaned up old log file: {log_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up log file {log_file}: {e}")

        if cleaned_count > 0:
            self.logger.info(
                f"Cleaned up {cleaned_count} old log files (older than {days_to_keep} days)"
            )
