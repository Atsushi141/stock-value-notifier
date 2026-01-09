"""
Enhanced Error Handler module for processing continuation and error classification

This module provides comprehensive error handling functionality:
- Individual stock error handling with processing continuation
- Critical vs non-critical error classification
- Error recovery strategies
- Processing continuation policies
- Error aggregation and reporting
- Mode-specific processing behavior (strict/tolerant/debug)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Type, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import traceback

from .exceptions import APIError, RateLimitError, DataNotFoundError
from .enhanced_logger import EnhancedLogger
from .error_metrics import ErrorMetrics, ErrorType
from .retry_manager import RetryManager, RetryConfig


class ErrorSeverity(Enum):
    """Error severity levels for processing decisions"""

    CRITICAL = "critical"  # Stop all processing
    HIGH = "high"  # Stop current batch, continue with next
    MEDIUM = "medium"  # Skip current item, continue processing
    LOW = "low"  # Log warning, continue processing
    INFO = "info"  # Log info, continue processing


class ProcessingAction(Enum):
    """Actions to take when errors occur"""

    STOP_ALL = "stop_all"  # Stop all processing immediately
    STOP_BATCH = "stop_batch"  # Stop current batch, continue with next
    SKIP_ITEM = "skip_item"  # Skip current item, continue with others
    CONTINUE = "continue"  # Continue processing normally
    RETRY = "retry"  # Retry the operation


@dataclass
class ErrorClassification:
    """Classification of an error"""

    severity: ErrorSeverity
    action: ProcessingAction
    retryable: bool
    description: str
    category: str


@dataclass
class ProcessingError:
    """Information about a processing error"""

    timestamp: datetime
    operation: str
    symbol: Optional[str]
    error: Exception
    classification: ErrorClassification
    context: Dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None
    retry_count: int = 0


@dataclass
class ProcessingResult:
    """Result of a processing operation"""

    success: bool
    processed_count: int
    skipped_count: int
    error_count: int
    critical_errors: List[ProcessingError] = field(default_factory=list)
    non_critical_errors: List[ProcessingError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    processing_time: float = 0.0

    def get_success_rate(self) -> float:
        """Calculate success rate"""
        total = self.processed_count + self.skipped_count + self.error_count
        return self.processed_count / total if total > 0 else 0.0

    def get_error_rate(self) -> float:
        """Calculate error rate"""
        total = self.processed_count + self.skipped_count + self.error_count
        return self.error_count / total if total > 0 else 0.0

    def has_critical_errors(self) -> bool:
        """Check if there are any critical errors"""
        return len(self.critical_errors) > 0


@dataclass
class ProcessingConfig:
    """Configuration for error handling and processing continuation"""

    # Processing continuation settings
    continue_on_individual_error: bool = True
    continue_on_batch_error: bool = True
    max_consecutive_errors: int = 10
    max_error_rate: float = 0.5  # Stop if error rate exceeds 50%

    # Error classification settings
    treat_data_not_found_as_warning: bool = True
    treat_rate_limit_as_retryable: bool = True
    treat_network_errors_as_retryable: bool = True

    # Retry settings
    enable_retries: bool = True
    max_retries_per_item: int = 3
    retry_delay: float = 1.0

    # Logging settings
    log_all_errors: bool = True
    log_skipped_items: bool = True
    log_processing_summary: bool = True
    include_stack_traces: bool = False


class ModeSpecificProcessor:
    """
    Handles mode-specific processing behavior for different error handling modes

    Implements requirements 7.2, 7.3, 7.4 for mode-specific behavior:
    - Strict mode: Stop on minor errors
    - Tolerant mode: Continue processing as much as possible
    - Debug mode: Detailed logging and continue processing
    """

    def __init__(self, config: ProcessingConfig, logger: logging.Logger):
        """
        Initialize mode-specific processor

        Args:
            config: Processing configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger

        # Determine mode from config settings
        self.mode = self._determine_mode_from_config()

    def _determine_mode_from_config(self) -> str:
        """
        Determine the operating mode based on configuration settings

        Returns:
            Mode string: 'strict', 'tolerant', or 'debug'
        """
        # Check for strict mode characteristics
        if (
            not self.config.continue_on_batch_error
            and self.config.max_consecutive_errors <= 5
            and self.config.max_error_rate <= 0.2
            and not self.config.treat_data_not_found_as_warning
        ):
            return "strict"

        # Check for debug mode characteristics
        if (
            self.config.max_consecutive_errors >= 50
            and self.config.max_error_rate >= 0.9
            and self.config.include_stack_traces
            and self.config.log_all_errors
            and self.config.log_skipped_items
        ):
            return "debug"

        # Default to tolerant mode
        return "tolerant"

    def should_stop_processing(
        self,
        error: Exception,
        classification: ErrorClassification,
        consecutive_errors: int,
        error_rate: float,
    ) -> bool:
        """
        Determine if processing should stop based on the current mode

        Args:
            error: The exception that occurred
            classification: Error classification
            consecutive_errors: Number of consecutive errors
            error_rate: Current error rate

        Returns:
            True if processing should stop

        Implements requirement 7.2 for strict mode behavior
        """
        if self.mode == "strict":
            return self._strict_mode_should_stop(
                error, classification, consecutive_errors, error_rate
            )
        elif self.mode == "tolerant":
            return self._tolerant_mode_should_stop(
                error, classification, consecutive_errors, error_rate
            )
        elif self.mode == "debug":
            return self._debug_mode_should_stop(
                error, classification, consecutive_errors, error_rate
            )
        else:
            # Fallback to tolerant behavior
            return self._tolerant_mode_should_stop(
                error, classification, consecutive_errors, error_rate
            )

    def _strict_mode_should_stop(
        self,
        error: Exception,
        classification: ErrorClassification,
        consecutive_errors: int,
        error_rate: float,
    ) -> bool:
        """
        Strict mode: Stop on minor errors

        Implements requirement 7.2: WHEN 厳格モードが有効な場合 THEN システムは軽微なエラーでも処理を停止する
        """
        # In strict mode, stop on any medium or higher severity error
        if classification.severity in [
            ErrorSeverity.CRITICAL,
            ErrorSeverity.HIGH,
            ErrorSeverity.MEDIUM,
        ]:
            self.logger.error(
                f"Strict mode: Stopping processing due to {classification.severity.value} error: {error}"
            )
            return True

        # Stop on lower consecutive error threshold
        if consecutive_errors >= 3:
            self.logger.error(
                f"Strict mode: Stopping due to {consecutive_errors} consecutive errors"
            )
            return True

        # Stop on lower error rate threshold
        if error_rate > 0.1:  # 10% error rate
            self.logger.error(
                f"Strict mode: Stopping due to high error rate: {error_rate:.1%}"
            )
            return True

        return False

    def _tolerant_mode_should_stop(
        self,
        error: Exception,
        classification: ErrorClassification,
        consecutive_errors: int,
        error_rate: float,
    ) -> bool:
        """
        Tolerant mode: Continue processing as much as possible

        Implements requirement 7.3: WHEN 寛容モードが有効な場合 THEN システムは可能な限りエラーを無視して処理を継続する
        """
        # Only stop on critical errors
        if classification.severity == ErrorSeverity.CRITICAL:
            self.logger.error(
                f"Tolerant mode: Stopping processing due to critical error: {error}"
            )
            return True

        # High threshold for consecutive errors
        if consecutive_errors >= 20:
            self.logger.error(
                f"Tolerant mode: Stopping due to {consecutive_errors} consecutive errors"
            )
            return True

        # High threshold for error rate
        if error_rate > 0.8:  # 80% error rate
            self.logger.error(
                f"Tolerant mode: Stopping due to very high error rate: {error_rate:.1%}"
            )
            return True

        return False

    def _debug_mode_should_stop(
        self,
        error: Exception,
        classification: ErrorClassification,
        consecutive_errors: int,
        error_rate: float,
    ) -> bool:
        """
        Debug mode: Detailed logging and continue processing

        Implements requirement 7.4: WHEN デバッグモードが有効な場合 THEN システムは詳細なデバッグ情報をログに出力する
        """
        # Log detailed debug information
        self._log_debug_information(
            error, classification, consecutive_errors, error_rate
        )

        # Almost never stop in debug mode (only on critical system errors)
        if classification.severity == ErrorSeverity.CRITICAL and isinstance(
            error, (MemoryError, KeyboardInterrupt)
        ):
            self.logger.critical(
                f"Debug mode: Stopping processing due to critical system error: {error}"
            )
            return True

        # Very high threshold for consecutive errors
        if consecutive_errors >= 50:
            self.logger.error(
                f"Debug mode: Stopping due to {consecutive_errors} consecutive errors"
            )
            return True

        # Very high threshold for error rate
        if error_rate > 0.95:  # 95% error rate
            self.logger.error(
                f"Debug mode: Stopping due to extremely high error rate: {error_rate:.1%}"
            )
            return True

        return False

    def _log_debug_information(
        self,
        error: Exception,
        classification: ErrorClassification,
        consecutive_errors: int,
        error_rate: float,
    ) -> None:
        """
        Log detailed debug information

        Implements requirement 7.4 for detailed debug logging
        """
        debug_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_severity": classification.severity.value,
            "error_action": classification.action.value,
            "error_category": classification.category,
            "consecutive_errors": consecutive_errors,
            "current_error_rate": f"{error_rate:.1%}",
            "retryable": classification.retryable,
            "stack_trace": traceback.format_exc(),
        }

        self.logger.debug(f"Debug mode - Detailed error information: {debug_info}")

    def log_mode_specific_message(self, message: str, level: str = "info") -> None:
        """
        Log a message with mode-specific formatting

        Args:
            message: Message to log
            level: Log level ('debug', 'info', 'warning', 'error', 'critical')
        """
        mode_prefix = f"[{self.mode.upper()} MODE]"
        formatted_message = f"{mode_prefix} {message}"

        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(formatted_message)

    def get_mode_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current mode and its characteristics

        Returns:
            Dictionary with mode information
        """
        mode_characteristics = {
            "strict": {
                "description": "Stop on minor errors",
                "error_tolerance": "Low",
                "logging_detail": "High",
                "processing_continuation": "Limited",
            },
            "tolerant": {
                "description": "Continue processing as much as possible",
                "error_tolerance": "High",
                "logging_detail": "Standard",
                "processing_continuation": "Aggressive",
            },
            "debug": {
                "description": "Detailed logging and continue processing",
                "error_tolerance": "Very High",
                "logging_detail": "Maximum",
                "processing_continuation": "Maximum",
            },
        }

        return {
            "current_mode": self.mode,
            "characteristics": mode_characteristics.get(self.mode, {}),
            "config_settings": {
                "continue_on_batch_error": self.config.continue_on_batch_error,
                "max_consecutive_errors": self.config.max_consecutive_errors,
                "max_error_rate": self.config.max_error_rate,
                "include_stack_traces": self.config.include_stack_traces,
                "log_all_errors": self.config.log_all_errors,
            },
        }


class EnhancedErrorHandler:
    """
    Enhanced error handler with processing continuation and error classification

    Implements requirements 4.3, 4.4 for processing continuation functionality
    """

    def __init__(
        self,
        config: Optional[ProcessingConfig] = None,
        retry_manager: Optional[RetryManager] = None,
    ):
        """
        Initialize EnhancedErrorHandler

        Args:
            config: Processing configuration
            retry_manager: RetryManager instance for retry operations
        """
        self.config = config or ProcessingConfig()
        self.retry_manager = retry_manager or RetryManager()
        self.logger = logging.getLogger(__name__)
        self.enhanced_logger = EnhancedLogger(logger_name=f"{__name__}.error_handler")
        self.error_metrics = ErrorMetrics()

        # Error classification rules
        self._error_classifications = self._initialize_error_classifications()

        # Processing state
        self.consecutive_errors = 0
        self.processing_errors: List[ProcessingError] = []

        # Mode-specific processor
        self.mode_processor = ModeSpecificProcessor(self.config, self.logger)

    def _initialize_error_classifications(
        self,
    ) -> Dict[Type[Exception], ErrorClassification]:
        """Initialize default error classifications"""
        return {
            # Data not found errors - usually not critical for individual stocks
            DataNotFoundError: ErrorClassification(
                severity=(
                    ErrorSeverity.LOW
                    if self.config.treat_data_not_found_as_warning
                    else ErrorSeverity.MEDIUM
                ),
                action=ProcessingAction.SKIP_ITEM,
                retryable=False,
                description="Data not found (possibly delisted stock)",
                category="data_availability",
            ),
            # Rate limit errors - retryable but may need longer delays
            RateLimitError: ErrorClassification(
                severity=ErrorSeverity.MEDIUM,
                action=(
                    ProcessingAction.RETRY
                    if self.config.treat_rate_limit_as_retryable
                    else ProcessingAction.SKIP_ITEM
                ),
                retryable=self.config.treat_rate_limit_as_retryable,
                description="API rate limit exceeded",
                category="api_limits",
            ),
            # Network errors - usually transient and retryable
            ConnectionError: ErrorClassification(
                severity=ErrorSeverity.MEDIUM,
                action=(
                    ProcessingAction.RETRY
                    if self.config.treat_network_errors_as_retryable
                    else ProcessingAction.SKIP_ITEM
                ),
                retryable=self.config.treat_network_errors_as_retryable,
                description="Network connection error",
                category="network",
            ),
            TimeoutError: ErrorClassification(
                severity=ErrorSeverity.MEDIUM,
                action=(
                    ProcessingAction.RETRY
                    if self.config.treat_network_errors_as_retryable
                    else ProcessingAction.SKIP_ITEM
                ),
                retryable=self.config.treat_network_errors_as_retryable,
                description="Network timeout error",
                category="network",
            ),
            # API errors - may be retryable depending on the specific error
            APIError: ErrorClassification(
                severity=ErrorSeverity.MEDIUM,
                action=ProcessingAction.CONTINUE,  # Changed from SKIP_ITEM to CONTINUE so it counts as error
                retryable=True,
                description="API error",
                category="api",
            ),
            # Programming errors - critical, should not continue
            ValueError: ErrorClassification(
                severity=ErrorSeverity.CRITICAL,
                action=ProcessingAction.STOP_ALL,
                retryable=False,
                description="Invalid value error",
                category="programming",
            ),
            TypeError: ErrorClassification(
                severity=ErrorSeverity.CRITICAL,
                action=ProcessingAction.STOP_ALL,
                retryable=False,
                description="Type error",
                category="programming",
            ),
            # Memory errors - critical
            MemoryError: ErrorClassification(
                severity=ErrorSeverity.CRITICAL,
                action=ProcessingAction.STOP_ALL,
                retryable=False,
                description="Memory error",
                category="system",
            ),
            # Keyboard interrupt - critical
            KeyboardInterrupt: ErrorClassification(
                severity=ErrorSeverity.CRITICAL,
                action=ProcessingAction.STOP_ALL,
                retryable=False,
                description="User interruption",
                category="user",
            ),
        }

    def classify_error(
        self, error: Exception, context: Optional[Dict[str, Any]] = None
    ) -> ErrorClassification:
        """
        Classify an error to determine how it should be handled

        Args:
            error: The exception to classify
            context: Additional context about the error

        Returns:
            ErrorClassification with handling instructions

        Implements requirement 4.4 for error classification
        """
        error_type = type(error)

        # Check for exact type match first
        if error_type in self._error_classifications:
            return self._error_classifications[error_type]

        # Check for parent class matches
        for exc_type, classification in self._error_classifications.items():
            if isinstance(error, exc_type):
                return classification

        # Default classification for unknown errors
        return ErrorClassification(
            severity=ErrorSeverity.HIGH,
            action=ProcessingAction.SKIP_ITEM,
            retryable=False,
            description=f"Unknown error: {error_type.__name__}",
            category="unknown",
        )

    def handle_processing_error(
        self,
        error: Exception,
        operation: str,
        symbol: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> ProcessingAction:
        """
        Handle a processing error and determine the action to take

        Args:
            error: The exception that occurred
            operation: Name of the operation that failed
            symbol: Stock symbol if applicable
            context: Additional context information

        Returns:
            ProcessingAction indicating what to do next

        Implements requirement 4.3 for individual stock error handling
        """
        # Classify the error
        classification = self.classify_error(error, context)

        # Create processing error record
        processing_error = ProcessingError(
            timestamp=datetime.now(),
            operation=operation,
            symbol=symbol,
            error=error,
            classification=classification,
            context=context or {},
            stack_trace=(
                traceback.format_exc() if self.config.include_stack_traces else None
            ),
        )

        # Record the error
        self.processing_errors.append(processing_error)
        self.error_metrics.record_error(
            error_type=ErrorType.from_exception(error),
            symbol=symbol or "unknown",
            operation=operation,
            details=str(error),
            additional_info=context,
        )

        # Log the error
        self._log_processing_error(processing_error)

        # Update consecutive error count
        if classification.severity in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH]:
            self.consecutive_errors += 1
        elif classification.severity == ErrorSeverity.MEDIUM:
            # In strict mode, medium errors also count as consecutive errors
            if self.mode_processor.mode == "strict":
                self.consecutive_errors += 1
            else:
                self.consecutive_errors = (
                    0  # Reset on medium errors in tolerant/debug mode
                )
        else:
            self.consecutive_errors = 0  # Reset on low/info errors

        # Calculate current error rate
        current_error_rate = self._calculate_current_error_rate()

        # Use mode-specific processor to determine if we should stop
        if self.mode_processor.should_stop_processing(
            error, classification, self.consecutive_errors, current_error_rate
        ):
            return ProcessingAction.STOP_ALL

        # Check if we should stop processing due to too many consecutive errors (fallback)
        if self.consecutive_errors >= self.config.max_consecutive_errors:
            self.logger.error(
                f"Too many consecutive errors ({self.consecutive_errors}), stopping processing"
            )
            return ProcessingAction.STOP_ALL

        # Check error rate (fallback)
        if self._should_stop_due_to_error_rate():
            self.logger.error("Error rate too high, stopping processing")
            return ProcessingAction.STOP_ALL

        return classification.action

    def process_items_with_continuation(
        self,
        items: List[Any],
        processor_func: Callable[[Any], Any],
        operation_name: str,
        get_symbol_func: Optional[Callable[[Any], str]] = None,
    ) -> ProcessingResult:
        """
        Process a list of items with error handling and continuation

        Args:
            items: List of items to process
            processor_func: Function to process each item
            operation_name: Name of the operation for logging
            get_symbol_func: Function to extract symbol from item (optional)

        Returns:
            ProcessingResult with processing statistics

        Implements requirement 4.3 for processing continuation
        """
        start_time = datetime.now()
        processed_count = 0
        skipped_count = 0
        error_count = 0
        critical_errors = []
        non_critical_errors = []
        warnings = []

        self.logger.info(f"Starting {operation_name} for {len(items)} items")

        for i, item in enumerate(items):
            symbol = get_symbol_func(item) if get_symbol_func else f"item_{i}"

            try:
                # Process the item
                if self.config.enable_retries:
                    # Use retry manager for processing
                    retry_result = self.retry_manager.execute_with_retry(
                        processor_func, f"{operation_name}_{symbol}", item
                    )

                    if retry_result.success:
                        processed_count += 1
                        self.consecutive_errors = 0  # Reset on success
                    else:
                        # Handle the final error from retry attempts
                        action = self.handle_processing_error(
                            retry_result.final_error,
                            operation_name,
                            symbol,
                            {"retry_attempts": retry_result.total_attempts},
                        )

                        if action == ProcessingAction.STOP_ALL:
                            break
                        elif action == ProcessingAction.SKIP_ITEM:
                            skipped_count += 1
                        else:
                            error_count += 1
                else:
                    # Process without retries
                    result = processor_func(item)
                    processed_count += 1
                    self.consecutive_errors = 0  # Reset on success

            except Exception as e:
                # Handle the error
                action = self.handle_processing_error(e, operation_name, symbol)

                if action == ProcessingAction.STOP_ALL:
                    self.logger.error(
                        f"Critical error encountered, stopping processing: {e}"
                    )
                    break
                elif action == ProcessingAction.STOP_BATCH:
                    self.logger.warning(
                        f"Batch error encountered, stopping current batch: {e}"
                    )
                    break
                elif action == ProcessingAction.SKIP_ITEM:
                    skipped_count += 1
                    if self.config.log_skipped_items:
                        self.logger.warning(f"Skipping {symbol} due to error: {e}")
                else:
                    error_count += 1
                    self.logger.error(f"Error processing {symbol}: {e}")

        # Categorize errors
        for error in self.processing_errors:
            if error.classification.severity == ErrorSeverity.CRITICAL:
                critical_errors.append(error)
            else:
                non_critical_errors.append(error)

        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()

        # Create result
        result = ProcessingResult(
            success=error_count == 0 and len(critical_errors) == 0,
            processed_count=processed_count,
            skipped_count=skipped_count,
            error_count=error_count,
            critical_errors=critical_errors,
            non_critical_errors=non_critical_errors,
            warnings=warnings,
            processing_time=processing_time,
        )

        # Log processing summary
        if self.config.log_processing_summary:
            self._log_processing_summary(operation_name, result)

        # Clear processing errors for next batch
        self.processing_errors.clear()

        return result

    def _should_stop_due_to_error_rate(self) -> bool:
        """Check if processing should stop due to high error rate"""
        if len(self.processing_errors) < 10:  # Need minimum sample size
            return False

        # Calculate error rate for recent errors
        recent_errors = self.processing_errors[-20:]  # Last 20 operations
        critical_or_high_errors = sum(
            1
            for error in recent_errors
            if error.classification.severity
            in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH]
        )

        error_rate = critical_or_high_errors / len(recent_errors)
        return error_rate > self.config.max_error_rate

    def _calculate_current_error_rate(self) -> float:
        """Calculate the current error rate"""
        if len(self.processing_errors) < 5:  # Need minimum sample size
            return 0.0

        # Calculate error rate for recent errors
        recent_errors = self.processing_errors[-20:]  # Last 20 operations
        critical_or_high_errors = sum(
            1
            for error in recent_errors
            if error.classification.severity
            in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH, ErrorSeverity.MEDIUM]
        )

        return critical_or_high_errors / len(recent_errors)

    def _log_processing_error(self, processing_error: ProcessingError) -> None:
        """Log a processing error with appropriate level"""
        error = processing_error.error
        symbol = processing_error.symbol or "unknown"
        operation = processing_error.operation
        classification = processing_error.classification

        # Use mode-specific logging
        mode_message = f"Processing error in {operation} for {symbol}: {error}"

        # Use enhanced logger for detailed error logging
        if isinstance(error, DataNotFoundError):
            self.enhanced_logger.log_delisted_stock_error(
                symbol=symbol,
                operation=operation,
                error=error,
                error_indicators=["data_not_found"],
                additional_context=processing_error.context,
            )
            self.mode_processor.log_mode_specific_message(
                f"Data not found for {symbol} - {error}", "warning"
            )
        elif isinstance(error, (ConnectionError, TimeoutError)):
            # Use generic logging for network errors since log_network_error doesn't exist
            self.logger.warning(
                f"Network error in {operation} for {symbol} - {error} "
                f"(retry count: {processing_error.retry_count})"
            )
            self.mode_processor.log_mode_specific_message(
                f"Network error in {operation} for {symbol} - {error}", "warning"
            )
        else:
            # Generic error logging with mode-specific formatting
            log_level = {
                ErrorSeverity.CRITICAL: logging.CRITICAL,
                ErrorSeverity.HIGH: logging.ERROR,
                ErrorSeverity.MEDIUM: logging.WARNING,
                ErrorSeverity.LOW: logging.WARNING,
                ErrorSeverity.INFO: logging.INFO,
            }.get(classification.severity, logging.ERROR)

            self.logger.log(
                log_level,
                f"Processing error - Operation: {operation}, Symbol: {symbol}, "
                f"Error: {error}, Severity: {classification.severity.value}, "
                f"Action: {classification.action.value}",
            )

            # Add mode-specific logging
            mode_level = {
                logging.CRITICAL: "critical",
                logging.ERROR: "error",
                logging.WARNING: "warning",
                logging.INFO: "info",
            }.get(log_level, "error")

            self.mode_processor.log_mode_specific_message(mode_message, mode_level)

    def _log_processing_summary(
        self, operation_name: str, result: ProcessingResult
    ) -> None:
        """Log a summary of processing results"""
        total_items = result.processed_count + result.skipped_count + result.error_count

        # Standard summary
        summary_message = (
            f"{operation_name} completed - "
            f"Total: {total_items}, "
            f"Processed: {result.processed_count}, "
            f"Skipped: {result.skipped_count}, "
            f"Errors: {result.error_count}, "
            f"Success Rate: {result.get_success_rate():.1%}, "
            f"Time: {result.processing_time:.2f}s"
        )

        self.logger.info(summary_message)
        self.mode_processor.log_mode_specific_message(summary_message, "info")

        if result.critical_errors:
            critical_message = (
                f"Critical errors encountered: {len(result.critical_errors)}"
            )
            self.logger.error(critical_message)
            self.mode_processor.log_mode_specific_message(critical_message, "error")

        if result.non_critical_errors:
            non_critical_message = (
                f"Non-critical errors: {len(result.non_critical_errors)}"
            )
            self.logger.warning(non_critical_message)
            self.mode_processor.log_mode_specific_message(
                non_critical_message, "warning"
            )

        # Log mode summary in debug mode
        if self.mode_processor.mode == "debug":
            mode_summary = self.mode_processor.get_mode_summary()
            self.logger.debug(f"Mode summary: {mode_summary}")

    def add_error_classification(
        self, exception_type: Type[Exception], classification: ErrorClassification
    ) -> None:
        """Add or update error classification for an exception type"""
        self._error_classifications[exception_type] = classification
        self.logger.info(
            f"Added error classification for {exception_type.__name__}: "
            f"{classification.severity.value} -> {classification.action.value}"
        )

    def get_error_statistics(self) -> Dict[str, Any]:
        """Get comprehensive error statistics"""
        if not self.processing_errors:
            return {"total_errors": 0}

        # Group errors by category and severity
        by_category = {}
        by_severity = {}
        by_action = {}

        for error in self.processing_errors:
            category = error.classification.category
            severity = error.classification.severity.value
            action = error.classification.action.value

            by_category[category] = by_category.get(category, 0) + 1
            by_severity[severity] = by_severity.get(severity, 0) + 1
            by_action[action] = by_action.get(action, 0) + 1

        # Calculate time-based statistics
        now = datetime.now()
        recent_errors = [
            error
            for error in self.processing_errors
            if (now - error.timestamp).total_seconds() < 3600  # Last hour
        ]

        return {
            "total_errors": len(self.processing_errors),
            "recent_errors_1h": len(recent_errors),
            "consecutive_errors": self.consecutive_errors,
            "by_category": by_category,
            "by_severity": by_severity,
            "by_action": by_action,
            "error_rate_recent": (
                len(recent_errors) / 60 if recent_errors else 0
            ),  # Errors per minute
        }

    def get_mode_information(self) -> Dict[str, Any]:
        """Get information about the current processing mode"""
        return self.mode_processor.get_mode_summary()

    def set_mode_from_config(self, error_handling_config) -> None:
        """
        Set the processing mode from ErrorHandlingConfig

        Args:
            error_handling_config: ErrorHandlingConfig instance
        """
        # Convert ErrorHandlingConfig to ProcessingConfig
        self.config = error_handling_config.to_processing_config()

        # Recreate mode processor with new config
        self.mode_processor = ModeSpecificProcessor(self.config, self.logger)

        mode_info = self.get_mode_information()
        self.logger.info(f"Error handler mode updated: {mode_info['current_mode']}")

    def reset_error_state(self) -> None:
        """Reset error handling state"""
        self.consecutive_errors = 0
        self.processing_errors.clear()
        self.error_metrics.reset_metrics()
        self.logger.info("Error handling state reset")

    def configure_processing(
        self,
        continue_on_error: bool = True,
        max_consecutive_errors: int = 10,
        max_error_rate: float = 0.5,
        enable_retries: bool = True,
    ) -> None:
        """
        Configure processing continuation behavior

        Args:
            continue_on_error: Whether to continue processing after individual errors
            max_consecutive_errors: Maximum consecutive errors before stopping
            max_error_rate: Maximum error rate before stopping
            enable_retries: Whether to enable retry functionality
        """
        self.config.continue_on_individual_error = continue_on_error
        self.config.max_consecutive_errors = max_consecutive_errors
        self.config.max_error_rate = max_error_rate
        self.config.enable_retries = enable_retries

        self.logger.info(
            f"Processing configuration updated - Continue on error: {continue_on_error}, "
            f"Max consecutive errors: {max_consecutive_errors}, "
            f"Max error rate: {max_error_rate:.1%}, Enable retries: {enable_retries}"
        )


# Convenience functions for common error handling patterns


def create_tolerant_error_handler() -> EnhancedErrorHandler:
    """Create an error handler with tolerant settings"""
    config = ProcessingConfig(
        continue_on_individual_error=True,
        continue_on_batch_error=True,
        max_consecutive_errors=20,
        max_error_rate=0.8,
        treat_data_not_found_as_warning=True,
        enable_retries=True,
        max_retries_per_item=3,
        log_all_errors=True,
        log_skipped_items=False,  # Reduce noise in tolerant mode
        include_stack_traces=False,
    )
    return EnhancedErrorHandler(config)


def create_strict_error_handler() -> EnhancedErrorHandler:
    """Create an error handler with strict settings"""
    config = ProcessingConfig(
        continue_on_individual_error=True,
        continue_on_batch_error=False,  # Stop batch on errors in strict mode
        max_consecutive_errors=3,  # Low tolerance
        max_error_rate=0.1,  # 10% error rate threshold
        treat_data_not_found_as_warning=False,  # Treat as errors in strict mode
        enable_retries=True,
        max_retries_per_item=2,  # Fewer retries in strict mode
        log_all_errors=True,
        log_skipped_items=True,
        include_stack_traces=True,  # Detailed logging for analysis
    )
    return EnhancedErrorHandler(config)


def create_debug_error_handler() -> EnhancedErrorHandler:
    """Create an error handler with debug settings"""
    config = ProcessingConfig(
        continue_on_individual_error=True,
        continue_on_batch_error=True,
        max_consecutive_errors=50,  # Very high for debugging
        max_error_rate=0.95,  # Almost never stop
        treat_data_not_found_as_warning=True,
        enable_retries=True,
        max_retries_per_item=3,
        log_all_errors=True,
        log_skipped_items=True,
        log_processing_summary=True,
        include_stack_traces=True,  # Maximum detail for debugging
    )
    return EnhancedErrorHandler(config)


def create_error_handler_from_config(error_handling_config) -> EnhancedErrorHandler:
    """
    Create an error handler from ErrorHandlingConfig

    Args:
        error_handling_config: ErrorHandlingConfig instance

    Returns:
        EnhancedErrorHandler configured according to the mode
    """
    # Convert to ProcessingConfig
    processing_config = error_handling_config.to_processing_config()

    # Create error handler
    error_handler = EnhancedErrorHandler(processing_config)

    return error_handler
