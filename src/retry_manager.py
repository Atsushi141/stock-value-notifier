"""
Retry Manager module for handling retry logic with exponential backoff

This module provides comprehensive retry functionality for the stock value notifier system:
- Exponential backoff retry logic
- API rate limit handling
- Retry limit control
- Error classification for retry decisions
- Configurable retry policies
"""

import logging
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Type, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import asyncio
from functools import wraps

from .exceptions import APIError, RateLimitError, DataNotFoundError
from .enhanced_logger import EnhancedLogger


class RetryStrategy(Enum):
    """Retry strategy types"""

    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    IMMEDIATE = "immediate"


class ErrorSeverity(Enum):
    """Error severity levels for retry decisions"""

    CRITICAL = "critical"  # Don't retry
    TRANSIENT = "transient"  # Retry with normal policy
    RATE_LIMITED = "rate_limited"  # Retry with extended delay
    NETWORK = "network"  # Retry with exponential backoff


@dataclass
class RetryConfig:
    """Configuration for retry behavior"""

    max_retries: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay in seconds
    exponential_base: float = 2.0  # Exponential backoff multiplier
    jitter: bool = True  # Add random jitter to delays
    jitter_range: float = 0.1  # Jitter range (±10% by default)

    # API rate limit specific settings
    rate_limit_delay: float = 60.0  # Base delay for rate limit errors
    rate_limit_max_delay: float = 300.0  # Maximum delay for rate limits

    # Retry strategy
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF

    # Error classification
    retryable_exceptions: List[Type[Exception]] = field(
        default_factory=lambda: [
            APIError,
            RateLimitError,
            ConnectionError,
            TimeoutError,
        ]
    )
    non_retryable_exceptions: List[Type[Exception]] = field(
        default_factory=lambda: [DataNotFoundError, ValueError, TypeError]
    )

    # Logging
    log_retries: bool = True
    log_failures: bool = True


@dataclass
class RetryAttempt:
    """Information about a retry attempt"""

    attempt_number: int
    delay: float
    error: Exception
    timestamp: datetime
    operation_name: str
    total_elapsed: float = 0.0


@dataclass
class RetryResult:
    """Result of a retry operation"""

    success: bool
    result: Any = None
    final_error: Optional[Exception] = None
    attempts: List[RetryAttempt] = field(default_factory=list)
    total_attempts: int = 0
    total_elapsed: float = 0.0

    def get_success_rate(self) -> float:
        """Get success rate (1.0 for success, 0.0 for failure)"""
        return 1.0 if self.success else 0.0

    def get_average_delay(self) -> float:
        """Get average delay between attempts"""
        if not self.attempts:
            return 0.0
        return sum(attempt.delay for attempt in self.attempts) / len(self.attempts)


class RetryManager:
    """
    Manages retry logic with exponential backoff and API rate limit handling

    Implements requirements 4.1, 4.2, 4.5 for improved retry functionality
    """

    def __init__(self, config: Optional[RetryConfig] = None):
        """
        Initialize RetryManager

        Args:
            config: Retry configuration, uses defaults if None
        """
        self.config = config or RetryConfig()
        self.logger = logging.getLogger(__name__)
        self.enhanced_logger = EnhancedLogger(logger_name=f"{__name__}.retry")

        # Statistics tracking
        self.retry_stats: Dict[str, List[RetryResult]] = {}
        self.total_operations = 0
        self.successful_operations = 0
        self.failed_operations = 0

    def execute_with_retry(
        self, operation: Callable, operation_name: str, *args, **kwargs
    ) -> RetryResult:
        """
        Execute an operation with retry logic

        Args:
            operation: Function to execute
            operation_name: Name of the operation for logging
            *args, **kwargs: Arguments to pass to the operation

        Returns:
            RetryResult with operation outcome and retry information

        Implements requirement 4.1 for exponential backoff retry
        """
        start_time = datetime.now()
        attempts = []
        last_exception = None

        self.total_operations += 1

        for attempt_num in range(self.config.max_retries + 1):
            try:
                # Execute the operation
                result = operation(*args, **kwargs)

                # Success - record statistics and return
                elapsed = (datetime.now() - start_time).total_seconds()
                retry_result = RetryResult(
                    success=True,
                    result=result,
                    attempts=attempts,
                    total_attempts=attempt_num + 1,
                    total_elapsed=elapsed,
                )

                self.successful_operations += 1
                self._record_retry_result(operation_name, retry_result)

                if attempt_num > 0 and self.config.log_retries:
                    self.logger.info(
                        f"Operation '{operation_name}' succeeded after {attempt_num + 1} attempts "
                        f"(elapsed: {elapsed:.2f}s)"
                    )

                return retry_result

            except Exception as e:
                last_exception = e
                elapsed = (datetime.now() - start_time).total_seconds()

                # Check if this error should be retried
                if not self._should_retry(e, attempt_num):
                    # Don't retry - return failure immediately
                    retry_result = RetryResult(
                        success=False,
                        final_error=e,
                        attempts=attempts,
                        total_attempts=attempt_num + 1,
                        total_elapsed=elapsed,
                    )

                    self.failed_operations += 1
                    self._record_retry_result(operation_name, retry_result)

                    if self.config.log_failures:
                        self.logger.error(
                            f"Operation '{operation_name}' failed (non-retryable): {e}"
                        )

                    return retry_result

                # Calculate delay for next attempt
                if attempt_num < self.config.max_retries:
                    delay = self._calculate_delay(e, attempt_num)

                    # Record this attempt
                    attempt = RetryAttempt(
                        attempt_number=attempt_num + 1,
                        delay=delay,
                        error=e,
                        timestamp=datetime.now(),
                        operation_name=operation_name,
                        total_elapsed=elapsed,
                    )
                    attempts.append(attempt)

                    # Log retry attempt
                    if self.config.log_retries:
                        self.logger.warning(
                            f"Operation '{operation_name}' failed (attempt {attempt_num + 1}), "
                            f"retrying in {delay:.2f}s: {e}"
                        )

                    # Wait before retry
                    time.sleep(delay)
                else:
                    # Max retries reached
                    break

        # All retries exhausted - return failure
        elapsed = (datetime.now() - start_time).total_seconds()
        retry_result = RetryResult(
            success=False,
            final_error=last_exception,
            attempts=attempts,
            total_attempts=len(attempts) + 1,
            total_elapsed=elapsed,
        )

        self.failed_operations += 1
        self._record_retry_result(operation_name, retry_result)

        if self.config.log_failures:
            self.logger.error(
                f"Operation '{operation_name}' failed after {self.config.max_retries + 1} attempts "
                f"(elapsed: {elapsed:.2f}s): {last_exception}"
            )

        return retry_result

    def _should_retry(self, error: Exception, attempt_num: int) -> bool:
        """
        Determine if an error should trigger a retry

        Args:
            error: The exception that occurred
            attempt_num: Current attempt number (0-based)

        Returns:
            True if the operation should be retried

        Implements requirement 4.2 for error classification
        """
        # Check if we've reached max retries
        if attempt_num >= self.config.max_retries:
            return False

        # Check non-retryable exceptions first
        for exc_type in self.config.non_retryable_exceptions:
            if isinstance(error, exc_type):
                return False

        # Check retryable exceptions
        for exc_type in self.config.retryable_exceptions:
            if isinstance(error, exc_type):
                return True

        # Default: don't retry unknown exceptions
        return False

    def _calculate_delay(self, error: Exception, attempt_num: int) -> float:
        """
        Calculate delay before next retry attempt

        Args:
            error: The exception that occurred
            attempt_num: Current attempt number (0-based)

        Returns:
            Delay in seconds

        Implements requirement 4.2 for API rate limit handling
        """
        if isinstance(error, RateLimitError):
            # Special handling for rate limit errors
            delay = self.config.rate_limit_delay * (
                self.config.exponential_base**attempt_num
            )
            delay = min(delay, self.config.rate_limit_max_delay)
        else:
            # Standard retry delay calculation
            if self.config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
                delay = self.config.base_delay * (
                    self.config.exponential_base**attempt_num
                )
            elif self.config.strategy == RetryStrategy.LINEAR_BACKOFF:
                delay = self.config.base_delay * (attempt_num + 1)
            elif self.config.strategy == RetryStrategy.FIXED_DELAY:
                delay = self.config.base_delay
            else:  # IMMEDIATE
                delay = 0.0

            # Apply maximum delay limit
            delay = min(delay, self.config.max_delay)

        # Add jitter if enabled
        if self.config.jitter and delay > 0:
            jitter_amount = delay * self.config.jitter_range
            jitter = random.uniform(-jitter_amount, jitter_amount)
            delay = max(0.0, delay + jitter)

        return delay

    def _record_retry_result(self, operation_name: str, result: RetryResult) -> None:
        """Record retry result for statistics"""
        if operation_name not in self.retry_stats:
            self.retry_stats[operation_name] = []

        self.retry_stats[operation_name].append(result)

        # Keep only recent results (last 1000 per operation)
        if len(self.retry_stats[operation_name]) > 1000:
            self.retry_stats[operation_name] = self.retry_stats[operation_name][-1000:]

    def get_retry_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive retry statistics

        Returns:
            Dictionary with retry statistics

        Implements requirement 4.5 for retry monitoring
        """
        stats = {
            "total_operations": self.total_operations,
            "successful_operations": self.successful_operations,
            "failed_operations": self.failed_operations,
            "success_rate": (
                self.successful_operations / self.total_operations
                if self.total_operations > 0
                else 0.0
            ),
            "operations": {},
        }

        for operation_name, results in self.retry_stats.items():
            if not results:
                continue

            successful = sum(1 for r in results if r.success)
            failed = len(results) - successful
            total_attempts = sum(r.total_attempts for r in results)
            total_elapsed = sum(r.total_elapsed for r in results)

            avg_attempts = total_attempts / len(results) if results else 0
            avg_elapsed = total_elapsed / len(results) if results else 0

            # Calculate retry rate (operations that needed retries)
            operations_with_retries = sum(1 for r in results if r.total_attempts > 1)
            retry_rate = operations_with_retries / len(results) if results else 0

            stats["operations"][operation_name] = {
                "total_executions": len(results),
                "successful": successful,
                "failed": failed,
                "success_rate": successful / len(results) if results else 0.0,
                "average_attempts": avg_attempts,
                "average_elapsed_time": avg_elapsed,
                "retry_rate": retry_rate,
                "total_attempts": total_attempts,
                "total_elapsed_time": total_elapsed,
            }

        return stats

    def get_recent_failures(self, hours: int = 1) -> List[RetryResult]:
        """
        Get recent failed operations within specified time window

        Args:
            hours: Hours to look back

        Returns:
            List of failed RetryResult objects
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_failures = []

        for results in self.retry_stats.values():
            for result in results:
                if not result.success and result.attempts:
                    # Check if any attempt was within the time window
                    recent_attempts = [
                        attempt
                        for attempt in result.attempts
                        if attempt.timestamp >= cutoff_time
                    ]
                    if recent_attempts:
                        recent_failures.append(result)

        return recent_failures

    def reset_statistics(self) -> None:
        """Reset all retry statistics"""
        self.retry_stats.clear()
        self.total_operations = 0
        self.successful_operations = 0
        self.failed_operations = 0

        self.logger.info("Retry statistics reset")

    def configure_retry_policy(
        self,
        max_retries: Optional[int] = None,
        base_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
        strategy: Optional[RetryStrategy] = None,
        rate_limit_delay: Optional[float] = None,
    ) -> None:
        """
        Update retry policy configuration

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries
            max_delay: Maximum delay between retries
            strategy: Retry strategy to use
            rate_limit_delay: Base delay for rate limit errors
        """
        if max_retries is not None:
            self.config.max_retries = max_retries
        if base_delay is not None:
            self.config.base_delay = base_delay
        if max_delay is not None:
            self.config.max_delay = max_delay
        if strategy is not None:
            self.config.strategy = strategy
        if rate_limit_delay is not None:
            self.config.rate_limit_delay = rate_limit_delay

        self.logger.info(
            f"Retry policy updated - Max retries: {self.config.max_retries}, "
            f"Base delay: {self.config.base_delay}s, Strategy: {self.config.strategy.value}"
        )

    def add_retryable_exception(self, exception_type: Type[Exception]) -> None:
        """Add an exception type to the retryable list"""
        if exception_type not in self.config.retryable_exceptions:
            self.config.retryable_exceptions.append(exception_type)
            self.logger.info(f"Added {exception_type.__name__} to retryable exceptions")

    def add_non_retryable_exception(self, exception_type: Type[Exception]) -> None:
        """Add an exception type to the non-retryable list"""
        if exception_type not in self.config.non_retryable_exceptions:
            self.config.non_retryable_exceptions.append(exception_type)
            self.logger.info(
                f"Added {exception_type.__name__} to non-retryable exceptions"
            )

    def create_retry_decorator(self, operation_name: str, **retry_kwargs):
        """
        Create a decorator for automatic retry functionality

        Args:
            operation_name: Name for the operation (for logging/stats)
            **retry_kwargs: Additional arguments to pass to execute_with_retry

        Returns:
            Decorator function
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                result = self.execute_with_retry(func, operation_name, *args, **kwargs)
                if result.success:
                    return result.result
                else:
                    # Re-raise the final error
                    raise result.final_error

            return wrapper

        return decorator


# Convenience functions for common retry patterns


def create_api_retry_manager() -> RetryManager:
    """Create a RetryManager configured for API operations"""
    config = RetryConfig(
        max_retries=3,
        base_delay=1.0,
        max_delay=30.0,
        rate_limit_delay=60.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        jitter=True,
    )
    return RetryManager(config)


def create_network_retry_manager() -> RetryManager:
    """Create a RetryManager configured for network operations"""
    config = RetryConfig(
        max_retries=5,
        base_delay=0.5,
        max_delay=10.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        jitter=True,
        retryable_exceptions=[ConnectionError, TimeoutError, APIError, RateLimitError],
    )
    return RetryManager(config)


def create_conservative_retry_manager() -> RetryManager:
    """Create a RetryManager with conservative retry settings"""
    config = RetryConfig(
        max_retries=2,
        base_delay=2.0,
        max_delay=60.0,
        rate_limit_delay=120.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        jitter=False,
    )
    return RetryManager(config)
