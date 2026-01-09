"""
Tests for RetryManager class functionality
"""

import pytest
import time
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.retry_manager import (
    RetryManager,
    RetryConfig,
    RetryStrategy,
    RetryResult,
    create_api_retry_manager,
    create_network_retry_manager,
)
from src.exceptions import APIError, RateLimitError, DataNotFoundError


class TestRetryManager:
    """Test RetryManager functionality"""

    def setup_method(self):
        """Set up test environment"""
        self.config = RetryConfig(
            max_retries=3,
            base_delay=0.1,  # Short delay for tests
            max_delay=1.0,
            jitter=False,  # Disable jitter for predictable tests
        )
        self.retry_manager = RetryManager(self.config)

    def test_successful_operation_no_retry(self):
        """Test successful operation that doesn't need retry"""

        def successful_operation():
            return "success"

        result = self.retry_manager.execute_with_retry(
            successful_operation, "test_operation"
        )

        assert result.success is True
        assert result.result == "success"
        assert result.total_attempts == 1
        assert len(result.attempts) == 0  # No retry attempts

    def test_operation_succeeds_after_retries(self):
        """Test operation that succeeds after some retries"""
        call_count = 0

        def failing_then_succeeding_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise APIError("Temporary failure")
            return "success"

        result = self.retry_manager.execute_with_retry(
            failing_then_succeeding_operation, "test_operation"
        )

        assert result.success is True
        assert result.result == "success"
        assert result.total_attempts == 3
        assert len(result.attempts) == 2  # Two retry attempts

    def test_operation_fails_after_max_retries(self):
        """Test operation that fails after exhausting all retries"""

        def always_failing_operation():
            raise APIError("Persistent failure")

        result = self.retry_manager.execute_with_retry(
            always_failing_operation, "test_operation"
        )

        assert result.success is False
        assert isinstance(result.final_error, APIError)
        assert result.total_attempts == 4  # Initial + 3 retries
        assert len(result.attempts) == 3

    def test_non_retryable_error_no_retry(self):
        """Test that non-retryable errors don't trigger retries"""

        def operation_with_non_retryable_error():
            raise DataNotFoundError("Data not found")

        result = self.retry_manager.execute_with_retry(
            operation_with_non_retryable_error, "test_operation"
        )

        assert result.success is False
        assert isinstance(result.final_error, DataNotFoundError)
        assert result.total_attempts == 1  # No retries
        assert len(result.attempts) == 0

    def test_retry_statistics(self):
        """Test retry statistics collection"""

        # Successful operation
        def successful_op():
            return "success"

        # Failing operation
        def failing_op():
            raise APIError("Failure")

        # Execute operations
        self.retry_manager.execute_with_retry(successful_op, "success_op")
        self.retry_manager.execute_with_retry(failing_op, "failing_op")

        stats = self.retry_manager.get_retry_statistics()

        assert stats["total_operations"] == 2
        assert stats["successful_operations"] == 1
        assert stats["failed_operations"] == 1
        assert stats["success_rate"] == 0.5

        # Check operation-specific stats
        assert "success_op" in stats["operations"]
        assert "failing_op" in stats["operations"]
        assert stats["operations"]["success_op"]["successful"] == 1
        assert stats["operations"]["failing_op"]["failed"] == 1


class TestRetryResult:
    """Test RetryResult functionality"""

    def test_retry_result_success_rate(self):
        """Test success rate calculation"""
        success_result = RetryResult(success=True)
        failure_result = RetryResult(success=False)

        assert success_result.get_success_rate() == 1.0
        assert failure_result.get_success_rate() == 0.0

    def test_retry_result_average_delay(self):
        """Test average delay calculation"""
        from src.retry_manager import RetryAttempt

        attempts = [
            RetryAttempt(1, 1.0, Exception(), datetime.now(), "test"),
            RetryAttempt(2, 2.0, Exception(), datetime.now(), "test"),
            RetryAttempt(3, 3.0, Exception(), datetime.now(), "test"),
        ]

        result = RetryResult(success=False, attempts=attempts)
        assert result.get_average_delay() == 2.0

        # Test empty attempts
        empty_result = RetryResult(success=True)
        assert empty_result.get_average_delay() == 0.0
