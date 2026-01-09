"""
Tests for EnhancedErrorHandler class functionality
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from src.error_handler import (
    EnhancedErrorHandler,
    ProcessingConfig,
    ErrorSeverity,
    ProcessingAction,
    ErrorClassification,
    ProcessingError,
    ProcessingResult,
    create_tolerant_error_handler,
    create_strict_error_handler,
)
from src.exceptions import APIError, RateLimitError, DataNotFoundError
from src.retry_manager import RetryManager, RetryConfig


class TestEnhancedErrorHandler:
    """Test EnhancedErrorHandler functionality"""

    def setup_method(self):
        """Set up test environment"""
        self.config = ProcessingConfig(
            continue_on_individual_error=True,
            max_consecutive_errors=5,
            max_error_rate=0.5,
            log_all_errors=False,  # Reduce noise in tests
        )
        self.error_handler = EnhancedErrorHandler(self.config)

    def test_classify_data_not_found_error(self):
        """Test classification of DataNotFoundError"""
        error = DataNotFoundError("Stock not found")
        classification = self.error_handler.classify_error(error)

        assert classification.severity == ErrorSeverity.LOW
        assert classification.action == ProcessingAction.SKIP_ITEM
        assert classification.retryable is False
        assert "data_availability" in classification.category

    def test_classify_rate_limit_error(self):
        """Test classification of RateLimitError"""
        error = RateLimitError("Rate limit exceeded")
        classification = self.error_handler.classify_error(error)

        assert classification.severity == ErrorSeverity.MEDIUM
        assert classification.action == ProcessingAction.RETRY
        assert classification.retryable is True

    def test_classify_critical_error(self):
        """Test classification of critical errors"""
        error = ValueError("Invalid value")
        classification = self.error_handler.classify_error(error)

        assert classification.severity == ErrorSeverity.CRITICAL
        assert classification.action == ProcessingAction.STOP_ALL
        assert classification.retryable is False

    def test_handle_processing_error_skip_item(self):
        """Test handling error that should skip item"""
        error = DataNotFoundError("Stock not found")
        action = self.error_handler.handle_processing_error(
            error, "test_operation", "TEST.T"
        )

        assert action == ProcessingAction.SKIP_ITEM
        assert len(self.error_handler.processing_errors) == 1
        assert (
            self.error_handler.consecutive_errors == 0
        )  # Low severity doesn't increment

    def test_handle_processing_error_stop_all(self):
        """Test handling critical error that should stop all processing"""
        error = ValueError("Critical error")
        action = self.error_handler.handle_processing_error(
            error, "test_operation", "TEST.T"
        )

        assert action == ProcessingAction.STOP_ALL
        assert len(self.error_handler.processing_errors) == 1
        assert self.error_handler.consecutive_errors == 1

    def test_consecutive_error_limit(self):
        """Test that consecutive errors trigger stop condition"""
        # Generate consecutive high-severity errors
        for i in range(self.config.max_consecutive_errors):
            error = APIError(f"Error {i}")
            action = self.error_handler.handle_processing_error(
                error, "test_operation", f"TEST{i}.T"
            )

            if i < self.config.max_consecutive_errors - 1:
                assert (
                    action == ProcessingAction.CONTINUE
                )  # Updated to match new APIError classification
            else:
                assert action == ProcessingAction.STOP_ALL

    def test_process_items_with_continuation_success(self):
        """Test successful processing of all items"""
        items = ["item1", "item2", "item3"]

        def successful_processor(item):
            return f"processed_{item}"

        result = self.error_handler.process_items_with_continuation(
            items, successful_processor, "test_operation"
        )

        assert result.success is True
        assert result.processed_count == 3
        assert result.skipped_count == 0
        assert result.error_count == 0
        assert len(result.critical_errors) == 0

    def test_process_items_with_continuation_mixed_results(self):
        """Test processing with mixed success and failures"""
        items = ["success1", "fail", "success2", "skip"]

        def mixed_processor(item):
            if item == "fail":
                raise APIError("Processing failed")
            elif item == "skip":
                raise DataNotFoundError("Data not found")
            return f"processed_{item}"

        result = self.error_handler.process_items_with_continuation(
            items, mixed_processor, "test_operation"
        )

        assert result.processed_count == 2  # success1, success2
        assert result.skipped_count == 1  # skip (DataNotFoundError)
        assert result.error_count == 1  # fail (APIError)
        assert result.get_success_rate() == 0.5  # 2 out of 4

    def test_process_items_with_continuation_critical_error_stops(self):
        """Test that critical error stops processing"""
        items = ["item1", "critical", "item3"]

        def processor_with_critical_error(item):
            if item == "critical":
                raise ValueError("Critical error")
            return f"processed_{item}"

        result = self.error_handler.process_items_with_continuation(
            items, processor_with_critical_error, "test_operation"
        )

        assert result.processed_count == 1  # Only item1 processed
        assert result.skipped_count == 0
        assert result.error_count == 0
        assert len(result.critical_errors) == 1

    def test_process_items_with_retry_manager(self):
        """Test processing with retry manager integration"""
        # Create config with retries enabled
        config = ProcessingConfig(enable_retries=True)
        retry_config = RetryConfig(max_retries=2, base_delay=0.01)
        retry_manager = RetryManager(retry_config)
        error_handler = EnhancedErrorHandler(config, retry_manager)

        items = ["success", "retry_then_success", "always_fail"]
        call_counts = {"retry_then_success": 0, "always_fail": 0}

        def processor_with_retries(item):
            if item == "retry_then_success":
                call_counts[item] += 1
                if call_counts[item] < 2:
                    raise APIError("Temporary failure")
                return f"processed_{item}"
            elif item == "always_fail":
                call_counts[item] += 1
                raise APIError("Persistent failure")
            return f"processed_{item}"

        result = error_handler.process_items_with_continuation(
            items, processor_with_retries, "test_operation"
        )

        assert result.processed_count == 2  # success, retry_then_success
        assert call_counts["retry_then_success"] == 2  # Retried once
        assert call_counts["always_fail"] >= 2  # Retried but still failed

    def test_error_statistics(self):
        """Test error statistics collection"""
        # Generate various errors
        errors = [
            DataNotFoundError("Not found"),
            APIError("API error"),
            RateLimitError("Rate limited"),
            ConnectionError("Network error"),
        ]

        for i, error in enumerate(errors):
            self.error_handler.handle_processing_error(
                error, "test_operation", f"TEST{i}.T"
            )

        stats = self.error_handler.get_error_statistics()

        assert stats["total_errors"] == 4
        assert "data_availability" in stats["by_category"]
        assert "api" in stats["by_category"]
        assert "skip_item" in stats["by_action"]

    def test_configure_processing(self):
        """Test processing configuration updates"""
        self.error_handler.configure_processing(
            continue_on_error=False,
            max_consecutive_errors=20,
            max_error_rate=0.8,
            enable_retries=False,
        )

        assert self.error_handler.config.continue_on_individual_error is False
        assert self.error_handler.config.max_consecutive_errors == 20
        assert self.error_handler.config.max_error_rate == 0.8
        assert self.error_handler.config.enable_retries is False

    def test_add_custom_error_classification(self):
        """Test adding custom error classification"""
        # Add custom classification for RuntimeError
        custom_classification = ErrorClassification(
            severity=ErrorSeverity.HIGH,
            action=ProcessingAction.STOP_BATCH,
            retryable=True,
            description="Custom runtime error",
            category="custom",
        )

        self.error_handler.add_error_classification(RuntimeError, custom_classification)

        # Test the custom classification
        error = RuntimeError("Custom error")
        classification = self.error_handler.classify_error(error)

        assert classification.severity == ErrorSeverity.HIGH
        assert classification.action == ProcessingAction.STOP_BATCH
        assert classification.category == "custom"

    def test_reset_error_state(self):
        """Test resetting error state"""
        # Generate some errors - use ValueError which is classified as CRITICAL
        error = ValueError("Test error")
        self.error_handler.handle_processing_error(error, "test_op", "TEST.T")

        assert len(self.error_handler.processing_errors) > 0
        assert self.error_handler.consecutive_errors > 0

        # Reset state
        self.error_handler.reset_error_state()

        assert len(self.error_handler.processing_errors) == 0
        assert self.error_handler.consecutive_errors == 0


class TestProcessingResult:
    """Test ProcessingResult functionality"""

    def test_success_rate_calculation(self):
        """Test success rate calculation"""
        result = ProcessingResult(
            success=True, processed_count=8, skipped_count=1, error_count=1
        )

        assert result.get_success_rate() == 0.8  # 8 out of 10

    def test_error_rate_calculation(self):
        """Test error rate calculation"""
        result = ProcessingResult(
            success=False, processed_count=7, skipped_count=1, error_count=2
        )

        assert result.get_error_rate() == 0.2  # 2 out of 10

    def test_has_critical_errors(self):
        """Test critical error detection"""
        from src.error_handler import ProcessingError

        critical_error = ProcessingError(
            timestamp=datetime.now(),
            operation="test",
            symbol="TEST.T",
            error=ValueError("Critical"),
            classification=ErrorClassification(
                severity=ErrorSeverity.CRITICAL,
                action=ProcessingAction.STOP_ALL,
                retryable=False,
                description="Critical error",
                category="programming",
            ),
        )

        result_with_critical = ProcessingResult(
            success=False,
            processed_count=0,
            skipped_count=0,
            error_count=1,
            critical_errors=[critical_error],
        )

        result_without_critical = ProcessingResult(
            success=True, processed_count=5, skipped_count=0, error_count=0
        )

        assert result_with_critical.has_critical_errors() is True
        assert result_without_critical.has_critical_errors() is False


class TestErrorHandlerConvenienceFunctions:
    """Test convenience functions for creating ErrorHandler instances"""

    def test_create_tolerant_error_handler(self):
        """Test tolerant error handler creation"""
        handler = create_tolerant_error_handler()

        assert handler.config.continue_on_individual_error is True
        assert handler.config.max_consecutive_errors == 20
        assert handler.config.max_error_rate == 0.8
        assert handler.config.treat_data_not_found_as_warning is True

    def test_create_strict_error_handler(self):
        """Test strict error handler creation"""
        handler = create_strict_error_handler()

        assert handler.config.continue_on_individual_error is True
        assert handler.config.continue_on_batch_error is False
        assert handler.config.max_consecutive_errors == 3  # Updated for strict mode
        assert handler.config.max_error_rate == 0.1  # Updated for strict mode
        assert handler.config.treat_data_not_found_as_warning is False
