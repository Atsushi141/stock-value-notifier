"""
Tests for SymbolFilter functionality.

Tests the comprehensive symbol filtering system including:
- Symbol filtering with different modes
- Pre-filtering functionality
- Empty list detection and alerting
- Symbol list update logging
- Integration with SymbolValidator and ErrorMetrics
"""

import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from src.symbol_filter import (
    SymbolFilter,
    FilteringMode,
    FilteringResult,
    FilteringStats,
)
from src.symbol_validator import SymbolValidator, ValidationResult, ValidationStatus
from src.error_metrics import ErrorMetrics, ErrorType, AlertLevel


class TestSymbolFilter:
    """Test SymbolFilter functionality."""

    def setup_method(self):
        """Set up test environment."""
        # Suppress logging during tests
        logging.getLogger().setLevel(logging.CRITICAL)

    def test_symbol_filter_initialization(self):
        """Test SymbolFilter initialization with default parameters."""
        symbol_filter = SymbolFilter()

        assert symbol_filter.filtering_mode == FilteringMode.TOLERANT
        assert symbol_filter.high_filter_rate_threshold == 0.3
        assert symbol_filter.empty_list_alert is True
        assert isinstance(symbol_filter.symbol_validator, SymbolValidator)
        assert isinstance(symbol_filter.error_metrics, ErrorMetrics)

    def test_symbol_filter_initialization_with_custom_params(self):
        """Test SymbolFilter initialization with custom parameters."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator,
            error_metrics=mock_metrics,
            filtering_mode=FilteringMode.STRICT,
            high_filter_rate_threshold=0.2,
            empty_list_alert=False,
        )

        assert symbol_filter.filtering_mode == FilteringMode.STRICT
        assert symbol_filter.high_filter_rate_threshold == 0.2
        assert symbol_filter.empty_list_alert is False
        assert symbol_filter.symbol_validator is mock_validator
        assert symbol_filter.error_metrics is mock_metrics

    def test_filter_symbols_basic_functionality(self):
        """Test basic symbol filtering functionality."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        # Mock validation results
        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
            "1423": ValidationResult(
                symbol="1423.T", status=ValidationStatus.DELISTED, is_valid=False
            ),
            "6758": ValidationResult(
                symbol="6758.T", status=ValidationStatus.VALID, is_valid=True
            ),
            "9999": ValidationResult(
                symbol="9999.T", status=ValidationStatus.INVALID, is_valid=False
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator,
            error_metrics=mock_metrics,
            filtering_mode=FilteringMode.TOLERANT,
        )

        symbols = ["7203", "1423", "6758", "9999"]
        result = symbol_filter.filter_symbols(symbols)

        # Verify results
        assert isinstance(result, FilteringResult)
        assert result.original_symbols == symbols
        assert len(result.valid_symbols) == 2
        assert "7203" in result.valid_symbols
        assert "6758" in result.valid_symbols
        assert len(result.delisted_symbols) == 1
        assert "1423" in result.delisted_symbols
        assert len(result.invalid_symbols) == 1
        assert "9999" in result.invalid_symbols
        assert result.filtering_mode == FilteringMode.TOLERANT

        # Verify validator was called
        mock_validator.batch_validate_symbols.assert_called_once_with(symbols)

    def test_filter_symbols_strict_mode(self):
        """Test symbol filtering in strict mode."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        # Mock validation results with error status
        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
            "1423": ValidationResult(
                symbol="1423.T", status=ValidationStatus.DELISTED, is_valid=False
            ),
            "6758": ValidationResult(
                symbol="6758.T", status=ValidationStatus.ERROR, is_valid=False
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator,
            error_metrics=mock_metrics,
            filtering_mode=FilteringMode.STRICT,
        )

        symbols = ["7203", "1423", "6758"]
        result = symbol_filter.filter_symbols(
            symbols, filtering_mode=FilteringMode.STRICT
        )

        # In strict mode, only VALID symbols should be included
        assert len(result.valid_symbols) == 1
        assert "7203" in result.valid_symbols
        assert len(result.filtered_symbols) == 2
        assert "1423" in result.delisted_symbols
        assert "6758" in result.error_symbols

    def test_filter_symbols_permissive_mode(self):
        """Test symbol filtering in permissive mode."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        # Mock validation results
        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
            "1423": ValidationResult(
                symbol="1423.T", status=ValidationStatus.DELISTED, is_valid=False
            ),
            "6758": ValidationResult(
                symbol="6758.T", status=ValidationStatus.ERROR, is_valid=False
            ),
            "9999": ValidationResult(
                symbol="9999.T", status=ValidationStatus.INVALID, is_valid=False
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator,
            error_metrics=mock_metrics,
            filtering_mode=FilteringMode.PERMISSIVE,
        )

        symbols = ["7203", "1423", "6758", "9999"]
        result = symbol_filter.filter_symbols(
            symbols, filtering_mode=FilteringMode.PERMISSIVE
        )

        # In permissive mode, only DELISTED symbols should be filtered
        assert len(result.valid_symbols) == 3
        assert "7203" in result.valid_symbols
        assert "6758" in result.valid_symbols  # ERROR status included in permissive
        assert "9999" in result.valid_symbols  # INVALID status included in permissive
        assert len(result.delisted_symbols) == 1
        assert "1423" in result.delisted_symbols

    def test_pre_filter_symbol_list(self):
        """Test pre-filtering functionality with update logging."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        # Mock validation results
        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
            "1423": ValidationResult(
                symbol="1423.T", status=ValidationStatus.DELISTED, is_valid=False
            ),
            "6758": ValidationResult(
                symbol="6758.T", status=ValidationStatus.VALID, is_valid=True
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator, error_metrics=mock_metrics
        )

        symbols = ["7203", "1423", "6758"]
        valid_symbols = symbol_filter.pre_filter_symbol_list(
            symbols, operation_name="test_operation"
        )

        # Verify results
        assert len(valid_symbols) == 2
        assert "7203" in valid_symbols
        assert "6758" in valid_symbols
        assert "1423" not in valid_symbols

        # Verify validator was called
        mock_validator.batch_validate_symbols.assert_called_once_with(symbols)

    def test_empty_list_detection_and_alert(self):
        """Test empty list detection and alerting functionality."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator,
            error_metrics=mock_metrics,
            empty_list_alert=True,
        )

        # Test with empty list
        is_empty = symbol_filter.validate_and_alert_empty_list(
            [], operation_name="test_operation"
        )

        assert is_empty is True

        # Verify error was recorded in metrics
        mock_metrics.record_error.assert_called_once()
        call_args = mock_metrics.record_error.call_args
        assert call_args[1]["error_type"] == ErrorType.DATA_VALIDATION
        assert call_args[1]["symbol"] == "ALL_SYMBOLS"
        assert call_args[1]["severity"] == AlertLevel.CRITICAL

        # Test with non-empty list
        mock_metrics.reset_mock()
        is_empty = symbol_filter.validate_and_alert_empty_list(
            ["7203"], operation_name="test_operation"
        )

        assert is_empty is False
        mock_metrics.record_error.assert_not_called()

    def test_high_filter_rate_alert(self):
        """Test high filter rate alerting functionality."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        # Mock validation results with high filter rate (2 out of 4 filtered in tolerant mode)
        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
            "1423": ValidationResult(
                symbol="1423.T", status=ValidationStatus.DELISTED, is_valid=False
            ),
            "6758": ValidationResult(
                symbol="6758.T", status=ValidationStatus.INVALID, is_valid=False
            ),
            "9999": ValidationResult(
                symbol="9999.T", status=ValidationStatus.ERROR, is_valid=False
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator,
            error_metrics=mock_metrics,
            high_filter_rate_threshold=0.4,  # 40% threshold
        )

        symbols = ["7203", "1423", "6758", "9999"]
        result = symbol_filter.filter_symbols(symbols)

        # Filter rate should be 50% (2/4) in tolerant mode (DELISTED and INVALID filtered, ERROR included)
        assert result.filter_rate == 0.5

        # Verify high filter rate alert was recorded
        mock_metrics.record_error.assert_called()
        # Find the high filter rate alert call
        alert_calls = [
            call
            for call in mock_metrics.record_error.call_args_list
            if call[1].get("additional_info", {}).get("alert_type")
            == "high_filter_rate"
        ]
        assert len(alert_calls) > 0

    def test_filtering_statistics(self):
        """Test filtering statistics collection."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator, error_metrics=mock_metrics
        )

        # Mock validation results
        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
            "1423": ValidationResult(
                symbol="1423.T", status=ValidationStatus.DELISTED, is_valid=False
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        # Perform filtering
        symbols = ["7203", "1423"]
        result = symbol_filter.filter_symbols(symbols)

        # Get statistics
        stats = symbol_filter.get_filtering_statistics()

        # Verify statistics structure
        assert "overall_stats" in stats
        assert "breakdown_stats" in stats
        assert "recent_stats" in stats
        assert "alert_stats" in stats
        assert "configuration" in stats

        # Verify some basic statistics
        assert stats["overall_stats"]["total_operations"] == 1
        assert stats["overall_stats"]["total_symbols_processed"] == 2
        assert stats["overall_stats"]["total_valid_symbols"] == 1
        assert stats["breakdown_stats"]["delisted_count"] == 1

    def test_filtering_cache(self):
        """Test filtering result caching functionality."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        # Mock validation results
        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator,
            error_metrics=mock_metrics,
            cache_duration=timedelta(minutes=30),
        )

        symbols = ["7203"]

        # First call should hit the validator
        result1 = symbol_filter.filter_symbols(symbols)
        assert mock_validator.batch_validate_symbols.call_count == 1

        # Second call should use cache
        result2 = symbol_filter.filter_symbols(symbols)
        assert (
            mock_validator.batch_validate_symbols.call_count == 1
        )  # No additional calls

        # Results should be identical
        assert result1.valid_symbols == result2.valid_symbols
        assert result1.filtering_mode == result2.filtering_mode

    def test_configure_filtering(self):
        """Test filtering configuration updates."""
        symbol_filter = SymbolFilter()

        # Test configuration updates
        symbol_filter.configure_filtering(
            filtering_mode=FilteringMode.STRICT,
            high_filter_rate_threshold=0.2,
            empty_list_alert=False,
        )

        assert symbol_filter.filtering_mode == FilteringMode.STRICT
        assert symbol_filter.high_filter_rate_threshold == 0.2
        assert symbol_filter.empty_list_alert is False

    def test_clear_cache(self):
        """Test cache clearing functionality."""
        mock_validator = Mock(spec=SymbolValidator)
        mock_metrics = Mock(spec=ErrorMetrics)

        validation_results = {
            "7203": ValidationResult(
                symbol="7203.T", status=ValidationStatus.VALID, is_valid=True
            ),
        }

        mock_validator.batch_validate_symbols.return_value = validation_results

        symbol_filter = SymbolFilter(
            symbol_validator=mock_validator, error_metrics=mock_metrics
        )

        # Add something to cache
        symbols = ["7203"]
        symbol_filter.filter_symbols(symbols)
        assert len(symbol_filter.filtering_cache) > 0

        # Clear cache
        symbol_filter.clear_cache()
        assert len(symbol_filter.filtering_cache) == 0

    def test_filtering_result_properties(self):
        """Test FilteringResult property calculations."""
        result = FilteringResult(
            original_symbols=["A", "B", "C", "D"],
            valid_symbols=["A", "B"],
            filtered_symbols=["C", "D"],
            delisted_symbols=["C"],
            invalid_symbols=["D"],
            error_symbols=[],
            filtering_mode=FilteringMode.TOLERANT,
            processing_time=1.5,
        )

        assert result.filter_rate == 0.5  # 2 out of 4 filtered
        assert result.success_rate == 0.5  # 2 out of 4 valid

        # Test empty list case
        empty_result = FilteringResult(
            original_symbols=[],
            valid_symbols=[],
            filtered_symbols=[],
            delisted_symbols=[],
            invalid_symbols=[],
            error_symbols=[],
            filtering_mode=FilteringMode.TOLERANT,
            processing_time=0.0,
        )

        assert empty_result.filter_rate == 0.0
        assert empty_result.success_rate == 0.0

    def test_filtering_stats_update(self):
        """Test FilteringStats update functionality."""
        stats = FilteringStats()

        result = FilteringResult(
            original_symbols=["A", "B", "C"],
            valid_symbols=["A"],
            filtered_symbols=["B", "C"],
            delisted_symbols=["B"],
            invalid_symbols=["C"],
            error_symbols=[],
            filtering_mode=FilteringMode.TOLERANT,
            processing_time=2.0,
        )

        stats.update_from_result(result)

        assert stats.total_filtering_operations == 1
        assert stats.total_symbols_processed == 3
        assert stats.total_symbols_filtered == 2
        assert stats.total_valid_symbols == 1
        assert stats.delisted_count == 1
        assert stats.invalid_count == 1
        assert stats.error_count == 0
        assert stats.total_processing_time == 2.0
        assert stats.average_processing_time == 2.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
