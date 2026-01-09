"""
Tests for data validation functionality.

Tests the DataValidator and ValidationErrorProcessor classes to ensure
proper validation and error handling for financial, price, and dividend data.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from data_validator import (
    DataValidator,
    ValidationConfig,
    DataValidationResult,
    ValidationStatus,
)
from validation_error_processor import (
    ValidationErrorProcessor,
    ProcessingConfig,
    ValidationErrorSummary,
)


class TestDataValidator:
    """Test cases for DataValidator class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = DataValidator()
        self.test_symbol = "7203.T"

    def test_validate_financial_data_valid(self):
        """Test validation of valid financial data."""
        valid_data = {
            "symbol": "7203.T",
            "shortName": "Toyota Motor Corp",
            "currentPrice": 2500.0,
            "trailingPE": 12.5,
            "priceToBook": 1.2,
            "marketCap": 30000000000,
            "dividendYield": 0.025,
        }

        result = self.validator.validate_financial_data(self.test_symbol, valid_data)

        assert result.is_valid
        assert result.status == ValidationStatus.VALID
        assert len(result.errors) == 0
        assert result.quality_score > 0.9

    def test_validate_financial_data_missing_essential(self):
        """Test validation with missing essential fields."""
        invalid_data = {
            "symbol": "7203.T",
            # Missing currentPrice and shortName
            "trailingPE": 12.5,
        }

        result = self.validator.validate_financial_data(self.test_symbol, invalid_data)

        assert not result.is_valid
        assert result.status == ValidationStatus.INVALID
        assert len(result.errors) > 0
        assert "currentPrice" in str(result.errors)

    def test_validate_financial_data_warnings(self):
        """Test validation that generates warnings."""
        warning_data = {
            "symbol": "7203.T",
            "shortName": "Toyota Motor Corp",
            "currentPrice": 2500.0,
            "trailingPE": 150.0,  # Very high PER - should generate warning
            "priceToBook": 0.5,
        }

        result = self.validator.validate_financial_data(self.test_symbol, warning_data)

        assert result.is_valid  # Still valid with warnings
        assert result.status == ValidationStatus.WARNING
        assert len(result.warnings) > 0
        assert any("high PER" in warning.lower() for warning in result.warnings)

    def test_validate_price_data_valid(self):
        """Test validation of valid price data."""
        # Create valid price data
        dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
        valid_price_data = pd.DataFrame(
            {
                "Date": dates,
                "Open": [2400 + i for i in range(len(dates))],
                "High": [2450 + i for i in range(len(dates))],
                "Low": [2350 + i for i in range(len(dates))],
                "Close": [2400 + i for i in range(len(dates))],
                "Volume": [1000000 + i * 1000 for i in range(len(dates))],
            }
        )

        result = self.validator.validate_price_data(self.test_symbol, valid_price_data)

        assert result.is_valid
        assert result.status == ValidationStatus.VALID
        assert len(result.errors) == 0
        assert result.additional_info["record_count"] == len(dates)

    def test_validate_price_data_empty(self):
        """Test validation of empty price data."""
        empty_data = pd.DataFrame()

        result = self.validator.validate_price_data(self.test_symbol, empty_data)

        assert not result.is_valid
        assert result.status == ValidationStatus.INVALID
        assert "empty" in str(result.errors).lower()

    def test_validate_price_data_missing_columns(self):
        """Test validation with missing required columns."""
        incomplete_data = pd.DataFrame(
            {
                "Date": pd.date_range(start="2023-01-01", periods=100),
                "Close": [2400] * 100,
                # Missing Open, High, Low, Volume
            }
        )

        result = self.validator.validate_price_data(self.test_symbol, incomplete_data)

        assert not result.is_valid
        assert result.status == ValidationStatus.INVALID
        assert "missing required columns" in str(result.errors).lower()

    def test_validate_dividend_data_valid(self):
        """Test validation of valid dividend data."""
        valid_dividend_data = pd.DataFrame(
            {
                "Date": pd.date_range(start="2023-01-01", periods=4, freq="Q"),
                "Dividends": [25.0, 25.0, 25.0, 30.0],
            }
        )

        result = self.validator.validate_dividend_data(
            self.test_symbol, valid_dividend_data
        )

        assert result.is_valid
        assert result.status == ValidationStatus.VALID
        assert result.additional_info["dividend_paying"] is True
        assert result.additional_info["record_count"] == 4

    def test_validate_dividend_data_empty(self):
        """Test validation of empty dividend data (should be valid)."""
        empty_data = pd.DataFrame()

        result = self.validator.validate_dividend_data(self.test_symbol, empty_data)

        assert result.is_valid  # Empty dividend data is acceptable
        assert result.status == ValidationStatus.VALID
        assert result.additional_info["dividend_paying"] is False

    def test_validate_dividend_data_negative_dividends(self):
        """Test validation with negative dividends."""
        invalid_dividend_data = pd.DataFrame(
            {
                "Date": pd.date_range(start="2023-01-01", periods=2, freq="Q"),
                "Dividends": [25.0, -10.0],  # Negative dividend
            }
        )

        result = self.validator.validate_dividend_data(
            self.test_symbol, invalid_dividend_data
        )

        assert not result.is_valid
        assert result.status == ValidationStatus.INVALID
        assert "negative dividends" in str(result.errors).lower()

    def test_check_data_completeness_dict(self):
        """Test data completeness check for dictionary data."""
        complete_data = {"field1": "value1", "field2": "value2", "field3": "value3"}
        incomplete_data = {"field1": "value1", "field2": None, "field3": "value3"}

        assert self.validator.check_data_completeness(
            complete_data, ["field1", "field2"]
        )
        assert not self.validator.check_data_completeness(
            incomplete_data, ["field1", "field2"]
        )
        assert self.validator.check_data_completeness(
            incomplete_data, ["field1", "field3"]
        )

    def test_validation_statistics(self):
        """Test validation statistics tracking."""
        # Reset statistics
        self.validator.reset_statistics()

        # Perform some validations
        valid_data = {"symbol": "7203.T", "shortName": "Toyota", "currentPrice": 2500.0}
        invalid_data = {}

        self.validator.validate_financial_data("7203.T", valid_data)
        self.validator.validate_financial_data("1234.T", invalid_data)

        stats = self.validator.get_validation_statistics()

        assert stats["total_validations"] == 2
        assert stats["valid_count"] == 1
        assert stats["invalid_count"] == 1
        assert stats["success_rate"] == 0.5


class TestValidationErrorProcessor:
    """Test cases for ValidationErrorProcessor class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.processor = ValidationErrorProcessor()

    def test_process_financial_data_batch_valid(self):
        """Test processing batch of valid financial data."""
        valid_batch = [
            (
                "7203.T",
                {"symbol": "7203.T", "shortName": "Toyota", "currentPrice": 2500.0},
            ),
            (
                "6758.T",
                {"symbol": "6758.T", "shortName": "Sony", "currentPrice": 12000.0},
            ),
        ]

        valid_data, summary = self.processor.process_financial_data_batch(valid_batch)

        assert len(valid_data) == 2
        assert summary.total_processed == 2
        assert summary.successful_validations == 2
        assert summary.validation_errors == 0
        assert summary.get_success_rate() == 100.0

    def test_process_financial_data_batch_mixed(self):
        """Test processing batch with mixed valid/invalid data."""
        mixed_batch = [
            (
                "7203.T",
                {"symbol": "7203.T", "shortName": "Toyota", "currentPrice": 2500.0},
            ),
            ("INVALID.T", {}),  # Invalid - empty data
            (
                "6758.T",
                {"symbol": "6758.T", "shortName": "Sony", "currentPrice": 12000.0},
            ),
        ]

        valid_data, summary = self.processor.process_financial_data_batch(mixed_batch)

        assert len(valid_data) == 2  # Only valid items
        assert summary.total_processed == 3
        assert summary.successful_validations == 2
        assert summary.validation_errors == 1
        assert summary.skipped_items == 1
        assert "INVALID.T" in summary.skipped_symbols

    def test_validate_and_filter_data_comprehensive(self):
        """Test comprehensive data validation and filtering."""
        # Valid financial data
        financial_data = {
            "symbol": "7203.T",
            "shortName": "Toyota",
            "currentPrice": 2500.0,
        }

        # Valid price data
        price_data = pd.DataFrame(
            {
                "Date": pd.date_range(start="2023-01-01", periods=200),
                "Open": [2400] * 200,
                "High": [2450] * 200,
                "Low": [2350] * 200,
                "Close": [2400] * 200,
                "Volume": [1000000] * 200,
            }
        )

        # Valid dividend data
        dividend_data = pd.DataFrame(
            {
                "Date": pd.date_range(start="2023-01-01", periods=4, freq="Q"),
                "Dividends": [25.0, 25.0, 25.0, 30.0],
            }
        )

        should_include, warnings, errors = self.processor.validate_and_filter_data(
            "7203.T", financial_data, price_data, dividend_data
        )

        assert should_include is True
        assert len(errors) == 0
        # May have warnings but should still be included

    def test_validate_and_filter_data_invalid(self):
        """Test filtering out invalid data."""
        # Invalid financial data
        invalid_financial = {}

        # Invalid price data
        invalid_price = pd.DataFrame()

        should_include, warnings, errors = self.processor.validate_and_filter_data(
            "INVALID.T", invalid_financial, invalid_price, None
        )

        assert should_include is False
        assert len(errors) > 0

    def test_processing_config_strict_mode(self):
        """Test strict mode configuration."""
        strict_config = ProcessingConfig(
            continue_on_validation_error=False, max_consecutive_errors=1
        )

        processor = ValidationErrorProcessor(config=strict_config)

        # This should stop processing quickly with strict settings
        invalid_batch = [
            ("INVALID1.T", {}),
            ("INVALID2.T", {}),
            (
                "VALID.T",
                {"symbol": "VALID.T", "shortName": "Valid", "currentPrice": 1000.0},
            ),
        ]

        valid_data, summary = processor.process_financial_data_batch(invalid_batch)

        # Should stop early due to consecutive errors
        assert summary.total_processed <= 2  # Should not process all 3

    def test_error_summary_statistics(self):
        """Test error summary statistics calculation."""
        summary = ValidationErrorSummary()
        summary.total_processed = 10
        summary.successful_validations = 7
        summary.validation_errors = 2
        summary.skipped_items = 1

        assert summary.get_success_rate() == 70.0
        assert summary.get_error_rate() == 30.0  # (2 + 1) / 10 * 100


class TestDataValidatorIntegration:
    """Integration tests for data validation components."""

    def test_end_to_end_validation_flow(self):
        """Test complete validation flow from data input to final results."""
        # Create test data
        test_data = [
            (
                "7203.T",
                {
                    "symbol": "7203.T",
                    "shortName": "Toyota Motor Corp",
                    "currentPrice": 2500.0,
                    "trailingPE": 12.5,
                    "marketCap": 30000000000,
                },
            ),
            ("INVALID.T", {}),  # Should be filtered out
            (
                "6758.T",
                {
                    "symbol": "6758.T",
                    "shortName": "Sony Group Corp",
                    "currentPrice": 12000.0,
                    "trailingPE": 25.0,
                },
            ),
        ]

        # Process with validation
        processor = ValidationErrorProcessor()
        valid_data, summary = processor.process_financial_data_batch(test_data)

        # Verify results
        assert len(valid_data) == 2  # Only valid items
        assert summary.total_processed == 3
        assert summary.successful_validations == 2
        assert summary.validation_errors == 1
        assert summary.get_success_rate() > 60.0

        # Verify valid symbols are correct
        valid_symbols = [symbol for symbol, _ in valid_data]
        assert "7203.T" in valid_symbols
        assert "6758.T" in valid_symbols
        assert "INVALID.T" not in valid_symbols


if __name__ == "__main__":
    pytest.main([__file__])
