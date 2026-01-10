"""
Tests for type conversion handling in screening engine.

This test suite ensures that the screening engine properly handles various data types
from yfinance, including string values, None values, and invalid data that could cause
TypeError exceptions during comparison operations.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.screening_engine import ScreeningEngine
from src.models import ScreeningConfig, ValueStock


class TestTypeConversion:
    """Test type conversion and data validation in screening engine."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = ScreeningConfig()
        self.engine = ScreeningEngine(self.config)

    def test_safe_float_conversion(self):
        """Test the _safe_float method with various input types."""
        # Test numeric values
        assert self.engine._safe_float(12.5) == 12.5
        assert self.engine._safe_float(15) == 15.0
        assert self.engine._safe_float(0) == 0.0
        assert self.engine._safe_float(-5.5) == -5.5

        # Test string numeric values
        assert self.engine._safe_float("12.5") == 12.5
        assert self.engine._safe_float("15") == 15.0
        assert self.engine._safe_float("0.0") == 0.0

        # Test invalid string values
        assert self.engine._safe_float("N/A", 999.0) == 999.0
        assert self.engine._safe_float("na", 999.0) == 999.0
        assert self.engine._safe_float("NaN", 999.0) == 999.0
        assert self.engine._safe_float("null", 999.0) == 999.0
        assert self.engine._safe_float("none", 999.0) == 999.0
        assert self.engine._safe_float("", 999.0) == 999.0
        assert self.engine._safe_float("-", 999.0) == 999.0
        assert self.engine._safe_float("invalid", 999.0) == 999.0

        # Test None and NaN values
        assert self.engine._safe_float(None, 999.0) == 999.0
        assert self.engine._safe_float(np.nan, 999.0) == 999.0
        assert self.engine._safe_float(pd.NA, 999.0) == 999.0

        # Test infinity values
        assert self.engine._safe_float(float("inf"), 999.0) == 999.0
        assert self.engine._safe_float(float("-inf"), 999.0) == 999.0

        # Test very large values
        assert self.engine._safe_float(1e15, 999.0) == 999.0

    def test_screening_with_string_values(self):
        """Test screening with string values that previously caused TypeError."""
        test_data = pd.DataFrame(
            [
                {
                    "code": "7203.T",
                    "name": "Toyota",
                    "current_price": 2500.0,
                    "per": "12.5",  # String PER
                    "pbr": "1.2",  # String PBR
                    "dividend_yield": "0.028",  # String dividend yield
                    "financial_data": {"statements": []},
                    "dividend_data": {"dividends": []},
                },
                {
                    "code": "6758.T",
                    "name": "Sony",
                    "current_price": "13500.0",  # String price
                    "per": 14.2,  # Float PER
                    "pbr": 1.4,  # Float PBR
                    "dividend_yield": 0.008,  # Float dividend yield
                    "financial_data": {"statements": []},
                    "dividend_data": {"dividends": []},
                },
            ]
        )

        # Should not raise TypeError
        result = self.engine.screen_value_stocks(test_data)
        assert isinstance(result, list)

    def test_screening_with_invalid_values(self):
        """Test screening with invalid/missing values."""
        test_data = pd.DataFrame(
            [
                {
                    "code": "INVALID.T",
                    "name": "Invalid Stock",
                    "current_price": "N/A",
                    "per": "N/A",
                    "pbr": None,
                    "dividend_yield": "",
                    "financial_data": {"statements": []},
                    "dividend_data": {"dividends": []},
                },
                {
                    "code": "MISSING.T",
                    "name": "Missing Data",
                    "current_price": np.nan,
                    "per": float("inf"),
                    "pbr": "invalid",
                    "dividend_yield": "-",
                    "financial_data": {"statements": []},
                    "dividend_data": {"dividends": []},
                },
            ]
        )

        # Should not raise TypeError and should filter out invalid stocks
        result = self.engine.screen_value_stocks(test_data)
        assert isinstance(result, list)
        assert len(result) == 0  # All stocks should be filtered out due to invalid data

    def test_screening_with_mixed_data_types(self):
        """Test screening with mixed data types in the same column."""
        test_data = pd.DataFrame(
            [
                {
                    "code": "MIXED1.T",
                    "name": "Mixed Type 1",
                    "current_price": 1000.0,
                    "per": 10.5,  # Float
                    "pbr": "1.1",  # String
                    "dividend_yield": 0.03,  # Float
                    "financial_data": {"statements": []},
                    "dividend_data": {"dividends": []},
                },
                {
                    "code": "MIXED2.T",
                    "name": "Mixed Type 2",
                    "current_price": "2000.0",  # String
                    "per": "8.5",  # String
                    "pbr": 0.9,  # Float
                    "dividend_yield": "0.025",  # String
                    "financial_data": {"statements": []},
                    "dividend_data": {"dividends": []},
                },
            ]
        )

        # Should handle mixed types without errors
        result = self.engine.screen_value_stocks(test_data)
        assert isinstance(result, list)

    def test_meets_basic_criteria_with_string_values(self):
        """Test _meets_basic_criteria method with string values."""
        # Create test row with string values
        test_row = pd.Series(
            {
                "code": "TEST.T",
                "name": "Test Stock",
                "current_price": "1500.0",
                "per": "12.0",
                "pbr": "1.3",
                "dividend_yield": "0.025",
            }
        )

        # Should not raise TypeError
        result = self.engine._meets_basic_criteria(test_row)
        assert isinstance(result, bool)

    def test_meets_basic_criteria_with_invalid_values(self):
        """Test _meets_basic_criteria method with invalid values."""
        # Create test row with invalid values
        test_row = pd.Series(
            {
                "code": "INVALID.T",
                "name": "Invalid Stock",
                "current_price": "N/A",
                "per": "invalid",
                "pbr": None,
                "dividend_yield": "",
            }
        )

        # Should return False for invalid data
        result = self.engine._meets_basic_criteria(test_row)
        assert result is False

    def test_value_stock_creation_with_converted_values(self):
        """Test ValueStock creation with type-converted values."""
        test_data = pd.DataFrame(
            [
                {
                    "code": "CONVERT.T",
                    "name": "Conversion Test",
                    "current_price": "1200.0",
                    "per": "11.5",
                    "pbr": "1.1",
                    "dividend_yield": "0.035",
                    "financial_data": {"statements": []},
                    "dividend_data": {"dividends": []},
                }
            ]
        )

        result = self.engine.screen_value_stocks(test_data)

        if result:  # If stock passes screening
            stock = result[0]
            assert isinstance(stock, ValueStock)
            assert isinstance(stock.current_price, float)
            assert isinstance(stock.per, float)
            assert isinstance(stock.pbr, float)
            assert isinstance(stock.dividend_yield, float)

    def test_edge_case_string_formats(self):
        """Test edge cases in string format handling."""
        edge_cases = [
            "  12.5  ",  # Whitespace
            "12,500.50",  # Comma (should fail gracefully)
            "12.5%",  # Percentage sign (should fail gracefully)
            "$12.50",  # Currency symbol (should fail gracefully)
            "1.23e2",  # Scientific notation
            "+15.5",  # Plus sign
            "-.5",  # Negative decimal
        ]

        for case in edge_cases:
            # Should not raise exceptions
            result = self.engine._safe_float(case, 999.0)
            assert isinstance(result, float)

    def test_regression_original_error(self):
        """Regression test for the original TypeError."""
        # This is the exact scenario that caused the original error
        test_row = pd.Series(
            {
                "code": "7203.T",
                "name": "Toyota",
                "current_price": 2500.0,
                "per": "12.5",  # String value from yfinance
                "pbr": 1.2,
                "dividend_yield": 0.028,
            }
        )

        # This should NOT raise: TypeError: '>' not supported between instances of 'str' and 'float'
        try:
            result = self.engine._meets_basic_criteria(test_row)
            assert isinstance(result, bool)
            success = True
        except TypeError as e:
            if "'>' not supported between instances of 'str' and 'float'" in str(e):
                success = False
            else:
                raise  # Re-raise if it's a different TypeError

        assert success, "Original TypeError regression detected"


if __name__ == "__main__":
    pytest.main([__file__])
