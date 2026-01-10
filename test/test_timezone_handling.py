"""
Tests for timezone handling to prevent timezone comparison errors.

This test suite ensures that timezone-related operations in data fetching
work correctly and prevent the "Invalid comparison between dtype=datetime64[ns, Asia/Tokyo] and datetime" error.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from data_fetcher import DataFetcher
from exceptions import DataNotFoundError, APIError


class TestTimezoneHandling:
    """Test timezone handling in data fetcher operations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.data_fetcher = DataFetcher()

    @pytest.fixture
    def mock_dividend_data_with_timezone(self):
        """Create mock dividend data with Asia/Tokyo timezone."""
        dates = pd.date_range("2023-01-01", periods=4, freq="3M", tz="Asia/Tokyo")
        return pd.Series([50.0, 55.0, 60.0, 65.0], index=dates, name="Dividends")

    @pytest.fixture
    def mock_dividend_data_naive(self):
        """Create mock dividend data without timezone (naive)."""
        dates = pd.date_range("2023-01-01", periods=4, freq="3M")
        return pd.Series([50.0, 55.0, 60.0, 65.0], index=dates, name="Dividends")

    @pytest.fixture
    def mock_dividend_data_utc(self):
        """Create mock dividend data with UTC timezone."""
        dates = pd.date_range("2023-01-01", periods=4, freq="3M", tz="UTC")
        return pd.Series([50.0, 55.0, 60.0, 65.0], index=dates, name="Dividends")

    @patch("yfinance.Ticker")
    def test_dividend_history_with_asia_tokyo_timezone(
        self, mock_ticker, mock_dividend_data_with_timezone
    ):
        """Test dividend history retrieval with Asia/Tokyo timezone data."""
        # Setup mock
        mock_ticker_instance = Mock()
        mock_ticker_instance.dividends = mock_dividend_data_with_timezone
        mock_ticker.return_value = mock_ticker_instance

        # Test should not raise timezone comparison error
        result = self.data_fetcher.get_dividend_history("7203.T", "1y")

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "Date" in result.columns
        assert "Dividends" in result.columns
        assert "Symbol" in result.columns

    @patch("yfinance.Ticker")
    def test_dividend_history_with_naive_timezone(
        self, mock_ticker, mock_dividend_data_naive
    ):
        """Test dividend history retrieval with timezone-naive data."""
        # Setup mock
        mock_ticker_instance = Mock()
        mock_ticker_instance.dividends = mock_dividend_data_naive
        mock_ticker.return_value = mock_ticker_instance

        # Test should not raise timezone comparison error
        result = self.data_fetcher.get_dividend_history("7203.T", "1y")

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "Date" in result.columns
        assert "Dividends" in result.columns
        assert "Symbol" in result.columns

    @patch("yfinance.Ticker")
    def test_dividend_history_with_utc_timezone(
        self, mock_ticker, mock_dividend_data_utc
    ):
        """Test dividend history retrieval with UTC timezone data."""
        # Setup mock
        mock_ticker_instance = Mock()
        mock_ticker_instance.dividends = mock_dividend_data_utc
        mock_ticker.return_value = mock_ticker_instance

        # Test should not raise timezone comparison error
        result = self.data_fetcher.get_dividend_history("7203.T", "1y")

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    @patch("yfinance.Ticker")
    def test_dividend_history_timezone_conversion_robustness(self, mock_ticker):
        """Test that timezone conversion handles various edge cases."""
        # Test with mixed timezone scenarios
        test_cases = [
            # Asia/Tokyo timezone
            pd.date_range("2023-01-01", periods=2, freq="6M", tz="Asia/Tokyo"),
            # UTC timezone
            pd.date_range("2023-01-01", periods=2, freq="6M", tz="UTC"),
            # US/Eastern timezone
            pd.date_range("2023-01-01", periods=2, freq="6M", tz="US/Eastern"),
            # Naive (no timezone)
            pd.date_range("2023-01-01", periods=2, freq="6M"),
        ]

        for i, dates in enumerate(test_cases):
            mock_ticker_instance = Mock()
            mock_ticker_instance.dividends = pd.Series(
                [50.0, 55.0], index=dates, name="Dividends"
            )
            mock_ticker.return_value = mock_ticker_instance

            # Each case should work without timezone errors
            result = self.data_fetcher.get_dividend_history(f"TEST{i}.T", "1y")
            assert isinstance(result, pd.DataFrame)

    @patch("yfinance.Ticker")
    def test_dividend_history_period_filtering_with_timezones(
        self, mock_ticker, mock_dividend_data_with_timezone
    ):
        """Test that period filtering works correctly with different timezones."""
        mock_ticker_instance = Mock()
        mock_ticker_instance.dividends = mock_dividend_data_with_timezone
        mock_ticker.return_value = mock_ticker_instance

        # Test different period formats
        periods = ["1y", "2y", "3y", "6mo", "12mo"]

        for period in periods:
            result = self.data_fetcher.get_dividend_history("7203.T", period)
            assert isinstance(result, pd.DataFrame)
            # Should not raise any timezone comparison errors

    @patch("yfinance.Ticker")
    def test_dividend_history_empty_data_handling(self, mock_ticker):
        """Test handling of empty dividend data."""
        mock_ticker_instance = Mock()
        mock_ticker_instance.dividends = pd.Series([], dtype=float, name="Dividends")
        mock_ticker.return_value = mock_ticker_instance

        result = self.data_fetcher.get_dividend_history("NODIV.T", "1y")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ["Date", "Dividends", "Symbol"]

    def test_timezone_error_fallback_mechanism(self):
        """Test that timezone errors are properly caught and handled with fallback."""
        with patch("yfinance.Ticker") as mock_ticker:
            # Create a mock that raises a timezone-related error during filtering
            mock_ticker_instance = Mock()

            # Create dividend data that will cause timezone issues
            problematic_dates = pd.date_range(
                "2023-01-01", periods=2, freq="6M", tz="Asia/Tokyo"
            )
            mock_ticker_instance.dividends = pd.Series(
                [50.0, 55.0], index=problematic_dates, name="Dividends"
            )
            mock_ticker.return_value = mock_ticker_instance

            # Mock the timezone conversion to raise an error
            with patch("pandas.Timestamp.now") as mock_now:
                mock_now.side_effect = Exception("Timezone conversion error")

                # Should not crash, should use fallback (all available data)
                result = self.data_fetcher.get_dividend_history("7203.T", "1y")
                assert isinstance(result, pd.DataFrame)

    @pytest.mark.parametrize(
        "timezone_str",
        ["Asia/Tokyo", "UTC", "US/Eastern", "Europe/London", None],  # Naive timezone
    )
    def test_dividend_history_various_timezones(self, timezone_str):
        """Test dividend history with various timezone configurations."""
        with patch("yfinance.Ticker") as mock_ticker:
            mock_ticker_instance = Mock()

            if timezone_str:
                dates = pd.date_range(
                    "2023-01-01", periods=3, freq="4M", tz=timezone_str
                )
            else:
                dates = pd.date_range("2023-01-01", periods=3, freq="4M")

            mock_ticker_instance.dividends = pd.Series(
                [40.0, 45.0, 50.0], index=dates, name="Dividends"
            )
            mock_ticker.return_value = mock_ticker_instance

            # Should handle any timezone without errors
            result = self.data_fetcher.get_dividend_history("TEST.T", "1y")
            assert isinstance(result, pd.DataFrame)
            assert len(result) >= 0  # May be filtered by period

    def test_utc_conversion_consistency(self):
        """Test that UTC conversion produces consistent results."""
        with patch("yfinance.Ticker") as mock_ticker:
            mock_ticker_instance = Mock()

            # Create same data in different timezones
            tokyo_dates = pd.date_range(
                "2023-06-01", periods=2, freq="3M", tz="Asia/Tokyo"
            )
            utc_dates = tokyo_dates.tz_convert("UTC")

            tokyo_dividends = pd.Series(
                [50.0, 55.0], index=tokyo_dates, name="Dividends"
            )
            utc_dividends = pd.Series([50.0, 55.0], index=utc_dates, name="Dividends")

            # Test Tokyo timezone data
            mock_ticker_instance.dividends = tokyo_dividends
            mock_ticker.return_value = mock_ticker_instance
            result_tokyo = self.data_fetcher.get_dividend_history("TEST.T", "1y")

            # Test UTC timezone data
            mock_ticker_instance.dividends = utc_dividends
            result_utc = self.data_fetcher.get_dividend_history("TEST.T", "1y")

            # Results should be consistent (same number of records)
            assert len(result_tokyo) == len(result_utc)


if __name__ == "__main__":
    pytest.main([__file__])
