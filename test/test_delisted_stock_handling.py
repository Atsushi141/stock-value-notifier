"""
Tests for enhanced delisted stock handling in DataFetcher and SymbolValidator.
"""

import pytest
import logging
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime

from src.data_fetcher import DataFetcher
from src.symbol_validator import SymbolValidator, ValidationStatus
from src.exceptions import DataNotFoundError, APIError


class TestDelistedStockHandling:
    """Test enhanced delisted stock handling functionality."""

    def setup_method(self):
        """Set up test environment."""
        # Suppress logging during tests
        logging.getLogger().setLevel(logging.CRITICAL)

    def test_symbol_validator_detects_delisted_stock(self):
        """Test that SymbolValidator correctly detects delisted stocks."""
        validator = SymbolValidator()

        # Mock yfinance to simulate delisted stock
        mock_ticker = Mock()
        mock_ticker.info = {}  # Empty info indicates delisted/invalid
        mock_ticker.history.return_value = pd.DataFrame()  # Empty history

        with patch("yfinance.Ticker", return_value=mock_ticker):
            result = validator.validate_symbol("1423")  # Known delisted stock

            assert not result.is_valid
            assert result.status in [
                ValidationStatus.DELISTED,
                ValidationStatus.NOT_FOUND,
                ValidationStatus.INVALID,
            ]
            assert "1423.T" in result.symbol

    def test_symbol_validator_detects_valid_stock(self):
        """Test that SymbolValidator correctly detects valid stocks."""
        validator = SymbolValidator()

        # Mock yfinance to simulate valid stock
        mock_ticker = Mock()
        mock_ticker.info = {
            "symbol": "7203.T",
            "shortName": "Toyota Motor Corp",
            "currentPrice": 2500.0,
            "exchange": "TSE",
        }
        # Mock recent price history
        dates = pd.date_range(start="2024-01-01", periods=5, freq="D")
        mock_ticker.history.return_value = pd.DataFrame(
            {"Close": [2500, 2510, 2520, 2530, 2540]}, index=dates
        )

        with patch("yfinance.Ticker", return_value=mock_ticker):
            result = validator.validate_symbol("7203")  # Toyota

            assert result.is_valid
            assert result.status == ValidationStatus.VALID
            assert "7203.T" in result.symbol

    def test_symbol_validator_caching(self):
        """Test that SymbolValidator caches results correctly."""
        validator = SymbolValidator()

        # Mock yfinance
        mock_ticker = Mock()
        mock_ticker.info = {"symbol": "7203.T", "shortName": "Toyota"}
        mock_ticker.history.return_value = pd.DataFrame(
            {"Close": [2500]}, index=[datetime.now()]
        )

        with patch("yfinance.Ticker", return_value=mock_ticker) as mock_yf:
            # First call should hit the API
            result1 = validator.validate_symbol("7203")
            assert mock_yf.call_count == 1

            # Second call should use cache
            result2 = validator.validate_symbol("7203")
            assert mock_yf.call_count == 1  # No additional calls

            # Results should be identical
            assert result1.is_valid == result2.is_valid
            assert result1.status == result2.status

    def test_symbol_validator_batch_processing(self):
        """Test batch symbol validation."""
        validator = SymbolValidator()

        symbols = ["7203", "1423", "6758"]

        # Mock different responses for different symbols
        def mock_ticker_side_effect(symbol):
            mock_ticker = Mock()
            if symbol == "7203.T":
                mock_ticker.info = {"symbol": "7203.T", "shortName": "Toyota"}
                mock_ticker.history.return_value = pd.DataFrame(
                    {"Close": [2500]}, index=[datetime.now()]
                )
            elif symbol == "1423.T":
                mock_ticker.info = {}
                mock_ticker.history.return_value = pd.DataFrame()
            elif symbol == "6758.T":
                mock_ticker.info = {"symbol": "6758.T", "shortName": "Sony"}
                mock_ticker.history.return_value = pd.DataFrame(
                    {"Close": [12000]}, index=[datetime.now()]
                )
            return mock_ticker

        with patch("yfinance.Ticker", side_effect=mock_ticker_side_effect):
            results = validator.batch_validate_symbols(symbols)

            assert len(results) == 3
            assert results["7203"].is_valid
            assert not results["1423"].is_valid
            assert results["6758"].is_valid

    def test_symbol_validator_filter_valid_symbols(self):
        """Test filtering of valid symbols."""
        validator = SymbolValidator()

        symbols = ["7203", "1423", "6758", "9999"]

        # Mock responses
        def mock_ticker_side_effect(symbol):
            mock_ticker = Mock()
            if symbol in ["7203.T", "6758.T"]:
                mock_ticker.info = {"symbol": symbol, "shortName": "Valid Company"}
                mock_ticker.history.return_value = pd.DataFrame(
                    {"Close": [1000]}, index=[datetime.now()]
                )
            else:
                mock_ticker.info = {}
                mock_ticker.history.return_value = pd.DataFrame()
            return mock_ticker

        with patch("yfinance.Ticker", side_effect=mock_ticker_side_effect):
            valid_symbols = validator.filter_valid_symbols(symbols)

            assert len(valid_symbols) == 2
            assert "7203" in valid_symbols
            assert "6758" in valid_symbols
            assert "1423" not in valid_symbols
            assert "9999" not in valid_symbols

    def test_data_fetcher_enhanced_error_handling(self):
        """Test DataFetcher enhanced error handling for delisted stocks."""
        fetcher = DataFetcher()

        # Mock yfinance to simulate delisted stock
        mock_ticker = Mock()
        mock_ticker.history.return_value = pd.DataFrame()  # Empty data

        with patch("yfinance.Ticker", return_value=mock_ticker):
            with pytest.raises(DataNotFoundError) as exc_info:
                fetcher.get_stock_prices("1423")

            assert (
                "delisted" in str(exc_info.value).lower()
                or "invalid" in str(exc_info.value).lower()
            )

    def test_data_fetcher_financial_info_delisted_handling(self):
        """Test DataFetcher financial info handling for delisted stocks."""
        fetcher = DataFetcher()

        # Mock yfinance to simulate delisted stock
        mock_ticker = Mock()
        mock_ticker.info = {}  # Empty info

        with patch("yfinance.Ticker", return_value=mock_ticker):
            with pytest.raises(DataNotFoundError) as exc_info:
                fetcher.get_financial_info("1423")

            assert (
                "delisted" in str(exc_info.value).lower()
                or "invalid" in str(exc_info.value).lower()
            )

    def test_data_fetcher_multiple_stocks_delisted_handling(self):
        """Test DataFetcher multiple stocks handling with delisted stocks."""
        fetcher = DataFetcher()

        symbols = ["7203", "1423", "6758"]

        # Mock get_financial_info to simulate mixed results
        def mock_get_financial_info(symbol):
            if symbol == "1423":
                raise DataNotFoundError(f"Stock {symbol} appears to be delisted")
            else:
                return {
                    "symbol": f"{symbol}.T",
                    "shortName": f"Company {symbol}",
                    "currentPrice": 1500.0,
                }

        with patch.object(
            fetcher, "get_financial_info", side_effect=mock_get_financial_info
        ):
            results = fetcher.get_multiple_stocks_info(symbols)

            # Should only return valid stocks
            assert len(results) == 2
            assert "7203" in results
            assert "6758" in results
            assert "1423" not in results

    def test_data_fetcher_symbol_filtering_integration(self):
        """Test DataFetcher integration with SymbolFilter for filtering."""
        fetcher = DataFetcher()

        symbols = ["7203", "1423", "6758"]

        # Mock SymbolFilter instead of SymbolValidator
        mock_filter = Mock()
        mock_filtering_result = Mock()
        mock_filtering_result.valid_symbols = ["7203", "6758"]  # Filter out 1423
        mock_filtering_result.filtered_symbols = ["1423"]
        mock_filtering_result.delisted_symbols = ["1423"]
        mock_filtering_result.invalid_symbols = []
        mock_filtering_result.error_symbols = []
        mock_filtering_result.filter_rate = 0.33  # 1 out of 3 filtered
        mock_filter.filter_symbols.return_value = mock_filtering_result

        fetcher.symbol_filter = mock_filter

        valid_symbols = fetcher.validate_and_filter_symbols(symbols)

        assert len(valid_symbols) == 2
        assert "7203" in valid_symbols
        assert "6758" in valid_symbols
        assert "1423" not in valid_symbols

        # Verify the filter was called
        mock_filter.filter_symbols.assert_called_once_with(
            symbols=symbols,
            operation_name="validate_and_filter_symbols",
            log_details=True,
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
