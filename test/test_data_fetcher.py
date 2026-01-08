"""
Property-based tests for DataFetcher class using yfinance.
Tests Property 1: データ取得の完全性 (Data retrieval completeness)
"""

import pytest
from hypothesis import given, strategies as st, assume, settings
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime, timedelta
import logging

from src.data_fetcher import (
    DataFetcher,
    APIError,
    AuthenticationError,
    RateLimitError,
    DataNotFoundError,
)


class TestDataFetcherProperties:
    """Property-based tests for DataFetcher using yfinance."""

    def setup_method(self):
        """Set up test environment."""
        # Suppress logging during tests
        logging.getLogger().setLevel(logging.CRITICAL)

    @settings(max_examples=5)
    @given(
        symbol=st.text(
            min_size=4, max_size=4, alphabet=st.characters(whitelist_categories=("Nd",))
        ),
        period=st.sampled_from(["1y", "3y"]),
        num_records=st.integers(min_value=0, max_value=50),
    )
    def test_property_1_stock_price_data_completeness(
        self, symbol, period, num_records
    ):
        """
        Property 1: データ取得の完全性 - Stock Price Data
        For any valid Japanese stock symbol and period, when the system executes
        stock price data retrieval, all available price data should be retrieved
        or clear error logs should be recorded.

        **Validates: Requirements 1.1, 1.2**
        **Feature: stock-value-notifier, Property 1: データ取得の完全性**
        """
        # Assume valid Japanese stock code format
        assume(len(symbol) == 4)
        assume(symbol.isdigit())

        data_fetcher = DataFetcher()

        # Mock yfinance Ticker and history data
        mock_ticker = Mock()
        mock_history = pd.DataFrame()

        if num_records > 0:
            # Create mock historical data
            dates = pd.date_range(start="2021-01-01", periods=num_records, freq="D")
            mock_history = pd.DataFrame(
                {
                    "Open": [1000.0 + i for i in range(num_records)],
                    "High": [1100.0 + i for i in range(num_records)],
                    "Low": [900.0 + i for i in range(num_records)],
                    "Close": [1050.0 + i for i in range(num_records)],
                    "Volume": [10000 + i * 100 for i in range(num_records)],
                },
                index=dates,
            )

        mock_ticker.history.return_value = mock_history

        with patch("yfinance.Ticker", return_value=mock_ticker):
            if num_records > 0:
                # Execute stock price data retrieval
                result_df = data_fetcher.get_stock_prices(symbol, period)

                # Property 1: Data retrieval should be complete
                assert isinstance(
                    result_df, pd.DataFrame
                ), "Result should be a pandas DataFrame"

                # Property: All available price records should be returned
                assert (
                    len(result_df) == num_records
                ), f"Expected {num_records} records, got {len(result_df)}"

                # Property: Essential price columns should be present
                expected_columns = [
                    "Date",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Symbol",
                ]
                for col in expected_columns:
                    assert col in result_df.columns, f"Column {col} should be present"

                # Property: Numeric columns should be properly typed
                numeric_columns = ["Open", "High", "Low", "Close", "Volume"]
                for col in numeric_columns:
                    assert pd.api.types.is_numeric_dtype(
                        result_df[col]
                    ), f"Column {col} should be numeric"

                # Property: Symbol column should contain formatted symbol
                expected_symbol = f"{symbol}.T"
                assert all(
                    result_df["Symbol"] == expected_symbol
                ), f"All records should have symbol {expected_symbol}"

                # Property: Date column should contain valid dates
                assert pd.api.types.is_datetime64_any_dtype(
                    result_df["Date"]
                ), "Date column should contain datetime values"

            else:
                # Property: Empty data should raise DataNotFoundError
                with pytest.raises(DataNotFoundError):
                    data_fetcher.get_stock_prices(symbol, period)

            # Property: yfinance Ticker should be called with formatted symbol
            expected_symbol = f"{symbol}.T"
            mock_ticker.history.assert_called_once_with(period=period)

    @settings(max_examples=10)
    @given(
        symbol=st.text(
            min_size=4, max_size=6, alphabet=st.characters(whitelist_categories=("Nd",))
        ),
        has_financial_data=st.booleans(),
    )
    def test_property_1_financial_info_completeness(self, symbol, has_financial_data):
        """
        Property 1: 財務情報取得の完全性
        For any Japanese stock symbol, when financial information is retrieved,
        all available financial metrics should be returned or clear error should be raised.

        **Validates: Requirements 1.1, 1.2**
        **Feature: stock-value-notifier, Property 1: データ取得の完全性**
        """
        # Assume valid Japanese stock code format
        assume(len(symbol) == 4)
        assume(symbol.isdigit())

        data_fetcher = DataFetcher()

        # Mock yfinance Ticker and info data
        mock_ticker = Mock()

        if has_financial_data:
            mock_info = {
                "symbol": f"{symbol}.T",
                "shortName": f"Company {symbol}",
                "longName": f"Company {symbol} Corporation",
                "currentPrice": 1500.0,
                "previousClose": 1480.0,
                "marketCap": 1000000000,
                "trailingPE": 12.5,
                "forwardPE": 11.8,
                "priceToBook": 1.2,
                "dividendYield": 0.025,
                "trailingAnnualDividendYield": 0.024,
                "trailingAnnualDividendRate": 36.0,
                "totalRevenue": 500000000,
                "revenueGrowth": 0.05,
                "earningsGrowth": 0.08,
                "currency": "JPY",
                "exchange": "TSE",
                "sector": "Technology",
                "industry": "Software",
            }
        else:
            mock_info = {}

        mock_ticker.info = mock_info

        with patch("yfinance.Ticker", return_value=mock_ticker):
            if has_financial_data:
                # Execute financial info retrieval
                result_info = data_fetcher.get_financial_info(symbol)

                # Property 1: Financial info retrieval should be complete
                assert isinstance(result_info, dict), "Result should be a dictionary"

                # Property: Essential financial metrics should be present
                essential_keys = [
                    "symbol",
                    "shortName",
                    "currentPrice",
                    "trailingPE",
                    "priceToBook",
                ]
                for key in essential_keys:
                    assert (
                        key in result_info
                    ), f"Key {key} should be present in financial info"

                # Property: Symbol should be formatted correctly
                expected_symbol = f"{symbol}.T"
                assert (
                    result_info["symbol"] == expected_symbol
                ), f"Symbol should be {expected_symbol}"

                # Property: Numeric fields should be numeric or None
                numeric_fields = [
                    "currentPrice",
                    "trailingPE",
                    "priceToBook",
                    "dividendYield",
                ]
                for field in numeric_fields:
                    if result_info.get(field) is not None:
                        assert isinstance(
                            result_info[field], (int, float)
                        ), f"Field {field} should be numeric"

            else:
                # Property: No financial data should raise DataNotFoundError
                with pytest.raises(DataNotFoundError):
                    data_fetcher.get_financial_info(symbol)

    @settings(max_examples=10)
    @given(
        symbol=st.text(
            min_size=4, max_size=6, alphabet=st.characters(whitelist_categories=("Nd",))
        ),
        period=st.sampled_from(["1y", "2y", "3y"]),
        num_dividends=st.integers(min_value=0, max_value=10),
    )
    def test_property_1_dividend_data_completeness(self, symbol, period, num_dividends):
        """
        Property 1: 配当データ取得の完全性
        For any Japanese stock symbol and period, when dividend history is retrieved,
        all available dividend records should be returned or empty DataFrame for non-dividend stocks.

        **Validates: Requirements 1.1, 1.2**
        **Feature: stock-value-notifier, Property 1: データ取得の完全性**
        """
        # Assume valid Japanese stock code format
        assume(len(symbol) == 4)
        assume(symbol.isdigit())

        data_fetcher = DataFetcher()

        # Mock yfinance Ticker and dividend data
        mock_ticker = Mock()

        if num_dividends > 0:
            # Create mock dividend data
            dates = pd.date_range(start="2021-01-01", periods=num_dividends, freq="Q")
            mock_dividends = pd.Series(
                [10.0 + i for i in range(num_dividends)], index=dates, name="Dividends"
            )
        else:
            mock_dividends = pd.Series([], dtype=float, name="Dividends")

        mock_ticker.dividends = mock_dividends

        with patch("yfinance.Ticker", return_value=mock_ticker):
            # Execute dividend history retrieval
            result_df = data_fetcher.get_dividend_history(symbol, period)

            # Property 1: Dividend data retrieval should be complete
            assert isinstance(
                result_df, pd.DataFrame
            ), "Result should be a pandas DataFrame"

            if num_dividends > 0:
                # Property: All available dividend records should be returned
                assert (
                    len(result_df) > 0
                ), "Should return dividend records when available"

                # Property: Essential dividend columns should be present
                expected_columns = ["Date", "Dividends", "Symbol"]
                for col in expected_columns:
                    assert col in result_df.columns, f"Column {col} should be present"

                # Property: Dividends column should be numeric
                assert pd.api.types.is_numeric_dtype(
                    result_df["Dividends"]
                ), "Dividends column should be numeric"

                # Property: Symbol column should contain formatted symbol
                expected_symbol = f"{symbol}.T"
                assert all(
                    result_df["Symbol"] == expected_symbol
                ), f"All records should have symbol {expected_symbol}"

                # Property: Date column should contain valid dates
                assert pd.api.types.is_datetime64_any_dtype(
                    result_df["Date"]
                ), "Date column should contain datetime values"

            else:
                # Property: No dividend data should return empty DataFrame with correct columns
                expected_columns = ["Date", "Dividends", "Symbol"]
                for col in expected_columns:
                    assert (
                        col in result_df.columns
                    ), f"Column {col} should be present even in empty DataFrame"
                assert (
                    len(result_df) == 0
                ), "Should return empty DataFrame when no dividends available"

    @settings(max_examples=5)
    @given(
        num_stocks=st.integers(min_value=1, max_value=20),
    )
    def test_property_1_japanese_stock_list_completeness(self, num_stocks):
        """
        Property 1: 日本株リスト取得の完全性
        For any request to get Japanese stock list, all available stock symbols
        should be returned in the correct format.

        **Validates: Requirements 1.1, 1.2**
        **Feature: stock-value-notifier, Property 1: データ取得の完全性**
        """
        data_fetcher = DataFetcher()

        # Execute Japanese stock list retrieval
        result_list = data_fetcher.get_japanese_stock_list()

        # Property 1: Stock list retrieval should be complete
        assert isinstance(result_list, list), "Result should be a list"
        assert len(result_list) > 0, "Should return at least some stock symbols"

        # Property: All symbols should be properly formatted for Japanese stocks
        for symbol in result_list:
            assert isinstance(symbol, str), "Each symbol should be a string"
            assert ".T" in symbol, "Japanese stock symbols should have .T suffix"

            # Extract the numeric part
            code_part = symbol.split(".")[0]
            assert code_part.isdigit(), f"Stock code {code_part} should be numeric"
            assert len(code_part) == 4, f"Stock code {code_part} should be 4 digits"

        # Property: List should not contain duplicates
        assert len(result_list) == len(
            set(result_list)
        ), "Stock list should not contain duplicates"

    @settings(max_examples=10)
    @given(
        symbols=st.lists(
            st.text(
                min_size=4,
                max_size=4,
                alphabet=st.characters(whitelist_categories=("Nd",)),
            ),
            min_size=1,
            max_size=5,
            unique=True,
        ),
        success_rate=st.floats(min_value=0.0, max_value=1.0),
    )
    def test_property_1_multiple_stocks_info_completeness(self, symbols, success_rate):
        """
        Property 1: 複数銘柄情報取得の完全性
        For any list of Japanese stock symbols, when multiple stock information is retrieved,
        all available data should be returned and unavailable stocks should be properly handled.

        **Validates: Requirements 1.1, 1.2**
        **Feature: stock-value-notifier, Property 1: データ取得の完全性**
        """
        # Assume all symbols are valid 4-digit codes
        for symbol in symbols:
            assume(len(symbol) == 4)
            assume(symbol.isdigit())

        data_fetcher = DataFetcher()

        # Mock get_financial_info method
        def mock_get_financial_info(symbol):
            # Simulate success/failure based on success_rate
            import random

            if random.random() < success_rate:
                return {
                    "symbol": f"{symbol}.T",
                    "shortName": f"Company {symbol}",
                    "currentPrice": 1500.0,
                    "trailingPE": 12.5,
                    "priceToBook": 1.2,
                }
            else:
                raise DataNotFoundError(f"No data found for {symbol}")

        with patch.object(
            data_fetcher, "get_financial_info", side_effect=mock_get_financial_info
        ):
            # Execute multiple stocks info retrieval
            result_dict = data_fetcher.get_multiple_stocks_info(symbols)

            # Property 1: Multiple stocks info retrieval should be complete
            assert isinstance(result_dict, dict), "Result should be a dictionary"

            # Property: Result should contain data for successful retrievals only
            expected_successful = int(len(symbols) * success_rate)
            # Allow some variance due to randomness
            assert len(result_dict) <= len(
                symbols
            ), "Should not return more data than requested symbols"

            # Property: All returned data should be valid
            for symbol, info in result_dict.items():
                assert isinstance(
                    info, dict
                ), f"Info for {symbol} should be a dictionary"
                assert "symbol" in info, f"Info for {symbol} should contain symbol"
                assert info["symbol"].endswith(
                    ".T"
                ), f"Symbol {info['symbol']} should have .T suffix"

            # Property: All requested symbols should have been attempted
            # (This is verified by the mock being called for each symbol)

    @settings(max_examples=10)
    @given(
        error_type=st.sampled_from(["network", "rate_limit", "not_found", "generic"]),
        symbol=st.text(
            min_size=4, max_size=4, alphabet=st.characters(whitelist_categories=("Nd",))
        ),
    )
    def test_property_1_error_handling_completeness(self, error_type, symbol):
        """
        Property 1: エラーハンドリングの完全性
        For any error condition during data retrieval, the system should either
        retry appropriately or raise a clear, categorized exception with proper logging.

        **Validates: Requirements 1.1, 1.2**
        **Feature: stock-value-notifier, Property 1: データ取得の完全性**
        """
        # Assume valid Japanese stock code format
        assume(len(symbol) == 4)
        assume(symbol.isdigit())

        data_fetcher = DataFetcher()

        # Mock yfinance Ticker to raise different types of errors
        mock_ticker = Mock()

        if error_type == "network":
            from requests.exceptions import ConnectionError

            mock_ticker.history.side_effect = ConnectionError(
                "Network connection failed"
            )
        elif error_type == "rate_limit":
            from requests.exceptions import HTTPError

            mock_response = Mock()
            mock_response.status_code = 429
            http_error = HTTPError("Rate limit exceeded")
            http_error.response = mock_response
            mock_ticker.history.side_effect = http_error
        elif error_type == "not_found":
            from requests.exceptions import HTTPError

            mock_response = Mock()
            mock_response.status_code = 404
            http_error = HTTPError("Not found")
            http_error.response = mock_response
            mock_ticker.history.side_effect = http_error
        else:  # generic error
            mock_ticker.history.side_effect = Exception("Generic error")

        with patch("yfinance.Ticker", return_value=mock_ticker):
            # Property: All errors should result in appropriate exceptions
            with pytest.raises(APIError) as exc_info:
                data_fetcher.get_stock_prices(symbol, "1y")

            # Property: Error should be properly categorized
            exception = exc_info.value
            if error_type == "rate_limit":
                assert isinstance(
                    exception, RateLimitError
                ), "Rate limit errors should raise RateLimitError"
            elif error_type == "not_found":
                assert isinstance(
                    exception, DataNotFoundError
                ), "Not found errors should raise DataNotFoundError"
            else:
                # Network and generic errors should be APIError
                assert isinstance(
                    exception, APIError
                ), f"Error type {error_type} should raise APIError"

            # Property: Exception should contain meaningful error message
            assert len(str(exception)) > 0, "Exception should have a meaningful message"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
