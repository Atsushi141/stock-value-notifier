"""
Data fetcher module for yfinance API

This module handles data retrieval from Yahoo Finance using yfinance library including:
- Stock price data retrieval for Japanese stocks
- Financial information retrieval (PER, PBR, etc.)
- Dividend history data retrieval
- Japanese stock list management
- Comprehensive error handling and retry logic
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import pandas as pd
import yfinance as yf
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout


class APIError(Exception):
    """Custom exception for API-related errors"""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[Dict] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class AuthenticationError(APIError):
    """Exception for authentication-related errors"""

    pass


class RateLimitError(APIError):
    """Exception for rate limit errors"""

    pass


class DataNotFoundError(APIError):
    """Exception for data not found errors"""

    pass


class DataFetcher:
    """
    Handles data retrieval from Yahoo Finance using yfinance library

    Provides methods for:
    - Stock price data retrieval for Japanese stocks
    - Financial information retrieval (PER, PBR, dividend yield, etc.)
    - Dividend history data retrieval
    - Japanese stock list management
    - Comprehensive error handling and retry logic
    """

    MAX_RETRIES = 3
    RETRY_DELAY = 1.0  # Base delay in seconds
    RATE_LIMIT_DELAY = 60  # Delay for rate limit errors

    # Common Japanese stock exchanges and their suffixes
    JAPANESE_EXCHANGES = {
        "TSE": ".T",  # Tokyo Stock Exchange
        "OSE": ".OS",  # Osaka Stock Exchange
        "NSE": ".NS",  # Nagoya Stock Exchange
    }

    def __init__(self):
        """
        Initialize DataFetcher for yfinance

        Note: yfinance doesn't require authentication tokens
        """
        self.logger = logging.getLogger(__name__)

        # Cache for Japanese stock list to avoid repeated API calls
        self._japanese_stocks_cache: Optional[List[str]] = None
        self._cache_timestamp: Optional[datetime] = None
        self._cache_duration = timedelta(hours=24)  # Cache for 24 hours

    def _handle_yfinance_error(
        self, error: Exception, operation: str, symbol: str = ""
    ) -> None:
        """
        Handle yfinance/requests errors with appropriate exception types

        Args:
            error: The original exception
            operation: Description of the operation that failed
            symbol: Stock symbol if applicable

        Raises:
            RateLimitError: For rate limiting errors
            DataNotFoundError: For data not found errors
            APIError: For other API errors
        """
        symbol_info = f" for symbol {symbol}" if symbol else ""
        error_message = f"{operation}{symbol_info} failed: {str(error)}"

        if isinstance(error, (ConnectionError, Timeout)):
            self.logger.error(f"Network error: {error_message}")
            raise APIError(error_message)
        elif isinstance(error, HTTPError):
            if hasattr(error, "response") and error.response is not None:
                status_code = error.response.status_code
                if status_code == 429:
                    self.logger.warning(f"Rate limit exceeded: {error_message}")
                    raise RateLimitError(error_message, status_code)
                elif status_code == 404:
                    self.logger.warning(f"Data not found: {error_message}")
                    raise DataNotFoundError(error_message, status_code)
                else:
                    self.logger.error(f"HTTP error: {error_message}")
                    raise APIError(error_message, status_code)
            else:
                self.logger.error(f"HTTP error: {error_message}")
                raise APIError(error_message)
        else:
            self.logger.error(f"Unexpected error: {error_message}")
            raise APIError(error_message)

    def _retry_operation(self, operation_func, operation_name: str, *args, **kwargs):
        """
        Execute operation with retry logic for rate limits and transient errors

        Args:
            operation_func: Function to execute
            operation_name: Name of the operation for logging
            *args, **kwargs: Arguments to pass to the operation function

        Returns:
            Result of the operation function

        Raises:
            APIError: If all retry attempts fail
        """
        last_exception = None

        for attempt in range(self.MAX_RETRIES + 1):
            try:
                return operation_func(*args, **kwargs)

            except RateLimitError as e:
                last_exception = e
                if attempt < self.MAX_RETRIES:
                    delay = self.RATE_LIMIT_DELAY * (2**attempt)
                    self.logger.warning(
                        f"Rate limit hit for {operation_name}, retrying in {delay} seconds (attempt {attempt + 1})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    raise

            except (APIError, RequestException) as e:
                last_exception = e
                if attempt < self.MAX_RETRIES:
                    delay = self.RETRY_DELAY * (2**attempt)
                    self.logger.warning(
                        f"{operation_name} failed, retrying in {delay} seconds (attempt {attempt + 1}): {e}"
                    )
                    time.sleep(delay)
                    continue
                else:
                    if isinstance(e, APIError):
                        raise
                    else:
                        self._handle_yfinance_error(e, operation_name)

            except Exception as e:
                last_exception = e
                if attempt < self.MAX_RETRIES:
                    delay = self.RETRY_DELAY * (2**attempt)
                    self.logger.warning(
                        f"Unexpected error in {operation_name}, retrying in {delay} seconds (attempt {attempt + 1}): {e}"
                    )
                    time.sleep(delay)
                    continue
                else:
                    self._handle_yfinance_error(e, operation_name)

        # This should not be reached, but just in case
        raise APIError(
            f"{operation_name} failed after {self.MAX_RETRIES} retries: {last_exception}"
        )

    def _format_japanese_symbol(self, symbol: str) -> str:
        """
        Format Japanese stock symbol for yfinance

        Args:
            symbol: Stock code (e.g., "7203" or "7203.T")

        Returns:
            Formatted symbol for yfinance (e.g., "7203.T")
        """
        if "." not in symbol:
            # Default to Tokyo Stock Exchange
            return f"{symbol}.T"
        return symbol

    def get_stock_prices(self, symbol: str, period: str = "3y") -> pd.DataFrame:
        """
        Get stock price data for a specific Japanese stock

        Args:
            symbol: Stock symbol (e.g., "7203" or "7203.T")
            period: Period for historical data (e.g., "1y", "3y", "5y")

        Returns:
            DataFrame with stock price data (OHLCV)

        Raises:
            DataNotFoundError: If no data found for the symbol
            APIError: If data retrieval fails
        """

        def _fetch_stock_data():
            formatted_symbol = self._format_japanese_symbol(symbol)

            try:
                ticker = yf.Ticker(formatted_symbol)
                hist_data = ticker.history(period=period)

                if hist_data.empty:
                    raise DataNotFoundError(
                        f"No price data found for symbol {formatted_symbol}"
                    )

                # Reset index to make Date a column
                hist_data = hist_data.reset_index()

                # Rename columns to match expected format
                hist_data = hist_data.rename(
                    columns={
                        "Date": "Date",
                        "Open": "Open",
                        "High": "High",
                        "Low": "Low",
                        "Close": "Close",
                        "Volume": "Volume",
                    }
                )

                # Add symbol column
                hist_data["Symbol"] = formatted_symbol

                self.logger.info(
                    f"Retrieved {len(hist_data)} price records for {formatted_symbol}"
                )
                return hist_data

            except Exception as e:
                self._handle_yfinance_error(
                    e, "Stock price retrieval", formatted_symbol
                )

        return self._retry_operation(_fetch_stock_data, f"get_stock_prices({symbol})")

    def get_financial_info(self, symbol: str) -> Dict[str, Any]:
        """
        Get financial information for a specific stock (PER, PBR, etc.)

        Args:
            symbol: Stock symbol (e.g., "7203" or "7203.T")

        Returns:
            Dictionary with financial information

        Raises:
            DataNotFoundError: If no data found for the symbol
            APIError: If data retrieval fails
        """

        def _fetch_financial_info():
            formatted_symbol = self._format_japanese_symbol(symbol)

            try:
                ticker = yf.Ticker(formatted_symbol)
                info = ticker.info

                if not info or len(info) == 0:
                    raise DataNotFoundError(
                        f"No financial info found for symbol {formatted_symbol}"
                    )

                # Extract relevant financial metrics
                financial_info = {
                    "symbol": formatted_symbol,
                    "shortName": info.get("shortName", ""),
                    "longName": info.get("longName", ""),
                    "currentPrice": info.get("currentPrice"),
                    "previousClose": info.get("previousClose"),
                    "marketCap": info.get("marketCap"),
                    "trailingPE": info.get("trailingPE"),  # PER
                    "forwardPE": info.get("forwardPE"),
                    "priceToBook": info.get("priceToBook"),  # PBR
                    "dividendYield": info.get(
                        "dividendYield"
                    ),  # Dividend yield as decimal
                    "trailingAnnualDividendYield": info.get(
                        "trailingAnnualDividendYield"
                    ),
                    "trailingAnnualDividendRate": info.get(
                        "trailingAnnualDividendRate"
                    ),
                    "payoutRatio": info.get("payoutRatio"),
                    "totalRevenue": info.get("totalRevenue"),
                    "revenueGrowth": info.get("revenueGrowth"),
                    "earningsGrowth": info.get("earningsGrowth"),
                    "profitMargins": info.get("profitMargins"),
                    "operatingMargins": info.get("operatingMargins"),
                    "returnOnEquity": info.get("returnOnEquity"),
                    "returnOnAssets": info.get("returnOnAssets"),
                    "debtToEquity": info.get("debtToEquity"),
                    "currency": info.get("currency", "JPY"),
                    "exchange": info.get("exchange", ""),
                    "sector": info.get("sector", ""),
                    "industry": info.get("industry", ""),
                }

                self.logger.info(f"Retrieved financial info for {formatted_symbol}")
                return financial_info

            except Exception as e:
                self._handle_yfinance_error(
                    e, "Financial info retrieval", formatted_symbol
                )

        return self._retry_operation(
            _fetch_financial_info, f"get_financial_info({symbol})"
        )

    def get_dividend_history(self, symbol: str, period: str = "3y") -> pd.DataFrame:
        """
        Get dividend history data for a specific stock

        Args:
            symbol: Stock symbol (e.g., "7203" or "7203.T")
            period: Period for historical data (e.g., "1y", "3y", "5y")

        Returns:
            DataFrame with dividend history data

        Raises:
            DataNotFoundError: If no dividend data found for the symbol
            APIError: If data retrieval fails
        """

        def _fetch_dividend_data():
            formatted_symbol = self._format_japanese_symbol(symbol)

            try:
                ticker = yf.Ticker(formatted_symbol)
                dividends = ticker.dividends

                if dividends.empty:
                    # Not necessarily an error - some stocks don't pay dividends
                    self.logger.info(f"No dividend data found for {formatted_symbol}")
                    return pd.DataFrame(columns=["Date", "Dividends", "Symbol"])

                # Filter by period
                if period:
                    end_date = datetime.now()
                    if period.endswith("y"):
                        years = int(period[:-1])
                        start_date = end_date - timedelta(days=years * 365)
                    elif period.endswith("mo"):
                        months = int(period[:-2])
                        start_date = end_date - timedelta(days=months * 30)
                    else:
                        # Default to 3 years if period format is not recognized
                        start_date = end_date - timedelta(days=3 * 365)

                    dividends = dividends[dividends.index >= start_date]

                # Convert to DataFrame
                dividend_df = dividends.reset_index()
                dividend_df = dividend_df.rename(
                    columns={"Date": "Date", "Dividends": "Dividends"}
                )
                dividend_df["Symbol"] = formatted_symbol

                self.logger.info(
                    f"Retrieved {len(dividend_df)} dividend records for {formatted_symbol}"
                )
                return dividend_df

            except Exception as e:
                self._handle_yfinance_error(
                    e, "Dividend history retrieval", formatted_symbol
                )

        return self._retry_operation(
            _fetch_dividend_data, f"get_dividend_history({symbol})"
        )

    def get_japanese_stock_list(self) -> List[str]:
        """
        Get list of Japanese stock symbols

        Note: This is a simplified implementation. In practice, you might want to
        use a more comprehensive source for Japanese stock listings.

        Returns:
            List of Japanese stock symbols with .T suffix

        Raises:
            APIError: If stock list retrieval fails
        """

        def _fetch_stock_list():
            # Check cache first
            if (
                self._japanese_stocks_cache is not None
                and self._cache_timestamp is not None
                and datetime.now() - self._cache_timestamp < self._cache_duration
            ):
                self.logger.info(
                    f"Using cached Japanese stock list ({len(self._japanese_stocks_cache)} stocks)"
                )
                return self._japanese_stocks_cache

            try:
                # This is a simplified list of major Japanese stocks
                # In a production system, you would fetch this from a comprehensive source
                major_japanese_stocks = [
                    "7203.T",  # Toyota Motor
                    "6758.T",  # Sony Group
                    "9984.T",  # SoftBank Group
                    "6861.T",  # Keyence
                    "8306.T",  # Mitsubishi UFJ Financial Group
                    "7974.T",  # Nintendo
                    "9432.T",  # NTT
                    "4063.T",  # Shin-Etsu Chemical
                    "6954.T",  # Fanuc
                    "8035.T",  # Tokyo Electron
                    "4502.T",  # Takeda Pharmaceutical
                    "6098.T",  # Recruit Holdings
                    "4568.T",  # Daiichi Sankyo
                    "8058.T",  # Mitsubishi Corp
                    "7267.T",  # Honda Motor
                    "9983.T",  # Fast Retailing
                    "4519.T",  # Chugai Pharmaceutical
                    "6367.T",  # Daikin Industries
                    "8031.T",  # Mitsui & Co
                    "4578.T",  # Otsuka Holdings
                ]

                # Update cache
                self._japanese_stocks_cache = major_japanese_stocks
                self._cache_timestamp = datetime.now()

                self.logger.info(
                    f"Retrieved Japanese stock list ({len(major_japanese_stocks)} stocks)"
                )
                return major_japanese_stocks

            except Exception as e:
                self._handle_yfinance_error(e, "Japanese stock list retrieval")

        return self._retry_operation(_fetch_stock_list, "get_japanese_stock_list")

    def get_multiple_stocks_info(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get financial information for multiple stocks efficiently

        Args:
            symbols: List of stock symbols

        Returns:
            Dictionary mapping symbols to their financial info

        Raises:
            APIError: If data retrieval fails
        """

        def _fetch_multiple_info():
            results = {}

            for symbol in symbols:
                try:
                    info = self.get_financial_info(symbol)
                    results[symbol] = info
                except DataNotFoundError:
                    # Skip stocks with no data
                    self.logger.warning(f"No data found for {symbol}, skipping")
                    continue
                except Exception as e:
                    self.logger.error(f"Error fetching data for {symbol}: {e}")
                    continue

                # Add small delay to avoid rate limiting
                time.sleep(0.1)

            self.logger.info(
                f"Retrieved info for {len(results)} out of {len(symbols)} stocks"
            )
            return results

        return self._retry_operation(
            _fetch_multiple_info, f"get_multiple_stocks_info({len(symbols)} symbols)"
        )

    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate if a stock symbol exists and has data

        Args:
            symbol: Stock symbol to validate

        Returns:
            True if symbol is valid and has data, False otherwise
        """
        try:
            formatted_symbol = self._format_japanese_symbol(symbol)
            ticker = yf.Ticker(formatted_symbol)
            info = ticker.info

            # Check if we got meaningful data
            return bool(info and info.get("symbol") or info.get("shortName"))

        except Exception:
            return False
