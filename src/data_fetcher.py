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
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd
import yfinance as yf
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout

from .cache_manager import CacheManager
from .timezone_handler import TimezoneHandler
from .symbol_validator import SymbolValidator
from .symbol_filter import SymbolFilter, FilteringMode
from .data_validator import DataValidator, ValidationConfig
from .validation_error_processor import ValidationErrorProcessor, ProcessingConfig
from .exceptions import APIError, AuthenticationError, RateLimitError, DataNotFoundError
from .enhanced_logger import EnhancedLogger
from .error_metrics import ErrorType
from .retry_manager import RetryManager, RetryConfig, create_api_retry_manager
from .error_handler import (
    EnhancedErrorHandler,
    ProcessingConfig as ErrorProcessingConfig,
)
from .error_handling_config import ErrorHandlingConfig, ErrorHandlingConfigManager
from .tse_stock_list_manager import TSEStockListManager
from .models import TSEDataConfig


class DataFetcher:
    """
    Handles data retrieval from Yahoo Finance using yfinance library

    Provides methods for:
    - Stock price data retrieval for Japanese stocks
    - Financial information retrieval (PER, PBR, dividend yield, etc.)
    - Dividend history data retrieval
    - Japanese stock list management
    - Comprehensive error handling and retry logic with processing continuation
    """

    # Common Japanese stock exchanges and their suffixes
    JAPANESE_EXCHANGES = {
        "TSE": ".T",  # Tokyo Stock Exchange
        "OSE": ".OS",  # Osaka Stock Exchange
        "NSE": ".NS",  # Nagoya Stock Exchange
    }

    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        error_config: Optional[ErrorProcessingConfig] = None,
        error_handling_config: Optional["ErrorHandlingConfig"] = None,
        enable_enhanced_features: bool = True,
        tse_config: Optional[TSEDataConfig] = None,
    ):
        """
        Initialize DataFetcher for yfinance with enhanced error handling integration

        Args:
            retry_config: Configuration for retry behavior (legacy)
            error_config: Configuration for error handling and processing continuation (legacy)
            error_handling_config: New comprehensive error handling configuration
            enable_enhanced_features: Enable new error handling features (default: True)
            tse_config: Configuration for TSE stock list management

        Note: yfinance doesn't require authentication tokens

        Integration Features:
        - Backward compatibility with existing configurations
        - Optional enhanced error handling with ErrorHandlingConfig
        - Configurable feature enablement for gradual rollout
        - TSE official stock list integration
        """
        self.logger = logging.getLogger(__name__)

        # Feature flags for gradual rollout
        self.enable_enhanced_features = enable_enhanced_features

        # Initialize enhanced logging with error metrics
        self.enhanced_logger = EnhancedLogger(logger_name=f"{__name__}.enhanced")

        # Initialize cache manager
        self.cache_manager = CacheManager()

        # Initialize timezone handler
        self.timezone_handler = TimezoneHandler()

        # Initialize symbol validator
        self.symbol_validator = SymbolValidator()

        # Initialize symbol filter with enhanced functionality
        self.symbol_filter = SymbolFilter(
            symbol_validator=self.symbol_validator,
            error_metrics=self.enhanced_logger.get_error_metrics(),
            filtering_mode=FilteringMode.TOLERANT,
            high_filter_rate_threshold=0.3,  # 30% filter rate triggers alert
            empty_list_alert=True,
        )

        # Initialize data validator and error processor
        self.data_validator = DataValidator()
        self.validation_error_processor = ValidationErrorProcessor(self.data_validator)

        # Initialize TSE stock list manager
        self.tse_manager = TSEStockListManager(config=tse_config, logger=self.logger)

        # Enhanced error handling configuration integration
        self._setup_error_handling(retry_config, error_config, error_handling_config)

        # Cache for Japanese stock list to avoid repeated API calls
        self._japanese_stocks_cache: Optional[List[str]] = None
        self._cache_timestamp: Optional[datetime] = None
        self._cache_duration = timedelta(hours=24)  # Cache for 24 hours

    def _setup_error_handling(
        self,
        retry_config: Optional[RetryConfig] = None,
        error_config: Optional[ErrorProcessingConfig] = None,
        error_handling_config: Optional[ErrorHandlingConfig] = None,
    ) -> None:
        """
        Setup error handling components with backward compatibility and enhanced features.

        Args:
            retry_config: Legacy retry configuration
            error_config: Legacy error processing configuration
            error_handling_config: New comprehensive error handling configuration
        """
        if self.enable_enhanced_features and error_handling_config:
            # Use new comprehensive error handling configuration
            self.error_handling_config = error_handling_config

            # Initialize retry manager with new config
            self.retry_manager = RetryManager(error_handling_config.retry_config)

            # Initialize error handler with converted config
            self.error_handler = EnhancedErrorHandler(
                error_handling_config.to_processing_config(), self.retry_manager
            )

            # Configure symbol filtering based on error handling mode
            if error_handling_config.mode.value == "strict":
                self.symbol_filter.configure_filtering(
                    filtering_mode=FilteringMode.STRICT,
                    high_filter_rate_threshold=0.1,
                    empty_list_alert=True,
                )
            elif error_handling_config.mode.value == "debug":
                self.symbol_filter.configure_filtering(
                    filtering_mode=FilteringMode.PERMISSIVE,
                    high_filter_rate_threshold=0.5,
                    empty_list_alert=True,
                )

            # Configure validation based on error handling mode
            self.configure_validation(
                strict_mode=(error_handling_config.mode.value == "strict"),
                continue_on_error=error_handling_config.continue_on_individual_error,
            )

            self.logger.info(
                f"Enhanced error handling configured - Mode: {error_handling_config.mode.value}, "
                f"Continue on error: {error_handling_config.continue_on_individual_error}"
            )

        else:
            # Use legacy configuration for backward compatibility
            self.error_handling_config = None

            # Initialize retry manager and error handler with legacy configs
            self.retry_manager = (
                RetryManager(retry_config)
                if retry_config
                else create_api_retry_manager()
            )
            self.error_handler = EnhancedErrorHandler(error_config, self.retry_manager)

            self.logger.info("Legacy error handling configuration used")

    def configure_enhanced_error_handling(
        self, config: ErrorHandlingConfig, apply_immediately: bool = True
    ) -> None:
        """
        Configure enhanced error handling after initialization.

        Args:
            config: New error handling configuration
            apply_immediately: Whether to apply configuration immediately

        Implements requirement 7.1 for runtime configuration updates.
        """
        if not self.enable_enhanced_features:
            self.logger.warning("Enhanced features are disabled, configuration ignored")
            return

        self.error_handling_config = config

        if apply_immediately:
            # Reconfigure components with new settings
            self.retry_manager = RetryManager(config.retry_config)
            self.error_handler = EnhancedErrorHandler(
                config.to_processing_config(), self.retry_manager
            )

            # Update symbol filtering
            if config.mode.value == "strict":
                filtering_mode = FilteringMode.STRICT
                threshold = 0.1
            elif config.mode.value == "debug":
                filtering_mode = FilteringMode.PERMISSIVE
                threshold = 0.5
            else:
                filtering_mode = FilteringMode.TOLERANT
                threshold = 0.3

            self.symbol_filter.configure_filtering(
                filtering_mode=filtering_mode,
                high_filter_rate_threshold=threshold,
                empty_list_alert=True,
            )

            # Update validation settings
            self.configure_validation(
                strict_mode=(config.mode.value == "strict"),
                continue_on_error=config.continue_on_individual_error,
            )

            self.logger.info(
                f"Enhanced error handling reconfigured - Mode: {config.mode.value}"
            )

    def get_error_handling_status(self) -> Dict[str, Any]:
        """
        Get current error handling configuration and status.

        Returns:
            Dictionary with error handling status information

        Implements requirement 7.5 for configuration monitoring.
        """
        status = {
            "enhanced_features_enabled": self.enable_enhanced_features,
            "has_enhanced_config": self.error_handling_config is not None,
            "error_metrics_enabled": True,
            "components": {
                "retry_manager": bool(self.retry_manager),
                "error_handler": bool(self.error_handler),
                "symbol_filter": bool(self.symbol_filter),
                "data_validator": bool(self.data_validator),
                "validation_error_processor": bool(self.validation_error_processor),
            },
        }

        if self.error_handling_config:
            status["current_config"] = (
                self.error_handling_config.get_configuration_summary()
            )

        # Add error metrics summary
        error_metrics = self.enhanced_logger.get_error_metrics()
        status["error_metrics"] = error_metrics.get_error_summary(timedelta(hours=1))

        # Add retry statistics
        status["retry_statistics"] = self.retry_manager.get_retry_statistics()

        # Add error handling statistics
        status["error_handling_statistics"] = self.error_handler.get_error_statistics()

        return status

    def _handle_yfinance_error(
        self, error: Exception, operation: str, symbol: str = ""
    ) -> None:
        """
        Enhanced error handling for yfinance/requests errors with improved delisted stock detection

        Args:
            error: The original exception
            operation: Description of the operation that failed
            symbol: Stock symbol if applicable

        Raises:
            RateLimitError: For rate limiting errors
            DataNotFoundError: For data not found errors (including delisted stocks)
            APIError: For other API errors
        """
        symbol_info = f" for symbol {symbol}" if symbol else ""
        error_message = f"{operation}{symbol_info} failed: {str(error)}"

        # Enhanced delisted stock detection - check error message for various indicators
        error_str = str(error).lower()
        delisted_indicators = [
            "possibly delisted",
            "delisted",
            "no data found",
            "ticker not found",
            "invalid ticker",
            "not found",
            "no price data found",
            "no timezone found",
            "empty dataframe",
            "no data available",
            "symbol may be delisted",
            "ticker symbol not found",
            "invalid symbol",
            "data doesn't exist",
            "no such ticker",
        ]

        if any(indicator in error_str for indicator in delisted_indicators):
            # Log detailed information about delisted stock detection using enhanced logger
            self.enhanced_logger.log_delisted_stock_error(
                symbol=symbol,
                operation=operation,
                error=error,
                error_indicators=[
                    indicator
                    for indicator in delisted_indicators
                    if indicator in error_str
                ],
                additional_context={
                    "original_error_type": type(error).__name__,
                    "full_error_message": str(error),
                    "operation_context": operation,
                },
            )

            # Also log to standard logger for backward compatibility
            self.logger.warning(
                f"Delisted/invalid stock detected - Symbol: {symbol}, "
                f"Operation: {operation}, Error: {str(error)}, "
                f"Timestamp: {datetime.now().isoformat()}"
            )
            raise DataNotFoundError(
                f"Stock {symbol} appears to be delisted or invalid",
                response_data={
                    "symbol": symbol,
                    "operation": operation,
                    "original_error": str(error),
                },
            )

        # Handle HTTP errors with enhanced 404 detection
        if isinstance(error, HTTPError):
            if hasattr(error, "response") and error.response is not None:
                status_code = error.response.status_code
                if status_code == 404:
                    # Log detailed 404 error using enhanced logger
                    self.enhanced_logger.log_delisted_stock_error(
                        symbol=symbol,
                        operation=operation,
                        error=error,
                        error_indicators=["404_not_found"],
                        additional_context={
                            "status_code": status_code,
                            "http_error": True,
                            "response_available": True,
                        },
                    )

                    # Also log to standard logger
                    self.logger.warning(
                        f"404 Error - Stock not found - Symbol: {symbol}, "
                        f"Operation: {operation}, Timestamp: {datetime.now().isoformat()}"
                    )
                    raise DataNotFoundError(
                        f"Stock {symbol} not found (404 error)",
                        status_code=status_code,
                        response_data={"symbol": symbol, "operation": operation},
                    )
                elif status_code == 429:
                    self.logger.warning(f"Rate limit exceeded: {error_message}")
                    raise RateLimitError(error_message, status_code)
                else:
                    self.logger.error(f"HTTP error: {error_message}")
                    raise APIError(error_message, status_code)
            else:
                self.logger.error(f"HTTP error: {error_message}")
                raise APIError(error_message)
        elif isinstance(error, (ConnectionError, Timeout)):
            self.logger.error(f"Network error: {error_message}")
            raise APIError(error_message)
        else:
            self.logger.error(f"Unexpected error: {error_message}")
            raise APIError(error_message)

    def _retry_operation(self, operation_func, operation_name: str, *args, **kwargs):
        """
        Execute operation with enhanced retry logic using RetryManager

        Args:
            operation_func: Function to execute
            operation_name: Name of the operation for logging
            *args, **kwargs: Arguments to pass to the operation function

        Returns:
            Result of the operation function

        Raises:
            APIError: If all retry attempts fail
        """
        retry_result = self.retry_manager.execute_with_retry(
            operation_func, operation_name, *args, **kwargs
        )

        if retry_result.success:
            return retry_result.result
        else:
            # Re-raise the final error
            raise retry_result.final_error

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
        Get stock price data for a specific Japanese stock with enhanced delisted stock handling

        Args:
            symbol: Stock symbol (e.g., "7203" or "7203.T")
            period: Period for historical data (e.g., "1y", "3y", "5y")

        Returns:
            DataFrame with stock price data (OHLCV)

        Raises:
            DataNotFoundError: If no data found for the symbol (including delisted stocks)
            APIError: If data retrieval fails
        """

        def _fetch_stock_data():
            formatted_symbol = self._format_japanese_symbol(symbol)

            try:
                ticker = yf.Ticker(formatted_symbol)
                hist_data = ticker.history(period=period)

                # Enhanced empty data detection for delisted stocks
                if hist_data.empty:
                    self.logger.warning(
                        f"Empty price data detected - Symbol: {formatted_symbol}, "
                        f"Period: {period}, Timestamp: {datetime.now().isoformat()}"
                    )
                    raise DataNotFoundError(
                        f"No price data found for symbol {formatted_symbol} - stock may be delisted or invalid",
                        response_data={
                            "symbol": formatted_symbol,
                            "period": period,
                            "data_type": "price",
                        },
                    )

                # Check for insufficient data (might indicate delisting)
                if len(hist_data) < 5:  # Less than 5 trading days of data
                    self.logger.warning(
                        f"Insufficient price data - Symbol: {formatted_symbol}, "
                        f"Records: {len(hist_data)}, Period: {period}, "
                        f"Timestamp: {datetime.now().isoformat()}"
                    )
                    # Still return the data but log the warning

                # Check for data quality issues
                if hist_data["Close"].isna().all():
                    self.logger.warning(
                        f"All closing prices are NaN - Symbol: {formatted_symbol}, "
                        f"Timestamp: {datetime.now().isoformat()}"
                    )
                    raise DataNotFoundError(
                        f"Invalid price data for symbol {formatted_symbol} - all closing prices are NaN",
                        response_data={
                            "symbol": formatted_symbol,
                            "issue": "all_nan_prices",
                        },
                    )

                # Reset index to make Date a column
                hist_data = hist_data.reset_index()

                # Ensure the date column is named 'Date'
                if "index" in hist_data.columns:
                    hist_data = hist_data.rename(columns={"index": "Date"})
                elif hist_data.index.name and hist_data.index.name != "Date":
                    hist_data = hist_data.rename(columns={hist_data.index.name: "Date"})

                # Rename columns to match expected format
                hist_data = hist_data.rename(
                    columns={
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

            except DataNotFoundError:
                # Re-raise DataNotFoundError as-is
                raise
            except Exception as e:
                self._handle_yfinance_error(
                    e, "Stock price retrieval", formatted_symbol
                )

        return self._retry_operation(_fetch_stock_data, f"get_stock_prices({symbol})")

    def get_financial_info(self, symbol: str) -> Dict[str, Any]:
        """
        Get financial information for a specific stock with enhanced delisted stock handling

        Args:
            symbol: Stock symbol (e.g., "7203" or "7203.T")

        Returns:
            Dictionary with financial information

        Raises:
            DataNotFoundError: If no data found for the symbol (including delisted stocks)
            APIError: If data retrieval fails
        """
        formatted_symbol = self._format_japanese_symbol(symbol)

        # Try to get from cache first
        cached_data = self.cache_manager.get_cached_financial_info(formatted_symbol)
        if cached_data is not None:
            return cached_data

        def _fetch_financial_info():
            try:
                ticker = yf.Ticker(formatted_symbol)
                info = ticker.info

                # Enhanced empty data detection
                if not info or len(info) == 0:
                    self.logger.warning(
                        f"Empty financial info - Symbol: {formatted_symbol}, "
                        f"Timestamp: {datetime.now().isoformat()}"
                    )
                    raise DataNotFoundError(
                        f"No financial info found for symbol {formatted_symbol} - stock may be delisted or invalid",
                        response_data={
                            "symbol": formatted_symbol,
                            "data_type": "financial_info",
                        },
                    )

                # Check for minimal required data to detect invalid/delisted stocks
                essential_fields = ["symbol", "shortName", "longName"]
                has_essential_data = any(info.get(field) for field in essential_fields)

                if not has_essential_data:
                    self.logger.warning(
                        f"Insufficient financial info - Symbol: {formatted_symbol}, "
                        f"Available keys: {list(info.keys())[:10]}, "  # Log first 10 keys
                        f"Timestamp: {datetime.now().isoformat()}"
                    )
                    raise DataNotFoundError(
                        f"Insufficient financial info for symbol {formatted_symbol} - stock may be delisted",
                        response_data={
                            "symbol": formatted_symbol,
                            "available_keys": list(info.keys())[:10],
                        },
                    )

                # Check for "possibly delisted" or similar messages in the info
                if (
                    info.get("regularMarketPrice") is None
                    and info.get("currentPrice") is None
                ):
                    self.logger.warning(
                        f"No current price data - Symbol: {formatted_symbol}, "
                        f"Timestamp: {datetime.now().isoformat()}"
                    )
                    # Don't raise error here as some stocks might not have current price but still be valid

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

                # Cache the result
                self.cache_manager.cache_financial_info(
                    formatted_symbol, financial_info
                )

                self.logger.info(f"Retrieved financial info for {formatted_symbol}")
                return financial_info

            except DataNotFoundError:
                # Re-raise DataNotFoundError as-is
                raise
            except Exception as e:
                self._handle_yfinance_error(
                    e, "Financial info retrieval", formatted_symbol
                )

        return self._retry_operation(
            _fetch_financial_info, f"get_financial_info({symbol})"
        )

    def get_dividend_history(self, symbol: str, period: str = "1y") -> pd.DataFrame:
        """
        Get dividend history data for a specific stock with improved timezone handling

        Args:
            symbol: Stock symbol (e.g., "7203" or "7203.T")
            period: Period for historical data (e.g., "1y", "2y", "3y") - default 1y for performance

        Returns:
            DataFrame with dividend history data

        Raises:
            DataNotFoundError: If no dividend data found for the symbol
            APIError: If data retrieval fails
        """
        formatted_symbol = self._format_japanese_symbol(symbol)

        # Try to get from cache first
        cached_data = self.cache_manager.get_cached_dividend_history(formatted_symbol)
        if cached_data is not None:
            return cached_data

        def _fetch_dividend_data():
            try:
                ticker = yf.Ticker(formatted_symbol)
                dividends = ticker.dividends

                if dividends.empty:
                    # Not necessarily an error - some stocks don't pay dividends
                    self.logger.info(f"No dividend data found for {formatted_symbol}")
                    empty_df = pd.DataFrame(columns=["Date", "Dividends", "Symbol"])
                    # Cache empty result
                    self.cache_manager.cache_dividend_history(
                        formatted_symbol, empty_df
                    )
                    return empty_df

                # Filter by period with improved timezone handling
                if period:
                    try:
                        # Get current time in UTC to avoid timezone issues
                        end_date = pd.Timestamp.now(tz="UTC")
                        if period.endswith("y"):
                            years = int(period[:-1])
                            start_date = end_date - pd.Timedelta(days=years * 365)
                        elif period.endswith("mo"):
                            months = int(period[:-2])
                            start_date = end_date - pd.Timedelta(days=months * 30)
                        else:
                            # Default to 1 year if period format is not recognized
                            start_date = end_date - pd.Timedelta(days=365)

                        # Ensure start_date is timezone-aware in UTC
                        if start_date.tz is None:
                            start_date = start_date.tz_localize("UTC")
                        elif start_date.tz != pd.Timestamp.now(tz="UTC").tz:
                            start_date = start_date.tz_convert("UTC")

                        # Handle timezone compatibility for filtering
                        if (
                            hasattr(dividends.index, "tz")
                            and dividends.index.tz is not None
                        ):
                            # Convert dividends index to UTC for comparison
                            dividends_utc = dividends.copy()
                            dividends_utc.index = dividends_utc.index.tz_convert("UTC")
                            # Filter using UTC timestamps - both are now in UTC
                            dividends = dividends_utc[dividends_utc.index >= start_date]
                        else:
                            # If dividends index is timezone-naive, convert to UTC
                            dividends_with_tz = dividends.copy()
                            dividends_with_tz.index = pd.to_datetime(
                                dividends_with_tz.index
                            ).tz_localize("UTC")
                            # Filter using UTC timestamps - both are now in UTC
                            dividends = dividends_with_tz[
                                dividends_with_tz.index >= start_date
                            ]

                    except Exception as tz_error:
                        # Log detailed timezone error using enhanced logger
                        self.enhanced_logger.log_timezone_error(
                            symbol=formatted_symbol,
                            operation="dividend_history_filtering",
                            error=tz_error,
                            timezone_info={
                                "dividends_index_tz": str(
                                    getattr(dividends.index, "tz", "None")
                                ),
                                "start_date_tz": str(
                                    getattr(start_date, "tzinfo", "None")
                                ),
                                "period": period,
                                "filtering_method": "utc_conversion",
                            },
                            fallback_action="skip_date_filtering",
                            additional_context={
                                "dividends_length": len(dividends),
                                "start_date": str(start_date),
                            },
                        )

                        # Also log to standard logger for backward compatibility
                        self.logger.warning(
                            f"Timezone filtering failed for {formatted_symbol}: {tz_error}. Using all available dividend data."
                        )
                        # Fallback: use all available dividend data without filtering

                # Convert to DataFrame
                dividend_df = dividends.reset_index()
                dividend_df = dividend_df.rename(
                    columns={"Date": "Date", "Dividends": "Dividends"}
                )
                dividend_df["Symbol"] = formatted_symbol

                # Cache the result
                self.cache_manager.cache_dividend_history(formatted_symbol, dividend_df)

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

    def get_japanese_stock_list(self, mode: str = "curated") -> List[str]:
        """
        Get list of Japanese stock symbols from TSE (Tokyo Stock Exchange)

        Args:
            mode: "curated" for selected stocks (~130), "all" for all TSE stocks (~3800),
                  "tse_official" for TSE official stock list (excluding ETFs/REITs)

        Returns:
            List of Japanese stock symbols with .T suffix

        Raises:
            APIError: If stock list retrieval fails
        """

        def _fetch_stock_list():
            # Check cache first
            cache_key = f"{mode}_stocks"
            if (
                hasattr(self, f"_{cache_key}_cache")
                and getattr(self, f"_{cache_key}_cache") is not None
                and self._cache_timestamp is not None
                and datetime.now() - self._cache_timestamp < self._cache_duration
            ):
                cached_stocks = getattr(self, f"_{cache_key}_cache")
                self.logger.info(
                    f"Using cached Japanese stock list ({len(cached_stocks)} stocks, mode: {mode})"
                )
                return cached_stocks

            try:
                if mode == "tse_official":
                    return self._get_tse_official_stocks()
                elif mode == "all":
                    return self._get_all_tse_stocks()
                else:
                    return self._get_curated_stocks()

            except Exception as e:
                self._handle_yfinance_error(e, "Japanese stock list retrieval")

        return self._retry_operation(
            _fetch_stock_list, f"get_japanese_stock_list(mode={mode})"
        )

        return self._retry_operation(
            _fetch_stock_list, f"get_japanese_stock_list(mode={mode})"
        )

    def _get_all_tse_stocks(self) -> List[str]:
        """
        Get actual TSE listed stocks using smart range validation.

        Returns:
            List of valid TSE stock symbols (~600-800 stocks)
        """
        cache_file = "cache/tse_stocks_cache.json"
        cache_duration = timedelta(hours=24)

        # Try cache first
        cached_stocks = self._get_cached_tse_stocks(cache_file, cache_duration)
        if cached_stocks:
            self.logger.info(f"Using cached TSE stocks: {len(cached_stocks)} stocks")
            return cached_stocks

        self.logger.info("Fetching fresh TSE stock list using smart validation...")

        # Known TSE sector ranges with high validity rates
        tse_ranges = [
            (1300, 1400, "Construction"),
            (1800, 1900, "Construction"),
            (2000, 2100, "Food"),
            (2500, 2600, "Food & Beverages"),
            (2800, 2900, "Food"),
            (3000, 3100, "Textiles"),
            (3400, 3500, "Chemicals"),
            (3600, 3700, "Steel"),
            (3800, 3900, "Machinery"),
            (4000, 4100, "Chemicals"),
            (4500, 4600, "Pharmaceuticals"),
            (4900, 5000, "Chemicals"),
            (5000, 5100, "Steel"),
            (5400, 5500, "Steel"),
            (5700, 5800, "Glass & Ceramics"),
            (6000, 6100, "Machinery"),
            (6300, 6400, "Machinery"),
            (6500, 6600, "Electronics"),
            (6700, 6800, "Electronics"),
            (6900, 7000, "Electronics"),
            (7000, 7100, "Transportation Equipment"),
            (7200, 7300, "Transportation Equipment"),
            (7500, 7600, "Precision Instruments"),
            (7700, 7800, "Precision Instruments"),
            (7900, 8000, "Other Products"),
            (8000, 8100, "Trading Companies"),
            (8300, 8400, "Banks"),
            (8500, 8600, "Other Financing"),
            (8700, 8800, "Insurance"),
            (8800, 8900, "Real Estate"),
            (9000, 9100, "Transportation"),
            (9200, 9300, "Transportation"),
            (9400, 9500, "Information & Communication"),
            (9500, 9600, "Electric Power & Gas"),
            (9700, 9800, "Services"),
            (9900, 10000, "Services"),
        ]

        valid_stocks = []
        total_tested = 0

        for start, end, sector in tse_ranges:
            self.logger.info(f"Validating {sector} range ({start}-{end})...")

            range_valid = 0
            for code in range(start, end):  # Get all stocks in range
                symbol = f"{code}.T"

                if self._validate_tse_stock_quickly(symbol):
                    valid_stocks.append(symbol)
                    range_valid += 1

                total_tested += 1

                # Rate limiting to avoid overwhelming the API
                time.sleep(0.1)

            self.logger.debug(f"  {sector}: {range_valid} valid stocks found")

        success_rate = len(valid_stocks) / total_tested * 100 if total_tested > 0 else 0
        self.logger.info(
            f"TSE validation complete: {len(valid_stocks)}/{total_tested} "
            f"valid stocks ({success_rate:.1f}% success rate)"
        )

        # Cache results for future use
        self._cache_tse_stocks(cache_file, valid_stocks)

        # Update instance cache
        self._japanese_stocks_cache = valid_stocks
        self._cache_timestamp = datetime.now()

        daily_stocks = len(valid_stocks) // 5
        self.logger.info(
            f"Optimized for rotation: ~{daily_stocks} stocks per day with 5-day rotation"
        )

        return valid_stocks

    def _validate_tse_stock_quickly(self, symbol: str) -> bool:
        """
        Quick validation of TSE stock symbol using yfinance

        Args:
            symbol: Stock symbol (e.g., "7203.T")

        Returns:
            bool: True if stock is valid and active on TSE
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # Validation criteria for active TSE stocks
            return (
                info
                and len(info) > 5  # Has substantial info
                and info.get("shortName")  # Has a name
                and info.get("exchange") == "JPX"  # Is on Japanese exchange
                and info.get("symbol")  # Has symbol info
            )
        except Exception:
            return False

    def _get_cached_tse_stocks(
        self, cache_file: str, cache_duration: timedelta
    ) -> Optional[List[str]]:
        """
        Get cached TSE stocks if available and fresh

        Args:
            cache_file: Path to cache file
            cache_duration: Maximum age of cache

        Returns:
            List of cached stocks or None if cache is invalid/expired
        """
        try:
            import os
            import json

            if not os.path.exists(cache_file):
                return None

            with open(cache_file, "r") as f:
                cache_data = json.load(f)

            cache_time = datetime.fromisoformat(cache_data["timestamp"])
            if datetime.now() - cache_time < cache_duration:
                return cache_data["stocks"]
            else:
                self.logger.info("TSE stock cache expired, will refresh")
                return None

        except Exception as e:
            self.logger.warning(f"Error reading TSE stock cache: {e}")
            return None

    def _cache_tse_stocks(self, cache_file: str, stocks: List[str]) -> None:
        """
        Cache TSE stocks for future use

        Args:
            cache_file: Path to cache file
            stocks: List of valid TSE stocks to cache
        """
        try:
            import os
            import json

            # Ensure cache directory exists
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)

            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "stocks": stocks,
                "count": len(stocks),
                "source": "smart_range_validation",
                "note": "Valid TSE stocks validated with yfinance",
            }

            with open(cache_file, "w") as f:
                json.dump(cache_data, f, indent=2)

            self.logger.info(f"Cached {len(stocks)} TSE stocks to {cache_file}")

        except Exception as e:
            self.logger.error(f"Error caching TSE stocks: {e}")

    def _get_curated_stocks(self) -> List[str]:
        """Get curated list of major Japanese stocks."""
        # Comprehensive list of Japanese stocks across different market caps and sectors
        # This includes Nikkei 225 components, TOPIX Core 30, and other major stocks

        # Large Cap Stocks (Market Cap > 1 Trillion JPY)
        large_cap_stocks = [
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
            "9434.T",  # SoftBank
            "8316.T",  # Sumitomo Mitsui Financial Group
            "6981.T",  # Murata Manufacturing
            "4661.T",  # Oriental Land
            "6273.T",  # SMC
            "7751.T",  # Canon
            "6594.T",  # Nidec
            "4543.T",  # Terumo
            "6762.T",  # TDK
            "7733.T",  # Olympus
        ]

        # Mid Cap Stocks (Market Cap 100B - 1T JPY)
        mid_cap_stocks = [
            "8001.T",  # Itochu
            "8002.T",  # Marubeni
            "8053.T",  # Sumitomo Corp
            "5401.T",  # Nippon Steel
            "5411.T",  # JFE Holdings
            "3382.T",  # Seven & i Holdings
            "2914.T",  # Japan Tobacco
            "4452.T",  # Kao
            "4911.T",  # Shiseido
            "7201.T",  # Nissan Motor
            "7202.T",  # Isuzu Motors
            "7269.T",  # Suzuki Motor
            "7270.T",  # Subaru
            "9020.T",  # JR East
            "9021.T",  # JR Central
            "9022.T",  # JR West
            "9501.T",  # Tokyo Electric Power
            "9502.T",  # Chubu Electric Power
            "9503.T",  # Kansai Electric Power
            "3659.T",  # Nexon
            "4324.T",  # Dentsu Group
            "4385.T",  # Mercari
            "6503.T",  # Mitsubishi Electric
            "6504.T",  # Fuji Electric
            "6701.T",  # NEC
            "6702.T",  # Fujitsu
            "6724.T",  # Seiko Epson
            "6752.T",  # Panasonic Holdings
            "6753.T",  # Sharp
            "6770.T",  # Alps Alpine
        ]

        # Technology & Growth Stocks
        tech_stocks = [
            "4755.T",  # Rakuten Group
            "3765.T",  # Gung Ho Online Entertainment
            "3632.T",  # Gree
            "4689.T",  # Yahoo Japan (Z Holdings)
            "4704.T",  # Trend Micro
            "4751.T",  # CyberAgent
            "6178.T",  # Japan Post Holdings
            "6326.T",  # Kubota
            "6448.T",  # Brother Industries
            "6479.T",  # Minebea Mitsumi
            "6501.T",  # Hitachi
            "6502.T",  # Toshiba
            "6645.T",  # Omron
            "6674.T",  # GS Yuasa
            "6723.T",  # Renesas Electronics
            "6758.T",  # Sony Group
            "6841.T",  # Yokogawa Electric
            "6856.T",  # Horiba
            "6857.T",  # Advantest
            "6971.T",  # Kyocera
        ]

        # Financial Sector
        financial_stocks = [
            "8301.T",  # Nomura Holdings
            "8303.T",  # SBI Holdings
            "8304.T",  # Aozora Bank
            "8308.T",  # Resona Holdings
            "8309.T",  # Sumitomo Mitsui Trust Holdings
            "8354.T",  # Fukuoka Financial Group
            "8355.T",  # Shizuoka Bank
            "8411.T",  # Mizuho Financial Group
            "8473.T",  # SBI Holdings
            "8591.T",  # Orix
            "8604.T",  # Nomura Real Estate Holdings
            "8628.T",  # Matsui Securities
            "8630.T",  # SompoHoldings
            "8725.T",  # MS&AD Insurance Group Holdings
            "8750.T",  # Dai-ichi Life Holdings
            "8766.T",  # Tokio Marine Holdings
            "8795.T",  # T&D Holdings
        ]

        # Consumer & Retail
        consumer_stocks = [
            "2269.T",  # Meiji Holdings
            "2282.T",  # NH Foods
            "2501.T",  # Sapporo Holdings
            "2502.T",  # Asahi Group Holdings
            "2503.T",  # Kirin Holdings
            "2801.T",  # Kikkoman
            "2802.T",  # Ajinomoto
            "2871.T",  # Nichirei
            "2914.T",  # Japan Tobacco
            "3086.T",  # J.Front Retailing
            "3099.T",  # Isetan Mitsukoshi Holdings
            "3141.T",  # Welcia Holdings
            "3167.T",  # TOKAIホールディングス
            "3349.T",  # Cosmo Energy Holdings
            "3401.T",  # Teijin
            "3402.T",  # Toray Industries
            "3861.T",  # Oji Holdings
            "4188.T",  # Mitsubishi Chemical Holdings
            "4208.T",  # UBE Industries
            "7011.T",  # Mitsubishi Heavy Industries
        ]

        # Real Estate & Construction
        real_estate_stocks = [
            "1332.T",  # Nippon Suisan Kaisha
            "1605.T",  # INPEX
            "1801.T",  # Taisei
            "1802.T",  # Obayashi
            "1803.T",  # Shimizu
            "1812.T",  # Kajima
            "1925.T",  # Daiwa House Industry
            "1928.T",  # Sekisui House
            "1963.T",  # JGC Holdings
            "2002.T",  # Nisshin Seifun Group
            "8802.T",  # Mitsubishi Estate
            "8804.T",  # Tokyo Tatemono
            "8830.T",  # Sumitomo Realty & Development
        ]

        # Combine all stock lists
        all_stocks = (
            large_cap_stocks
            + mid_cap_stocks
            + tech_stocks
            + financial_stocks
            + consumer_stocks
            + real_estate_stocks
        )

        # Remove duplicates while preserving order
        unique_stocks = []
        seen = set()
        for stock in all_stocks:
            if stock not in seen:
                unique_stocks.append(stock)
                seen.add(stock)

        # Update cache
        self._japanese_stocks_cache = unique_stocks
        self._cache_timestamp = datetime.now()

        self.logger.info(
            f"Retrieved comprehensive Japanese stock list ({len(unique_stocks)} stocks)"
        )
        self.logger.info(
            f"Stock distribution: Large Cap: {len(large_cap_stocks)}, "
            f"Mid Cap: {len(mid_cap_stocks)}, Tech: {len(tech_stocks)}, "
            f"Financial: {len(financial_stocks)}, Consumer: {len(consumer_stocks)}, "
            f"Real Estate: {len(real_estate_stocks)}"
        )

        return unique_stocks

    def _get_tse_official_stocks(self) -> List[str]:
        """
        Get TSE official stock list using TSEStockListManager.

        Returns:
            List of valid TSE stock symbols with .T suffix (excluding ETFs/REITs)

        Raises:
            APIError: If TSE data loading fails and fallback is disabled
        """
        try:
            self.logger.info("Fetching TSE official stock list...")

            # Use TSEStockListManager to get stocks with fallback
            stocks = self.tse_manager.get_stocks_with_fallback()

            # Log processing statistics
            stats = self.tse_manager.get_processing_statistics()
            self.logger.info(
                f"TSE official stock list retrieved - "
                f"Total records: {stats.get('total_records', 'N/A')}, "
                f"Final stocks: {stats.get('final_stocks', len(stocks))}, "
                f"Excluded investment products: {stats.get('excluded_investment_products', 'N/A')}"
            )

            # Log market breakdown if available
            market_breakdown = stats.get("market_category_breakdown", {})
            if market_breakdown:
                self.logger.info(f"Market breakdown: {market_breakdown}")

            # Update cache
            self._tse_official_stocks_cache = stocks
            self._cache_timestamp = datetime.now()

            self.logger.info(
                f"TSE official stock list complete: {len(stocks)} stocks ready for screening"
            )

            return stocks

        except Exception as e:
            self.logger.error(f"Failed to get TSE official stock list: {e}")
            raise APIError(f"TSE official stock list retrieval failed: {str(e)}")

    def get_tse_stock_metadata(self, stock_code: str) -> Dict[str, Any]:
        """
        Get TSE metadata for a specific stock code.

        Args:
            stock_code: Stock code (with or without .T suffix)

        Returns:
            Dict containing TSE metadata (sector, market category, etc.)
        """
        try:
            return self.tse_manager.get_stock_metadata(stock_code)
        except Exception as e:
            self.logger.error(f"Failed to get TSE metadata for {stock_code}: {e}")
            return {}

    def get_tse_processing_statistics(self) -> Dict[str, Any]:
        """
        Get TSE data processing statistics.

        Returns:
            Dict containing processing statistics
        """
        try:
            return self.tse_manager.get_processing_statistics()
        except Exception as e:
            self.logger.error(f"Failed to get TSE processing statistics: {e}")
            return {}

    def refresh_tse_data_if_updated(self) -> bool:
        """
        Refresh TSE data cache if the data file has been updated.

        Returns:
            True if cache was refreshed, False otherwise
        """
        try:
            return self.tse_manager.refresh_if_updated()
        except Exception as e:
            self.logger.error(f"Failed to refresh TSE data: {e}")
            return False

    def get_multiple_stocks_info(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get financial information for multiple stocks with enhanced delisted stock handling

        Args:
            symbols: List of stock symbols

        Returns:
            Dictionary mapping symbols to their financial info

        Raises:
            APIError: If data retrieval fails
        """

        def _fetch_multiple_info():
            results = {}
            delisted_count = 0
            error_count = 0

            for symbol in symbols:
                try:
                    info = self.get_financial_info(symbol)
                    results[symbol] = info
                except DataNotFoundError as e:
                    # Enhanced logging for delisted stocks
                    delisted_count += 1
                    self.logger.warning(
                        f"Delisted/invalid stock skipped - Symbol: {symbol}, "
                        f"Error: {str(e)}, Timestamp: {datetime.now().isoformat()}"
                    )
                    continue
                except Exception as e:
                    error_count += 1
                    self.logger.error(
                        f"Error fetching data - Symbol: {symbol}, "
                        f"Error: {str(e)}, Timestamp: {datetime.now().isoformat()}"
                    )
                    continue

                # Add small delay to avoid rate limiting
                time.sleep(0.1)

            # Log summary statistics
            total_requested = len(symbols)
            successful = len(results)
            self.logger.info(
                f"Multiple stocks info retrieval completed - "
                f"Requested: {total_requested}, Successful: {successful}, "
                f"Delisted/Invalid: {delisted_count}, Errors: {error_count}, "
                f"Success rate: {successful/total_requested*100:.1f}%"
            )

            # Log warning if too many stocks are delisted/invalid
            if delisted_count > total_requested * 0.2:  # More than 20% delisted
                self.logger.warning(
                    f"High delisted stock rate detected: {delisted_count}/{total_requested} "
                    f"({delisted_count/total_requested*100:.1f}%) stocks appear to be delisted or invalid"
                )

            return results

        return self._retry_operation(
            _fetch_multiple_info, f"get_multiple_stocks_info({len(symbols)} symbols)"
        )

    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate if a stock symbol exists and has data using SymbolValidator

        Args:
            symbol: Stock symbol to validate

        Returns:
            True if symbol is valid and has data, False otherwise
        """
        try:
            result = self.symbol_validator.validate_symbol(symbol)
            return result.is_valid
        except Exception as e:
            self.logger.error(f"Symbol validation failed for {symbol}: {e}")
            return False

    def validate_and_filter_symbols(self, symbols: List[str]) -> List[str]:
        """
        Validate and filter a list of symbols to return only valid ones with enhanced functionality

        Args:
            symbols: List of stock symbols to validate and filter

        Returns:
            List containing only valid symbols

        Implements requirements 6.1, 6.2, 6.3, 6.5 for comprehensive symbol filtering and logging
        """
        self.logger.info(
            f"Starting enhanced symbol validation and filtering for {len(symbols)} symbols"
        )

        # Use enhanced SymbolFilter for comprehensive filtering
        filtering_result = self.symbol_filter.filter_symbols(
            symbols=symbols,
            operation_name="validate_and_filter_symbols",
            log_details=True,
        )

        # Check for empty list and alert (requirement 6.5)
        if not filtering_result.valid_symbols:
            self.symbol_filter.validate_and_alert_empty_list(
                filtering_result.valid_symbols,
                operation_name="validate_and_filter_symbols",
            )

        # Log comprehensive filtering results
        filter_rate = filtering_result.filter_rate * 100
        if filtering_result.filtered_symbols:
            self.logger.warning(
                f"Enhanced symbol filtering completed - Original: {len(symbols)}, "
                f"Valid: {len(filtering_result.valid_symbols)}, "
                f"Filtered out: {len(filtering_result.filtered_symbols)} ({filter_rate:.1f}%)"
            )

            # Log breakdown of filtered symbols
            if filtering_result.delisted_symbols:
                self.logger.warning(
                    f"Delisted symbols filtered: {len(filtering_result.delisted_symbols)} - "
                    f"Sample: {filtering_result.delisted_symbols[:5]}"
                    + ("..." if len(filtering_result.delisted_symbols) > 5 else "")
                )

            if filtering_result.invalid_symbols:
                self.logger.warning(
                    f"Invalid symbols filtered: {len(filtering_result.invalid_symbols)} - "
                    f"Sample: {filtering_result.invalid_symbols[:5]}"
                    + ("..." if len(filtering_result.invalid_symbols) > 5 else "")
                )

            if filtering_result.error_symbols:
                self.logger.error(
                    f"Error symbols filtered: {len(filtering_result.error_symbols)} - "
                    f"Sample: {filtering_result.error_symbols[:5]}"
                    + ("..." if len(filtering_result.error_symbols) > 5 else "")
                )
        else:
            self.logger.info(f"All {len(symbols)} symbols are valid")

        return filtering_result.valid_symbols

    def pre_filter_symbol_list(
        self,
        symbols: List[str],
        operation_name: str = "pre_filtering",
        filtering_mode: Optional[FilteringMode] = None,
    ) -> List[str]:
        """
        Pre-filter a symbol list before processing with comprehensive logging.

        Args:
            symbols: Original symbol list
            operation_name: Name of the operation for logging
            filtering_mode: Override default filtering mode

        Returns:
            Filtered list of valid symbols

        Implements requirements 6.1, 6.2, 6.3 for pre-filtering and update logging
        """
        self.logger.info(
            f"Pre-filtering symbol list - Operation: {operation_name}, "
            f"Original count: {len(symbols)}"
        )

        # Use SymbolFilter for pre-filtering with update logging
        valid_symbols = self.symbol_filter.pre_filter_symbol_list(
            symbols=symbols,
            operation_name=operation_name,
            update_log=True,
        )

        # Additional logging for integration
        filtered_count = len(symbols) - len(valid_symbols)
        if filtered_count > 0:
            self.logger.info(
                f"Pre-filtering completed - Operation: {operation_name}, "
                f"Removed: {filtered_count}, Remaining: {len(valid_symbols)}"
            )

        return valid_symbols

    def configure_symbol_filtering(
        self,
        filtering_mode: Optional[FilteringMode] = None,
        high_filter_rate_threshold: Optional[float] = None,
        empty_list_alert: Optional[bool] = None,
    ) -> None:
        """
        Configure symbol filtering behavior.

        Args:
            filtering_mode: New filtering mode (STRICT, TOLERANT, PERMISSIVE)
            high_filter_rate_threshold: Threshold for high filter rate alerts (0.0-1.0)
            empty_list_alert: Whether to enable empty list alerts

        Implements requirement 6.1 for configurable filtering behavior
        """
        self.symbol_filter.configure_filtering(
            filtering_mode=filtering_mode,
            high_filter_rate_threshold=high_filter_rate_threshold,
            empty_list_alert=empty_list_alert,
        )

        self.logger.info("Symbol filtering configuration updated")

    def get_symbol_filtering_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive symbol filtering statistics.

        Returns:
            Dictionary with detailed filtering statistics

        Implements requirement 6.3 for filtering monitoring
        """
        return self.symbol_filter.get_filtering_statistics()

    def get_multiple_stocks_info_with_continuation(
        self, symbols: List[str], skip_invalid: bool = True
    ) -> Tuple[Dict[str, Dict[str, Any]], Any]:
        """
        Get financial information for multiple stocks with enhanced error handling and processing continuation.

        Args:
            symbols: List of stock symbols
            skip_invalid: If True, skip invalid data and continue processing

        Returns:
            Tuple of (valid_results_dict, processing_result)

        Implements requirements 4.3, 4.4 for processing continuation with individual stock errors.
        """

        def process_single_stock(symbol: str) -> Dict[str, Any]:
            """Process a single stock symbol"""
            return self.get_financial_info(symbol)

        # Use enhanced error handler for processing with continuation
        processing_result = self.error_handler.process_items_with_continuation(
            items=symbols,
            processor_func=process_single_stock,
            operation_name="get_multiple_stocks_info",
            get_symbol_func=lambda symbol: symbol,
        )

        # Extract successful results
        results = {}
        for i, symbol in enumerate(symbols):
            if i < processing_result.processed_count:
                try:
                    # Re-fetch successful items (this could be optimized by storing results)
                    results[symbol] = self.get_financial_info(symbol)
                except Exception:
                    # Skip if still failing
                    continue

        self.logger.info(
            f"Enhanced multiple stocks info retrieval completed - "
            f"Requested: {len(symbols)}, Processed: {processing_result.processed_count}, "
            f"Skipped: {processing_result.skipped_count}, Errors: {processing_result.error_count}, "
            f"Success Rate: {processing_result.get_success_rate():.1%}"
        )

        return results, processing_result

    def get_multiple_stocks_info_with_validation(
        self, symbols: List[str], skip_invalid: bool = True
    ) -> Tuple[Dict[str, Dict[str, Any]], Any]:
        """
        Get financial information for multiple stocks with comprehensive validation and error handling.

        Args:
            symbols: List of stock symbols
            skip_invalid: If True, skip invalid data and continue processing

        Returns:
            Tuple of (valid_results_dict, validation_summary)

        Implements requirements 3.3, 3.4 for validation error processing.
        """
        self.logger.info(
            f"Starting validated data retrieval for {len(symbols)} symbols"
        )

        # Prepare data for batch processing
        data_batch = []
        for symbol in symbols:
            try:
                financial_info = self.get_financial_info(symbol)
                data_batch.append((symbol, financial_info))
            except DataNotFoundError as e:
                self.logger.warning(f"Skipping {symbol} - data not found: {e}")
                continue
            except Exception as e:
                self.logger.error(f"Error fetching data for {symbol}: {e}")
                if not skip_invalid:
                    raise
                continue

        # Process with validation
        valid_data, summary = (
            self.validation_error_processor.process_financial_data_batch(data_batch)
        )

        # Convert back to dictionary format
        results = {symbol: data for symbol, data in valid_data}

        self.logger.info(
            f"Validated data retrieval completed - "
            f"Requested: {len(symbols)}, Retrieved: {len(data_batch)}, "
            f"Valid: {len(results)}, Success Rate: {summary.get_success_rate():.1f}%"
        )

        return results, summary

    def validate_financial_data(self, symbol: str, data: Dict[str, Any]) -> bool:
        """
        Validate financial data using DataValidator with enhanced logging.

        Args:
            symbol: Stock symbol
            data: Financial data dictionary

        Returns:
            True if data is valid, False otherwise

        Implements requirement 3.1 for financial data validation.
        """
        try:
            result = self.data_validator.validate_financial_data(symbol, data)

            # Log validation results using enhanced logger if there are issues
            if not result.is_valid or result.warnings:
                self.enhanced_logger.log_data_validation_error(
                    symbol=symbol,
                    data_type="financial",
                    validation_errors=result.errors,
                    validation_warnings=result.warnings,
                    data_summary={
                        "has_current_price": bool(data.get("currentPrice")),
                        "has_per": bool(data.get("trailingPE")),
                        "has_pbr": bool(data.get("priceToBook")),
                        "has_dividend_yield": bool(data.get("dividendYield")),
                        "symbol": data.get("symbol", "unknown"),
                    },
                    action_taken=(
                        "skipped" if not result.is_valid else "included_with_warnings"
                    ),
                    additional_context={
                        "validation_status": result.status.value,
                        "quality_score": result.quality_score,
                    },
                )

            return result.is_valid
        except Exception as e:
            self.logger.error(f"Financial data validation failed for {symbol}: {e}")
            return False

    def validate_price_data(self, symbol: str, data: pd.DataFrame) -> bool:
        """
        Validate price data using DataValidator with enhanced logging.

        Args:
            symbol: Stock symbol
            data: Price data DataFrame

        Returns:
            True if data is valid, False otherwise

        Implements requirement 3.2 for price data validation.
        """
        try:
            result = self.data_validator.validate_price_data(symbol, data)

            # Log validation results using enhanced logger if there are issues
            if not result.is_valid or result.warnings:
                self.enhanced_logger.log_data_validation_error(
                    symbol=symbol,
                    data_type="price",
                    validation_errors=result.errors,
                    validation_warnings=result.warnings,
                    data_summary={
                        "record_count": len(data) if not data.empty else 0,
                        "has_close_prices": (
                            "Close" in data.columns if not data.empty else False
                        ),
                        "has_volume": (
                            "Volume" in data.columns if not data.empty else False
                        ),
                        "date_range": (
                            {
                                "start": (
                                    str(data.index.min()) if not data.empty else None
                                ),
                                "end": (
                                    str(data.index.max()) if not data.empty else None
                                ),
                            }
                            if not data.empty
                            else None
                        ),
                        "missing_values": (
                            data.isnull().sum().to_dict() if not data.empty else {}
                        ),
                    },
                    action_taken=(
                        "skipped" if not result.is_valid else "included_with_warnings"
                    ),
                    additional_context={
                        "validation_status": result.status.value,
                        "quality_score": result.quality_score,
                    },
                )

            return result.is_valid
        except Exception as e:
            self.logger.error(f"Price data validation failed for {symbol}: {e}")
            return False

    def validate_dividend_data(self, symbol: str, data: pd.DataFrame) -> bool:
        """
        Validate dividend data using DataValidator with enhanced logging.

        Args:
            symbol: Stock symbol
            data: Dividend data DataFrame

        Returns:
            True if data is valid, False otherwise

        Implements requirement 3.2 for dividend data validation.
        """
        try:
            result = self.data_validator.validate_dividend_data(symbol, data)

            # Log validation results using enhanced logger if there are issues
            if not result.is_valid or result.warnings:
                self.enhanced_logger.log_data_validation_error(
                    symbol=symbol,
                    data_type="dividend",
                    validation_errors=result.errors,
                    validation_warnings=result.warnings,
                    data_summary={
                        "record_count": len(data) if not data.empty else 0,
                        "has_dividends": (
                            "Dividends" in data.columns if not data.empty else False
                        ),
                        "dividend_sum": (
                            float(data["Dividends"].sum())
                            if not data.empty and "Dividends" in data.columns
                            else 0
                        ),
                        "date_range": (
                            {
                                "start": (
                                    str(data.index.min()) if not data.empty else None
                                ),
                                "end": (
                                    str(data.index.max()) if not data.empty else None
                                ),
                            }
                            if not data.empty
                            else None
                        ),
                        "positive_dividends": (
                            int((data["Dividends"] > 0).sum())
                            if not data.empty and "Dividends" in data.columns
                            else 0
                        ),
                    },
                    action_taken=(
                        "included_with_warnings"
                        if result.warnings
                        else ("skipped" if not result.is_valid else "included")
                    ),
                    additional_context={
                        "validation_status": result.status.value,
                        "quality_score": result.quality_score,
                    },
                )

            return result.is_valid
        except Exception as e:
            self.logger.error(f"Dividend data validation failed for {symbol}: {e}")
            return False

    def get_validated_stock_data(
        self,
        symbol: str,
        include_price: bool = True,
        include_dividends: bool = True,
        skip_invalid: bool = True,
    ) -> Tuple[Optional[Dict[str, Any]], List[str], List[str]]:
        """
        Get comprehensive stock data with validation for a single symbol.

        Args:
            symbol: Stock symbol
            include_price: Whether to include price data
            include_dividends: Whether to include dividend data
            skip_invalid: If True, return partial data if some validation fails

        Returns:
            Tuple of (stock_data_dict, warnings, errors)
            stock_data_dict contains 'financial', 'price', 'dividend' keys as available

        Implements requirements 3.3, 3.4 for comprehensive data validation.
        """
        warnings = []
        errors = []
        stock_data = {}

        try:
            # Get and validate financial data
            financial_data = self.get_financial_info(symbol)
            financial_result = self.data_validator.validate_financial_data(
                symbol, financial_data
            )

            if financial_result.is_valid:
                stock_data["financial"] = financial_data
                warnings.extend(financial_result.warnings)
            else:
                errors.extend(financial_result.errors)
                if not skip_invalid:
                    return None, warnings, errors

        except DataNotFoundError as e:
            error_msg = f"Financial data not found for {symbol}: {e}"
            errors.append(error_msg)
            self.logger.warning(error_msg)
            if not skip_invalid:
                return None, warnings, errors
        except Exception as e:
            error_msg = f"Error fetching financial data for {symbol}: {e}"
            errors.append(error_msg)
            self.logger.error(error_msg)
            if not skip_invalid:
                return None, warnings, errors

        # Get and validate price data if requested
        if include_price:
            try:
                price_data = self.get_stock_prices(symbol)
                price_result = self.data_validator.validate_price_data(
                    symbol, price_data
                )

                if price_result.is_valid:
                    stock_data["price"] = price_data
                    warnings.extend(price_result.warnings)
                else:
                    errors.extend(price_result.errors)
                    if not skip_invalid:
                        return None, warnings, errors

            except DataNotFoundError as e:
                error_msg = f"Price data not found for {symbol}: {e}"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                if not skip_invalid:
                    return None, warnings, errors
            except Exception as e:
                error_msg = f"Error fetching price data for {symbol}: {e}"
                errors.append(error_msg)
                self.logger.error(error_msg)
                if not skip_invalid:
                    return None, warnings, errors

        # Get and validate dividend data if requested
        if include_dividends:
            try:
                dividend_data = self.get_dividend_history(symbol)
                dividend_result = self.data_validator.validate_dividend_data(
                    symbol, dividend_data
                )

                if dividend_result.is_valid:
                    stock_data["dividend"] = dividend_data
                    warnings.extend(dividend_result.warnings)
                else:
                    # Dividend validation errors are usually not critical
                    warnings.extend(dividend_result.errors)

            except DataNotFoundError as e:
                warning_msg = f"Dividend data not found for {symbol}: {e}"
                warnings.append(warning_msg)
                self.logger.info(warning_msg)  # Info level for dividend data
            except Exception as e:
                warning_msg = f"Error fetching dividend data for {symbol}: {e}"
                warnings.append(warning_msg)
                self.logger.warning(warning_msg)

        # Final validation using ValidationErrorProcessor
        should_include, additional_warnings, additional_errors = (
            self.validation_error_processor.validate_and_filter_data(
                symbol,
                stock_data.get("financial"),
                stock_data.get("price"),
                stock_data.get("dividend"),
            )
        )

        warnings.extend(additional_warnings)
        errors.extend(additional_errors)

        if should_include and stock_data:
            return stock_data, warnings, errors
        else:
            return None, warnings, errors

    def configure_validation(
        self,
        strict_mode: bool = False,
        require_current_price: bool = True,
        min_price_records: int = 100,
        continue_on_error: bool = True,
    ) -> None:
        """
        Configure validation behavior.

        Args:
            strict_mode: If True, warnings become errors
            require_current_price: If True, require current price in financial data
            min_price_records: Minimum number of price records required
            continue_on_error: If True, continue processing after validation errors

        Implements requirement 3.4 for configurable validation behavior.
        """
        # Update DataValidator configuration
        validation_config = ValidationConfig(
            strict_mode=strict_mode,
            require_current_price=require_current_price,
            min_price_records=min_price_records,
            log_validation_details=True,
        )

        # Update ValidationErrorProcessor configuration
        processing_config = ProcessingConfig(
            continue_on_validation_error=continue_on_error,
            continue_on_processing_error=continue_on_error,
            log_validation_warnings=True,
            log_validation_errors=True,
            log_summary=True,
        )

        # Reinitialize with new configurations
        self.data_validator = DataValidator(validation_config)
        self.validation_error_processor = ValidationErrorProcessor(
            self.data_validator, processing_config
        )

        self.logger.info(
            f"Validation configuration updated - Strict: {strict_mode}, "
            f"Require Price: {require_current_price}, "
            f"Min Records: {min_price_records}, Continue on Error: {continue_on_error}"
        )

    def get_validation_statistics(self) -> Dict[str, Any]:
        """
        Get validation statistics from DataValidator and ValidationErrorProcessor.

        Returns:
            Dictionary with validation statistics

        Implements requirement 3.4 for validation monitoring.
        """
        validator_stats = self.data_validator.get_validation_statistics()
        processor_summary = self.validation_error_processor.get_error_summary()

        return {
            "validator_statistics": validator_stats,
            "processor_summary": {
                "total_processed": processor_summary.total_processed,
                "successful_validations": processor_summary.successful_validations,
                "validation_warnings": processor_summary.validation_warnings,
                "validation_errors": processor_summary.validation_errors,
                "skipped_items": processor_summary.skipped_items,
                "success_rate": processor_summary.get_success_rate(),
                "error_rate": processor_summary.get_error_rate(),
                "processing_duration": processor_summary.processing_duration,
            },
        }

    def get_enhanced_logger(self) -> EnhancedLogger:
        """
        Get the EnhancedLogger instance for external access.

        Returns:
            EnhancedLogger instance used by this DataFetcher
        """
        return self.enhanced_logger

    def get_error_metrics(self):
        """
        Get the ErrorMetrics instance for external monitoring.

        Returns:
            ErrorMetrics instance from the enhanced logger
        """
        return self.enhanced_logger.get_error_metrics()

    def log_error_summary(self, time_window_hours: int = 1) -> None:
        """
        Log a comprehensive error summary for monitoring.

        Args:
            time_window_hours: Hours to look back for error summary
        """
        self.enhanced_logger.log_error_summary(time_window_hours)

    def configure_retry_behavior(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        rate_limit_delay: float = 60.0,
    ) -> None:
        """
        Configure retry behavior for API operations.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            rate_limit_delay: Base delay for rate limit errors in seconds

        Implements requirement 4.5 for configurable retry behavior.
        """
        self.retry_manager.configure_retry_policy(
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay,
            rate_limit_delay=rate_limit_delay,
        )

        self.logger.info(
            f"Retry behavior configured - Max retries: {max_retries}, "
            f"Base delay: {base_delay}s, Max delay: {max_delay}s, "
            f"Rate limit delay: {rate_limit_delay}s"
        )

    def configure_error_handling(
        self,
        continue_on_error: bool = True,
        max_consecutive_errors: int = 10,
        max_error_rate: float = 0.5,
        treat_data_not_found_as_warning: bool = True,
    ) -> None:
        """
        Configure error handling and processing continuation behavior.

        Args:
            continue_on_error: Whether to continue processing after individual errors
            max_consecutive_errors: Maximum consecutive errors before stopping
            max_error_rate: Maximum error rate before stopping (0.0-1.0)
            treat_data_not_found_as_warning: Whether to treat DataNotFoundError as warning

        Implements requirements 4.3, 4.4 for configurable error handling.
        """
        self.error_handler.configure_processing(
            continue_on_error=continue_on_error,
            max_consecutive_errors=max_consecutive_errors,
            max_error_rate=max_error_rate,
            enable_retries=True,
        )

        # Update error classification if needed
        if treat_data_not_found_as_warning:
            from .error_handler import (
                ErrorClassification,
                ErrorSeverity,
                ProcessingAction,
            )

            self.error_handler.add_error_classification(
                DataNotFoundError,
                ErrorClassification(
                    severity=ErrorSeverity.LOW,
                    action=ProcessingAction.SKIP_ITEM,
                    retryable=False,
                    description="Data not found (treated as warning)",
                    category="data_availability",
                ),
            )

        self.logger.info(
            f"Error handling configured - Continue on error: {continue_on_error}, "
            f"Max consecutive errors: {max_consecutive_errors}, "
            f"Max error rate: {max_error_rate:.1%}, "
            f"Treat data not found as warning: {treat_data_not_found_as_warning}"
        )

    def get_retry_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive retry statistics.

        Returns:
            Dictionary with retry statistics

        Implements requirement 4.5 for retry monitoring.
        """
        return self.retry_manager.get_retry_statistics()

    def get_error_handling_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive error handling statistics.

        Returns:
            Dictionary with error handling statistics

        Implements requirement 4.4 for error monitoring.
        """
        return self.error_handler.get_error_statistics()

    def reset_retry_statistics(self) -> None:
        """Reset retry statistics for fresh monitoring."""
        self.retry_manager.reset_statistics()

    def reset_error_handling_state(self) -> None:
        """Reset error handling state for fresh processing."""
        self.error_handler.reset_error_state()


# Convenience factory functions for creating DataFetcher instances with different configurations


def create_datafetcher_with_strict_error_handling() -> DataFetcher:
    """
    Create DataFetcher with strict error handling configuration.

    Returns:
        DataFetcher configured for strict error handling
    """
    from .error_handling_config import create_strict_config

    config = create_strict_config()
    return DataFetcher(error_handling_config=config, enable_enhanced_features=True)


def create_datafetcher_with_tolerant_error_handling() -> DataFetcher:
    """
    Create DataFetcher with tolerant error handling configuration.

    Returns:
        DataFetcher configured for tolerant error handling
    """
    from .error_handling_config import create_tolerant_config

    config = create_tolerant_config()
    return DataFetcher(error_handling_config=config, enable_enhanced_features=True)


def create_datafetcher_with_debug_error_handling() -> DataFetcher:
    """
    Create DataFetcher with debug error handling configuration.

    Returns:
        DataFetcher configured for debug error handling
    """
    from .error_handling_config import create_debug_config

    config = create_debug_config()
    return DataFetcher(error_handling_config=config, enable_enhanced_features=True)


def create_datafetcher_from_environment() -> DataFetcher:
    """
    Create DataFetcher with error handling configuration loaded from environment.

    Returns:
        DataFetcher configured from environment variables
    """
    from .error_handling_config import load_config_from_environment

    config = load_config_from_environment()
    return DataFetcher(error_handling_config=config, enable_enhanced_features=True)


def create_legacy_datafetcher(
    retry_config: Optional[RetryConfig] = None,
    error_config: Optional[ErrorProcessingConfig] = None,
) -> DataFetcher:
    """
    Create DataFetcher with legacy configuration for backward compatibility.

    Args:
        retry_config: Legacy retry configuration
        error_config: Legacy error processing configuration

    Returns:
        DataFetcher configured with legacy settings
    """
    return DataFetcher(
        retry_config=retry_config,
        error_config=error_config,
        enable_enhanced_features=False,
    )
