"""
Symbol validator module for stock symbol validation and delisted stock detection.

This module provides functionality to:
- Validate stock symbol existence and validity
- Detect delisted or invalid stocks
- Cache validation results for performance
- Filter valid symbols from lists
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import yfinance as yf
import time

from .exceptions import DataNotFoundError, APIError


class ValidationStatus(Enum):
    """Enumeration for validation status types."""

    VALID = "valid"
    DELISTED = "delisted"
    INVALID = "invalid"
    NOT_FOUND = "not_found"
    ERROR = "error"


@dataclass
class ValidationResult:
    """Result of symbol validation with detailed information."""

    symbol: str
    status: ValidationStatus
    is_valid: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    validated_at: datetime = field(default_factory=datetime.now)
    additional_info: Dict = field(default_factory=dict)

    def __post_init__(self):
        """Set is_valid based on status."""
        self.is_valid = self.status == ValidationStatus.VALID


class SymbolValidator:
    """
    Validates stock symbols and detects delisted or invalid stocks.

    Provides caching functionality to avoid repeated validation of the same symbols.
    Implements requirements 1.1, 1.2, 6.1, 6.4 for symbol validation and caching.
    """

    def __init__(self, cache_duration: timedelta = timedelta(hours=24)):
        """
        Initialize SymbolValidator with configurable cache duration.

        Args:
            cache_duration: How long to cache validation results (default: 24 hours)
        """
        self.logger = logging.getLogger(__name__)
        self.cache_duration = cache_duration
        self.validation_cache: Dict[str, ValidationResult] = {}
        self._delisted_symbols: Set[str] = set()

        # Statistics tracking
        self._validation_stats = {
            "total_validations": 0,
            "cache_hits": 0,
            "valid_symbols": 0,
            "delisted_symbols": 0,
            "invalid_symbols": 0,
            "errors": 0,
        }

    def validate_symbol(self, symbol: str) -> ValidationResult:
        """
        Validate a single stock symbol with caching.

        Args:
            symbol: Stock symbol to validate (e.g., "7203" or "7203.T")

        Returns:
            ValidationResult with detailed validation information

        Implements requirements 1.1, 1.2, 6.4 for symbol validation and caching
        """
        formatted_symbol = self._format_japanese_symbol(symbol)

        # Check cache first
        cached_result = self._get_cached_result(formatted_symbol)
        if cached_result:
            self._validation_stats["cache_hits"] += 1
            self.logger.debug(f"Using cached validation result for {formatted_symbol}")
            return cached_result

        self._validation_stats["total_validations"] += 1

        # Perform actual validation
        try:
            result = self._perform_validation(formatted_symbol)

            # Cache the result
            self.validation_cache[formatted_symbol] = result

            # Update statistics
            if result.status == ValidationStatus.VALID:
                self._validation_stats["valid_symbols"] += 1
            elif result.status == ValidationStatus.DELISTED:
                self._validation_stats["delisted_symbols"] += 1
                self._delisted_symbols.add(formatted_symbol)
            elif result.status in [
                ValidationStatus.INVALID,
                ValidationStatus.NOT_FOUND,
            ]:
                self._validation_stats["invalid_symbols"] += 1
            else:
                self._validation_stats["errors"] += 1

            self.logger.info(
                f"Symbol validation completed - Symbol: {formatted_symbol}, "
                f"Status: {result.status.value}, Valid: {result.is_valid}"
            )

            return result

        except Exception as e:
            self.logger.error(f"Validation error for {formatted_symbol}: {e}")
            error_result = ValidationResult(
                symbol=formatted_symbol,
                status=ValidationStatus.ERROR,
                is_valid=False,
                error_type=type(e).__name__,
                error_message=str(e),
            )
            self._validation_stats["errors"] += 1
            return error_result

    def batch_validate_symbols(self, symbols: List[str]) -> Dict[str, ValidationResult]:
        """
        Validate multiple symbols efficiently with batch processing.

        Args:
            symbols: List of stock symbols to validate

        Returns:
            Dictionary mapping symbols to their validation results

        Implements requirements 1.1, 1.2, 6.1 for batch symbol validation
        """
        results = {}

        self.logger.info(f"Starting batch validation of {len(symbols)} symbols")
        start_time = datetime.now()

        for i, symbol in enumerate(symbols):
            try:
                result = self.validate_symbol(symbol)
                results[symbol] = result

                # Add small delay to avoid rate limiting
                if i > 0 and i % 10 == 0:  # Every 10 symbols
                    time.sleep(0.5)
                    self.logger.debug(
                        f"Batch validation progress: {i+1}/{len(symbols)}"
                    )

            except Exception as e:
                self.logger.error(f"Error validating symbol {symbol} in batch: {e}")
                results[symbol] = ValidationResult(
                    symbol=symbol,
                    status=ValidationStatus.ERROR,
                    is_valid=False,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

        # Log batch validation summary
        duration = datetime.now() - start_time
        valid_count = sum(1 for r in results.values() if r.is_valid)
        delisted_count = sum(
            1 for r in results.values() if r.status == ValidationStatus.DELISTED
        )
        invalid_count = sum(
            1
            for r in results.values()
            if r.status in [ValidationStatus.INVALID, ValidationStatus.NOT_FOUND]
        )
        error_count = sum(
            1 for r in results.values() if r.status == ValidationStatus.ERROR
        )

        self.logger.info(
            f"Batch validation completed - Duration: {duration.total_seconds():.1f}s, "
            f"Total: {len(symbols)}, Valid: {valid_count}, Delisted: {delisted_count}, "
            f"Invalid: {invalid_count}, Errors: {error_count}"
        )

        return results

    def is_delisted(self, symbol: str) -> bool:
        """
        Check if a symbol is known to be delisted.

        Args:
            symbol: Stock symbol to check

        Returns:
            True if symbol is known to be delisted, False otherwise

        Implements requirement 1.2 for delisted stock detection
        """
        formatted_symbol = self._format_japanese_symbol(symbol)

        # Check delisted symbols cache
        if formatted_symbol in self._delisted_symbols:
            return True

        # Check validation cache
        cached_result = self._get_cached_result(formatted_symbol)
        if cached_result and cached_result.status == ValidationStatus.DELISTED:
            return True

        return False

    def filter_valid_symbols(self, symbols: List[str]) -> List[str]:
        """
        Filter a list of symbols to return only valid ones.

        Args:
            symbols: List of stock symbols to filter

        Returns:
            List containing only valid symbols

        Implements requirements 6.1, 6.2 for symbol filtering
        """
        self.logger.info(f"Filtering {len(symbols)} symbols for validity")

        validation_results = self.batch_validate_symbols(symbols)
        valid_symbols = [
            symbol for symbol, result in validation_results.items() if result.is_valid
        ]

        filtered_count = len(symbols) - len(valid_symbols)
        self.logger.info(
            f"Symbol filtering completed - Original: {len(symbols)}, "
            f"Valid: {len(valid_symbols)}, Filtered out: {filtered_count}"
        )

        if filtered_count > 0:
            # Log details about filtered symbols
            delisted = [
                s
                for s, r in validation_results.items()
                if r.status == ValidationStatus.DELISTED
            ]
            invalid = [
                s
                for s, r in validation_results.items()
                if r.status in [ValidationStatus.INVALID, ValidationStatus.NOT_FOUND]
            ]
            errors = [
                s
                for s, r in validation_results.items()
                if r.status == ValidationStatus.ERROR
            ]

            if delisted:
                self.logger.warning(
                    f"Delisted symbols filtered: {delisted[:10]}{'...' if len(delisted) > 10 else ''}"
                )
            if invalid:
                self.logger.warning(
                    f"Invalid symbols filtered: {invalid[:10]}{'...' if len(invalid) > 10 else ''}"
                )
            if errors:
                self.logger.error(
                    f"Error symbols filtered: {errors[:10]}{'...' if len(errors) > 10 else ''}"
                )

        return valid_symbols

    def get_validation_stats(self) -> Dict:
        """
        Get validation statistics for monitoring and debugging.

        Returns:
            Dictionary with validation statistics
        """
        stats = self._validation_stats.copy()
        stats.update(
            {
                "cache_size": len(self.validation_cache),
                "delisted_cache_size": len(self._delisted_symbols),
                "cache_hit_rate": (
                    stats["cache_hits"] / max(stats["total_validations"], 1)
                )
                * 100,
            }
        )
        return stats

    def clear_cache(self) -> None:
        """Clear all cached validation results."""
        self.validation_cache.clear()
        self._delisted_symbols.clear()
        self.logger.info("Validation cache cleared")

    def _format_japanese_symbol(self, symbol: str) -> str:
        """
        Format Japanese stock symbol for yfinance.

        Args:
            symbol: Stock code (e.g., "7203" or "7203.T")

        Returns:
            Formatted symbol for yfinance (e.g., "7203.T")
        """
        if "." not in symbol:
            return f"{symbol}.T"
        return symbol

    def _get_cached_result(self, symbol: str) -> Optional[ValidationResult]:
        """
        Get cached validation result if still valid.

        Args:
            symbol: Formatted symbol to check

        Returns:
            Cached ValidationResult if valid, None otherwise
        """
        if symbol not in self.validation_cache:
            return None

        cached_result = self.validation_cache[symbol]

        # Check if cache is still valid
        if datetime.now() - cached_result.validated_at > self.cache_duration:
            # Cache expired, remove it
            del self.validation_cache[symbol]
            if symbol in self._delisted_symbols:
                self._delisted_symbols.remove(symbol)
            return None

        return cached_result

    def _perform_validation(self, formatted_symbol: str) -> ValidationResult:
        """
        Perform actual symbol validation using yfinance.

        Args:
            formatted_symbol: Formatted symbol to validate

        Returns:
            ValidationResult with validation outcome
        """
        try:
            ticker = yf.Ticker(formatted_symbol)

            # Try to get basic info first (lightweight check)
            info = ticker.info

            if not info or len(info) == 0:
                return ValidationResult(
                    symbol=formatted_symbol,
                    status=ValidationStatus.NOT_FOUND,
                    is_valid=False,
                    error_message="No ticker info available",
                )

            # Check for delisted indicators in the info
            if self._check_delisted_indicators(info):
                return ValidationResult(
                    symbol=formatted_symbol,
                    status=ValidationStatus.DELISTED,
                    is_valid=False,
                    error_message="Stock appears to be delisted",
                    additional_info={"info_keys": list(info.keys())[:10]},
                )

            # Check if we have essential data that indicates a valid stock
            essential_fields = [
                "symbol",
                "shortName",
                "longName",
                "regularMarketPrice",
                "currentPrice",
            ]
            has_essential_data = any(info.get(field) for field in essential_fields)

            if not has_essential_data:
                return ValidationResult(
                    symbol=formatted_symbol,
                    status=ValidationStatus.INVALID,
                    is_valid=False,
                    error_message="Insufficient data - missing essential fields",
                    additional_info={"available_keys": list(info.keys())[:10]},
                )

            # Try to get recent price data as additional validation
            try:
                hist = ticker.history(period="5d")  # Last 5 days
                if hist.empty:
                    return ValidationResult(
                        symbol=formatted_symbol,
                        status=ValidationStatus.DELISTED,
                        is_valid=False,
                        error_message="No recent price data available - likely delisted",
                    )
            except Exception as hist_error:
                self.logger.debug(
                    f"Could not get price history for {formatted_symbol}: {hist_error}"
                )
                # Don't fail validation just because of price history issues

            # If we get here, the symbol appears to be valid
            return ValidationResult(
                symbol=formatted_symbol,
                status=ValidationStatus.VALID,
                is_valid=True,
                additional_info={
                    "has_current_price": bool(
                        info.get("currentPrice") or info.get("regularMarketPrice")
                    ),
                    "exchange": info.get("exchange", ""),
                    "currency": info.get("currency", ""),
                },
            )

        except Exception as e:
            error_str = str(e).lower()

            # Check for specific error patterns that indicate delisted stocks
            delisted_patterns = [
                "possibly delisted",
                "delisted",
                "no data found",
                "ticker not found",
                "invalid ticker",
                "not found",
                "no price data found",
                "symbol may be delisted",
            ]

            if any(pattern in error_str for pattern in delisted_patterns):
                return ValidationResult(
                    symbol=formatted_symbol,
                    status=ValidationStatus.DELISTED,
                    is_valid=False,
                    error_type=type(e).__name__,
                    error_message=f"Delisted stock detected: {str(e)}",
                )

            # For other errors, mark as validation error
            return ValidationResult(
                symbol=formatted_symbol,
                status=ValidationStatus.ERROR,
                is_valid=False,
                error_type=type(e).__name__,
                error_message=str(e),
            )

    def _check_delisted_indicators(self, info: Dict) -> bool:
        """
        Check if ticker info contains indicators of a delisted stock.

        Args:
            info: Ticker info dictionary from yfinance

        Returns:
            True if delisted indicators found, False otherwise
        """
        # Check for explicit delisted status
        if info.get("quoteType") == "DELISTED":
            return True

        # If we have basic company info (symbol, name), it's likely valid
        has_basic_info = bool(
            info.get("symbol") or info.get("shortName") or info.get("longName")
        )

        # If we have basic info, consider it valid (even without current price)
        if has_basic_info:
            return False

        # Check for delisted-related messages in various fields
        text_fields = ["longBusinessSummary", "industry", "sector"]
        for field in text_fields:
            value = info.get(field, "")
            if isinstance(value, str) and "delisted" in value.lower():
                return True

        return False
