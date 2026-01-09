"""
Data validation module for stock value notifier system.

This module provides comprehensive data validation for financial data including:
- Financial information completeness validation
- Stock price data validity validation
- Dividend data validation
- Data quality scoring and error handling

Implements requirements 3.1, 3.2 for data validation functionality.
"""

import logging
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum


class ValidationStatus(Enum):
    """Status of data validation."""

    VALID = "valid"
    INVALID = "invalid"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class DataValidationResult:
    """Result of data validation with detailed information."""

    symbol: str
    data_type: str  # "financial", "price", "dividend"
    status: ValidationStatus
    quality_score: float = 1.0  # 0.0 to 1.0, where 1.0 is perfect quality
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.now)
    additional_info: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Update quality score based on warnings and errors."""
        # Adjust quality score based on warnings and errors
        if self.errors:
            self.quality_score = max(0.0, self.quality_score - 0.3 * len(self.errors))
        if self.warnings:
            self.quality_score = max(0.0, self.quality_score - 0.1 * len(self.warnings))

    @property
    def is_valid(self) -> bool:
        """Check if validation result is valid."""
        return self.status in [ValidationStatus.VALID, ValidationStatus.WARNING]


@dataclass
class ValidationConfig:
    """Configuration for data validation rules."""

    # Financial data validation settings
    require_current_price: bool = True
    require_per_pbr: bool = False  # PER/PBR not always available for all stocks
    min_market_cap: Optional[float] = None  # Minimum market cap in JPY
    max_per_threshold: float = 1000.0  # Maximum reasonable PER
    max_pbr_threshold: float = 100.0  # Maximum reasonable PBR

    # Price data validation settings
    min_price_records: int = 100  # Minimum number of price records for 3y period
    max_missing_days: int = 10  # Maximum consecutive missing trading days
    min_price_value: float = 1.0  # Minimum reasonable stock price in JPY
    max_price_value: float = 1000000.0  # Maximum reasonable stock price in JPY
    max_daily_change: float = 0.3  # Maximum daily price change (30%)

    # Dividend data validation settings
    min_dividend_value: float = 0.01  # Minimum dividend value in JPY
    max_dividend_yield: float = 0.5  # Maximum reasonable dividend yield (50%)
    require_recent_dividends: bool = (
        False  # Whether to require recent dividend payments
    )

    # General validation settings
    strict_mode: bool = False  # If True, warnings become errors
    enable_quality_scoring: bool = True
    log_validation_details: bool = True


class DataValidator:
    """
    Comprehensive data validator for financial data.

    Provides validation for:
    - Financial information completeness and reasonableness
    - Stock price data quality and consistency
    - Dividend data validity and patterns

    Implements requirements 3.1, 3.2 for data validation.
    """

    def __init__(self, config: Optional[ValidationConfig] = None):
        """
        Initialize DataValidator with configuration.

        Args:
            config: Validation configuration, uses defaults if None
        """
        self.config = config or ValidationConfig()
        self.logger = logging.getLogger(__name__)

        # Track validation statistics
        self.validation_stats = {
            "total_validations": 0,
            "valid_count": 0,
            "invalid_count": 0,
            "warning_count": 0,
            "error_count": 0,
        }

    def validate_financial_data(
        self, symbol: str, data: Dict[str, Any]
    ) -> DataValidationResult:
        """
        Validate financial information data for completeness and reasonableness.

        Args:
            symbol: Stock symbol
            data: Financial information dictionary

        Returns:
            DataValidationResult with validation outcome

        Implements requirement 3.1 for financial data validation.
        """
        self.validation_stats["total_validations"] += 1

        result = DataValidationResult(
            symbol=symbol, data_type="financial", status=ValidationStatus.VALID
        )

        try:
            # Check for empty or None data
            if not data or len(data) == 0:
                result.status = ValidationStatus.INVALID
                result.errors.append("Financial data is empty or None")
                self._update_stats("invalid")
                return result

            # Check for essential fields
            essential_fields = ["symbol", "currentPrice", "shortName"]
            missing_essential = []
            for field in essential_fields:
                if field not in data or data[field] is None:
                    missing_essential.append(field)

            if missing_essential:
                if (
                    self.config.require_current_price
                    and "currentPrice" in missing_essential
                ):
                    result.status = ValidationStatus.INVALID
                    result.errors.append(f"Missing essential field: currentPrice")
                else:
                    result.status = ValidationStatus.WARNING
                    result.warnings.append(
                        f"Missing essential fields: {missing_essential}"
                    )

            # Validate current price
            current_price = data.get("currentPrice")
            if current_price is not None:
                if not isinstance(current_price, (int, float)) or current_price <= 0:
                    result.errors.append(
                        "Invalid current price: must be positive number"
                    )
                    result.status = ValidationStatus.INVALID
                elif current_price < self.config.min_price_value:
                    result.warnings.append(
                        f"Current price {current_price} is unusually low"
                    )
                elif current_price > self.config.max_price_value:
                    result.warnings.append(
                        f"Current price {current_price} is unusually high"
                    )

            # Validate PER (Price-to-Earnings Ratio)
            per = data.get("trailingPE")
            if per is not None:
                if isinstance(per, (int, float)):
                    if per < 0:
                        result.warnings.append(
                            "Negative PER detected (company may have losses)"
                        )
                    elif per > self.config.max_per_threshold:
                        result.warnings.append(
                            f"Very high PER: {per} (threshold: {self.config.max_per_threshold})"
                        )
                else:
                    result.warnings.append("PER is not a numeric value")
            elif self.config.require_per_pbr:
                result.warnings.append("PER data is missing")

            # Validate PBR (Price-to-Book Ratio)
            pbr = data.get("priceToBook")
            if pbr is not None:
                if isinstance(pbr, (int, float)):
                    if pbr <= 0:
                        result.warnings.append("Invalid PBR: must be positive")
                    elif pbr > self.config.max_pbr_threshold:
                        result.warnings.append(
                            f"Very high PBR: {pbr} (threshold: {self.config.max_pbr_threshold})"
                        )
                else:
                    result.warnings.append("PBR is not a numeric value")
            elif self.config.require_per_pbr:
                result.warnings.append("PBR data is missing")

            # Validate market cap
            market_cap = data.get("marketCap")
            if market_cap is not None:
                if isinstance(market_cap, (int, float)):
                    if market_cap <= 0:
                        result.warnings.append("Invalid market cap: must be positive")
                    elif (
                        self.config.min_market_cap
                        and market_cap < self.config.min_market_cap
                    ):
                        result.warnings.append(
                            f"Market cap {market_cap} below minimum threshold"
                        )
                else:
                    result.warnings.append("Market cap is not a numeric value")

            # Validate dividend yield
            dividend_yield = data.get("dividendYield")
            if dividend_yield is not None:
                if isinstance(dividend_yield, (int, float)):
                    if dividend_yield < 0:
                        result.warnings.append("Negative dividend yield detected")
                    elif dividend_yield > self.config.max_dividend_yield:
                        result.warnings.append(
                            f"Very high dividend yield: {dividend_yield*100:.1f}%"
                        )
                else:
                    result.warnings.append("Dividend yield is not a numeric value")

            # Check data completeness score
            total_fields = len(data)
            non_null_fields = sum(1 for v in data.values() if v is not None)
            completeness_ratio = (
                non_null_fields / total_fields if total_fields > 0 else 0
            )

            result.additional_info["completeness_ratio"] = completeness_ratio
            result.additional_info["total_fields"] = total_fields
            result.additional_info["non_null_fields"] = non_null_fields

            if completeness_ratio < 0.5:
                result.warnings.append(
                    f"Low data completeness: {completeness_ratio*100:.1f}%"
                )

            # Apply strict mode if configured
            if self.config.strict_mode and result.warnings:
                result.status = ValidationStatus.INVALID
                result.errors.extend(result.warnings)
                result.warnings.clear()

            # Update statistics
            if result.status == ValidationStatus.VALID:
                self._update_stats("valid")
            elif result.status == ValidationStatus.WARNING:
                self._update_stats("warning")
            else:
                self._update_stats("invalid")

            # Log validation details if configured
            if self.config.log_validation_details:
                self._log_validation_result(result)

            return result

        except Exception as e:
            self.logger.error(f"Error validating financial data for {symbol}: {e}")
            result.status = ValidationStatus.ERROR
            result.errors.append(f"Validation error: {str(e)}")
            self._update_stats("error")
            return result

    def validate_price_data(
        self, symbol: str, data: pd.DataFrame
    ) -> DataValidationResult:
        """
        Validate stock price data for quality and consistency.

        Args:
            symbol: Stock symbol
            data: Price data DataFrame with OHLCV columns

        Returns:
            DataValidationResult with validation outcome

        Implements requirement 3.2 for price data validation.
        """
        self.validation_stats["total_validations"] += 1

        result = DataValidationResult(
            symbol=symbol, data_type="price", status=ValidationStatus.VALID
        )

        try:
            # Check for empty DataFrame
            if data is None or data.empty:
                result.status = ValidationStatus.INVALID
                result.errors.append("Price data is empty")
                self._update_stats("invalid")
                return result

            # Check required columns
            required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
            missing_columns = [
                col for col in required_columns if col not in data.columns
            ]
            if missing_columns:
                result.status = ValidationStatus.INVALID
                result.errors.append(f"Missing required columns: {missing_columns}")
                self._update_stats("invalid")
                return result

            # Check minimum number of records
            if len(data) < self.config.min_price_records:
                result.warnings.append(
                    f"Insufficient price records: {len(data)} (minimum: {self.config.min_price_records})"
                )

            # Validate price values
            price_columns = ["Open", "High", "Low", "Close"]
            for col in price_columns:
                if col in data.columns:
                    # Check for negative or zero prices
                    invalid_prices = (data[col] <= 0) | data[col].isna()
                    if invalid_prices.any():
                        invalid_count = invalid_prices.sum()
                        result.warnings.append(
                            f"Invalid {col} prices found: {invalid_count} records"
                        )

                    # Check for unreasonable price values
                    valid_prices = data[col][~invalid_prices]
                    if not valid_prices.empty:
                        min_price = valid_prices.min()
                        max_price = valid_prices.max()

                        if min_price < self.config.min_price_value:
                            result.warnings.append(f"Very low {col} price: {min_price}")
                        if max_price > self.config.max_price_value:
                            result.warnings.append(
                                f"Very high {col} price: {max_price}"
                            )

            # Validate OHLC relationships
            if all(col in data.columns for col in ["Open", "High", "Low", "Close"]):
                # High should be >= Open, Low, Close
                high_violations = (
                    (data["High"] < data["Open"])
                    | (data["High"] < data["Low"])
                    | (data["High"] < data["Close"])
                )
                if high_violations.any():
                    result.warnings.append(
                        f"OHLC violations (High): {high_violations.sum()} records"
                    )

                # Low should be <= Open, High, Close
                low_violations = (
                    (data["Low"] > data["Open"])
                    | (data["Low"] > data["High"])
                    | (data["Low"] > data["Close"])
                )
                if low_violations.any():
                    result.warnings.append(
                        f"OHLC violations (Low): {low_violations.sum()} records"
                    )

            # Check for excessive daily price changes
            if "Close" in data.columns and len(data) > 1:
                data_sorted = data.sort_values("Date")
                daily_returns = data_sorted["Close"].pct_change().abs()
                excessive_changes = daily_returns > self.config.max_daily_change
                if excessive_changes.any():
                    max_change = daily_returns.max()
                    result.warnings.append(
                        f"Excessive daily price changes detected: max {max_change*100:.1f}%"
                    )

            # Check for missing trading days (gaps in data)
            if "Date" in data.columns and len(data) > 1:
                data_sorted = data.sort_values("Date")
                date_diffs = data_sorted["Date"].diff()
                # Convert to days if datetime
                if pd.api.types.is_datetime64_any_dtype(date_diffs):
                    date_diffs_days = date_diffs.dt.days
                    long_gaps = date_diffs_days > self.config.max_missing_days
                    if long_gaps.any():
                        max_gap = date_diffs_days.max()
                        result.warnings.append(
                            f"Long data gaps detected: max {max_gap} days"
                        )

            # Validate volume data
            if "Volume" in data.columns:
                negative_volume = data["Volume"] < 0
                if negative_volume.any():
                    result.warnings.append(
                        f"Negative volume detected: {negative_volume.sum()} records"
                    )

                zero_volume = data["Volume"] == 0
                if zero_volume.any():
                    zero_count = zero_volume.sum()
                    zero_ratio = zero_count / len(data)
                    if zero_ratio > 0.1:  # More than 10% zero volume
                        result.warnings.append(
                            f"High zero volume ratio: {zero_ratio*100:.1f}%"
                        )

            # Calculate data quality metrics
            result.additional_info["record_count"] = len(data)
            result.additional_info["date_range"] = {
                "start": data["Date"].min() if "Date" in data.columns else None,
                "end": data["Date"].max() if "Date" in data.columns else None,
            }

            # Apply strict mode if configured
            if self.config.strict_mode and result.warnings:
                result.status = ValidationStatus.INVALID
                result.errors.extend(result.warnings)
                result.warnings.clear()

            # Update statistics
            if result.status == ValidationStatus.VALID:
                self._update_stats("valid")
            elif result.status == ValidationStatus.WARNING:
                self._update_stats("warning")
            else:
                self._update_stats("invalid")

            # Log validation details if configured
            if self.config.log_validation_details:
                self._log_validation_result(result)

            return result

        except Exception as e:
            self.logger.error(f"Error validating price data for {symbol}: {e}")
            result.status = ValidationStatus.ERROR
            result.errors.append(f"Validation error: {str(e)}")
            self._update_stats("error")
            return result

    def validate_dividend_data(
        self, symbol: str, data: pd.DataFrame
    ) -> DataValidationResult:
        """
        Validate dividend data for consistency and reasonableness.

        Args:
            symbol: Stock symbol
            data: Dividend data DataFrame

        Returns:
            DataValidationResult with validation outcome

        Implements requirement 3.2 for dividend data validation.
        """
        self.validation_stats["total_validations"] += 1

        result = DataValidationResult(
            symbol=symbol, data_type="dividend", status=ValidationStatus.VALID
        )

        try:
            # Empty dividend data is acceptable (not all stocks pay dividends)
            if data is None or data.empty:
                result.additional_info["dividend_paying"] = False
                result.additional_info["record_count"] = 0
                self._update_stats("valid")
                return result

            result.additional_info["dividend_paying"] = True
            result.additional_info["record_count"] = len(data)

            # Check required columns
            required_columns = ["Date", "Dividends"]
            missing_columns = [
                col for col in required_columns if col not in data.columns
            ]
            if missing_columns:
                result.status = ValidationStatus.INVALID
                result.errors.append(f"Missing required columns: {missing_columns}")
                self._update_stats("invalid")
                return result

            # Validate dividend values
            if "Dividends" in data.columns:
                # Check for negative dividends
                negative_dividends = data["Dividends"] < 0
                if negative_dividends.any():
                    result.errors.append(
                        f"Negative dividends detected: {negative_dividends.sum()} records"
                    )
                    result.status = ValidationStatus.INVALID

                # Check for zero dividends (unusual but not invalid)
                zero_dividends = data["Dividends"] == 0
                if zero_dividends.any():
                    result.warnings.append(
                        f"Zero dividends detected: {zero_dividends.sum()} records"
                    )

                # Check for very small dividends
                valid_dividends = data["Dividends"][data["Dividends"] > 0]
                if not valid_dividends.empty:
                    min_dividend = valid_dividends.min()
                    if min_dividend < self.config.min_dividend_value:
                        result.warnings.append(
                            f"Very small dividend detected: {min_dividend}"
                        )

                    # Check dividend consistency (large variations might indicate errors)
                    if len(valid_dividends) > 1:
                        dividend_std = valid_dividends.std()
                        dividend_mean = valid_dividends.mean()
                        cv = dividend_std / dividend_mean if dividend_mean > 0 else 0
                        if cv > 2.0:  # Coefficient of variation > 2.0
                            result.warnings.append(
                                f"High dividend variability detected (CV: {cv:.2f})"
                            )

            # Check dividend dates
            if "Date" in data.columns:
                # Check for future dividend dates
                current_date = datetime.now().date()
                if pd.api.types.is_datetime64_any_dtype(data["Date"]):
                    future_dates = data["Date"].dt.date > current_date
                elif pd.api.types.is_object_dtype(data["Date"]):
                    # Try to convert to datetime
                    try:
                        date_series = pd.to_datetime(data["Date"])
                        future_dates = date_series.dt.date > current_date
                    except:
                        result.warnings.append("Could not parse dividend dates")
                        future_dates = pd.Series([False] * len(data))
                else:
                    future_dates = pd.Series([False] * len(data))

                if future_dates.any():
                    result.warnings.append(
                        f"Future dividend dates detected: {future_dates.sum()} records"
                    )

                # Check for recent dividends if required
                if self.config.require_recent_dividends and len(data) > 0:
                    try:
                        latest_date = pd.to_datetime(data["Date"]).max()
                        days_since_last = (datetime.now() - latest_date).days
                        if days_since_last > 365:  # More than 1 year
                            result.warnings.append(
                                f"No recent dividends: last dividend {days_since_last} days ago"
                            )
                    except:
                        result.warnings.append("Could not check recent dividend dates")

            # Calculate dividend metrics
            if "Dividends" in data.columns and not data.empty:
                valid_dividends = data["Dividends"][data["Dividends"] > 0]
                if not valid_dividends.empty:
                    result.additional_info["total_dividends"] = valid_dividends.sum()
                    result.additional_info["average_dividend"] = valid_dividends.mean()
                    result.additional_info["dividend_frequency"] = len(valid_dividends)

                    # Estimate annual dividend yield (rough calculation)
                    if len(valid_dividends) >= 4:  # At least 4 payments
                        recent_dividends = valid_dividends.tail(
                            4
                        ).sum()  # Last 4 payments
                        result.additional_info["estimated_annual_dividend"] = (
                            recent_dividends
                        )

            # Apply strict mode if configured
            if self.config.strict_mode and result.warnings:
                result.status = ValidationStatus.INVALID
                result.errors.extend(result.warnings)
                result.warnings.clear()

            # Update statistics
            if result.status == ValidationStatus.VALID:
                self._update_stats("valid")
            elif result.status == ValidationStatus.WARNING:
                self._update_stats("warning")
            else:
                self._update_stats("invalid")

            # Log validation details if configured
            if self.config.log_validation_details:
                self._log_validation_result(result)

            return result

        except Exception as e:
            self.logger.error(f"Error validating dividend data for {symbol}: {e}")
            result.status = ValidationStatus.ERROR
            result.errors.append(f"Validation error: {str(e)}")
            self._update_stats("error")
            return result

    def check_data_completeness(self, data: Any, required_fields: List[str]) -> bool:
        """
        Check if data contains all required fields with non-null values.

        Args:
            data: Data to check (dict, DataFrame, or other)
            required_fields: List of required field names

        Returns:
            True if all required fields are present and non-null
        """
        try:
            if isinstance(data, dict):
                return all(
                    field in data and data[field] is not None
                    for field in required_fields
                )
            elif isinstance(data, pd.DataFrame):
                return all(
                    field in data.columns and not data[field].isna().all()
                    for field in required_fields
                )
            else:
                # For other data types, try to access as attributes
                return all(
                    hasattr(data, field) and getattr(data, field) is not None
                    for field in required_fields
                )
        except Exception as e:
            self.logger.error(f"Error checking data completeness: {e}")
            return False

    def get_validation_statistics(self) -> Dict[str, Any]:
        """
        Get validation statistics summary.

        Returns:
            Dictionary with validation statistics
        """
        total = self.validation_stats["total_validations"]
        if total == 0:
            return self.validation_stats.copy()

        stats = self.validation_stats.copy()
        stats["success_rate"] = stats["valid_count"] / total
        stats["warning_rate"] = stats["warning_count"] / total
        stats["error_rate"] = (stats["invalid_count"] + stats["error_count"]) / total

        return stats

    def reset_statistics(self) -> None:
        """Reset validation statistics."""
        self.validation_stats = {
            "total_validations": 0,
            "valid_count": 0,
            "invalid_count": 0,
            "warning_count": 0,
            "error_count": 0,
        }

    def _update_stats(self, result_type: str) -> None:
        """Update validation statistics."""
        if result_type in self.validation_stats:
            self.validation_stats[f"{result_type}_count"] += 1

    def _log_validation_result(self, result: DataValidationResult) -> None:
        """Log validation result details."""
        if result.status == ValidationStatus.VALID:
            self.logger.debug(
                f"Validation passed - Symbol: {result.symbol}, "
                f"Type: {result.data_type}, Quality: {result.quality_score:.2f}"
            )
        elif result.status == ValidationStatus.WARNING:
            self.logger.warning(
                f"Validation warnings - Symbol: {result.symbol}, "
                f"Type: {result.data_type}, Warnings: {len(result.warnings)}, "
                f"Details: {'; '.join(result.warnings)}"
            )
        else:
            self.logger.error(
                f"Validation failed - Symbol: {result.symbol}, "
                f"Type: {result.data_type}, Status: {result.status.value}, "
                f"Errors: {len(result.errors)}, Details: {'; '.join(result.errors)}"
            )
