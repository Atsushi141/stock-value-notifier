"""Screening engine module for value stock analysis."""

import pandas as pd
import numpy as np
from typing import List, Optional
from .models import ValueStock, ScreeningConfig


class ScreeningEngine:
    """Engine for screening value stocks based on multiple criteria."""

    def __init__(self, config: Optional[ScreeningConfig] = None):
        """Initialize the screening engine with configuration.

        Args:
            config: Screening configuration. If None, uses default values.
        """
        self.config = config or ScreeningConfig()

    def _safe_float(self, value, default=float("inf")):
        """Safely convert value to float, handling strings and None values."""
        if value is None or pd.isna(value):
            return default

        # Handle string values
        if isinstance(value, str):
            # Common string representations of invalid/missing data
            invalid_strings = ["n/a", "na", "nan", "null", "none", "", "-"]
            if value.lower().strip() in invalid_strings:
                return default

            # Try to convert string to float
            try:
                return float(value)
            except (ValueError, TypeError):
                return default

        # Handle numeric values
        try:
            float_val = float(value)
            # Check for infinity or very large values that indicate missing data
            if not np.isfinite(float_val) or abs(float_val) > 1e10:
                return default
            return float_val
        except (ValueError, TypeError, OverflowError):
            return default

    def screen_value_stocks(self, stock_data: pd.DataFrame) -> List[ValueStock]:
        """Screen stocks based on value criteria.

        Args:
            stock_data: DataFrame containing stock data with columns:
                - code: 銘柄コード
                - name: 企業名
                - current_price: 現在株価
                - per: PER
                - pbr: PBR
                - dividend_yield: 配当利回り
                - financial_data: 財務データ (dict)
                - dividend_data: 配当データ (dict)

        Returns:
            List of ValueStock objects that meet the screening criteria.
        """
        candidates = []

        for _, row in stock_data.iterrows():
            # Basic filtering criteria (要件 2.1, 2.2, 2.3)
            if not self._meets_basic_criteria(row):
                continue

            # Calculate growth metrics
            dividend_growth_years = self._calculate_dividend_growth_years(
                row.get("dividend_data", {})
            )
            revenue_growth_years = self._calculate_revenue_growth_years(
                row.get("financial_data", {})
            )
            profit_growth_years = self._calculate_profit_growth_years(
                row.get("financial_data", {})
            )
            per_stability = self._calculate_per_stability(row.get("financial_data", {}))

            # Check growth requirements (要件 2.4, 2.5, 2.6)
            if dividend_growth_years < self.config.min_growth_years:
                continue
            if revenue_growth_years < self.config.min_growth_years:
                continue
            if profit_growth_years < self.config.min_growth_years:
                continue

            # Create ValueStock object with safe type conversion
            value_stock = ValueStock(
                code=row["code"],
                name=row["name"],
                current_price=self._safe_float(row["current_price"], 0.0),
                per=self._safe_float(row["per"], float("inf")),
                pbr=self._safe_float(row["pbr"], float("inf")),
                dividend_yield=self._safe_float(row["dividend_yield"], 0.0),
                dividend_growth_years=dividend_growth_years,
                revenue_growth_years=revenue_growth_years,
                profit_growth_years=profit_growth_years,
                per_stability=per_stability,
            )

            candidates.append(value_stock)

        # Rank and return candidates
        return self.rank_stocks(candidates)

    def _meets_basic_criteria(self, row: pd.Series) -> bool:
        """Check if stock meets basic screening criteria.

        Args:
            row: Stock data row

        Returns:
            True if stock meets basic criteria, False otherwise
        """
        # Convert values to float with proper error handling
        per = self._safe_float(row["per"], float("inf"))
        pbr = self._safe_float(row["pbr"], float("inf"))
        dividend_yield = self._safe_float(row["dividend_yield"], 0.0)
        current_price = self._safe_float(row["current_price"], 0.0)

        # Check for invalid values
        if not np.isfinite(per) or not np.isfinite(pbr):
            return False

        if current_price <= 0:
            return False

        # 要件 2.1: PER 15倍以下
        if per > self.config.max_per:
            return False

        # 要件 2.2: PBR 1.5倍以下
        if pbr > self.config.max_pbr:
            return False

        # 要件 2.3: 配当利回り2%以上
        if dividend_yield < self.config.min_dividend_yield:
            return False

        return True

    def _calculate_dividend_growth_years(self, dividend_data: dict) -> int:
        """Calculate consecutive years of dividend growth.

        Args:
            dividend_data: Dictionary containing dividend history

        Returns:
            Number of consecutive years of dividend growth
        """
        if not dividend_data or "dividends" not in dividend_data:
            return 0

        dividends = dividend_data["dividends"]
        if len(dividends) < 2:
            return 0

        # Sort by year to ensure chronological order
        sorted_dividends = sorted(dividends, key=lambda x: x.get("year", 0))

        growth_years = 0
        for i in range(1, len(sorted_dividends)):
            current_dividend = sorted_dividends[i].get("dividend", 0)
            previous_dividend = sorted_dividends[i - 1].get("dividend", 0)

            # Handle None values
            if current_dividend is None:
                current_dividend = 0
            if previous_dividend is None:
                previous_dividend = 0

            if current_dividend > previous_dividend and previous_dividend > 0:
                growth_years += 1
            else:
                break

        return growth_years

    def _calculate_revenue_growth_years(self, financial_data: dict) -> int:
        """Calculate consecutive years of revenue growth.

        Args:
            financial_data: Dictionary containing financial statements

        Returns:
            Number of consecutive years of revenue growth
        """
        if not financial_data or "statements" not in financial_data:
            return 0

        statements = financial_data["statements"]
        if len(statements) < 2:
            return 0

        # Sort by year to ensure chronological order
        sorted_statements = sorted(statements, key=lambda x: x.get("year", 0))

        growth_years = 0
        for i in range(1, len(sorted_statements)):
            current_revenue = sorted_statements[i].get("revenue", 0)
            previous_revenue = sorted_statements[i - 1].get("revenue", 0)

            # Handle None values
            if current_revenue is None:
                current_revenue = 0
            if previous_revenue is None:
                previous_revenue = 0

            if current_revenue > previous_revenue and previous_revenue > 0:
                growth_years += 1
            else:
                break

        return growth_years

    def _calculate_profit_growth_years(self, financial_data: dict) -> int:
        """Calculate consecutive years of profit growth.

        Args:
            financial_data: Dictionary containing financial statements

        Returns:
            Number of consecutive years of profit growth
        """
        if not financial_data or "statements" not in financial_data:
            return 0

        statements = financial_data["statements"]
        if len(statements) < 2:
            return 0

        # Sort by year to ensure chronological order
        sorted_statements = sorted(statements, key=lambda x: x.get("year", 0))

        growth_years = 0
        for i in range(1, len(sorted_statements)):
            current_profit = sorted_statements[i].get("net_income", 0)
            previous_profit = sorted_statements[i - 1].get("net_income", 0)

            # Handle None values
            if current_profit is None:
                current_profit = 0
            if previous_profit is None:
                previous_profit = 0

            if current_profit > previous_profit and previous_profit > 0:
                growth_years += 1
            else:
                break

        return growth_years

    def calculate_per_stability(self, financial_data: dict) -> float:
        """Calculate PER stability (coefficient of variation).

        Args:
            financial_data: Dictionary containing financial statements with PER data

        Returns:
            PER coefficient of variation as percentage
        """
        if not financial_data or "statements" not in financial_data:
            return float("inf")  # Unstable if no data

        statements = financial_data["statements"]
        if len(statements) < 2:
            return float("inf")  # Need at least 2 years of data

        # Extract PER values
        per_values = []
        for statement in statements:
            per = statement.get("per", 0)
            # Handle None values
            if per is None:
                per = 0
            if per > 0:  # Only include positive PER values
                per_values.append(per)

        if len(per_values) < 2:
            return float("inf")  # Need at least 2 valid PER values

        # Calculate coefficient of variation (CV = std/mean * 100)
        per_array = np.array(per_values)
        mean_per = np.mean(per_array)
        std_per = np.std(per_array)

        if mean_per == 0:
            return float("inf")

        cv = (std_per / mean_per) * 100
        return cv

    def _calculate_per_stability(self, financial_data: dict) -> float:
        """Calculate PER stability (coefficient of variation)."""
        return self.calculate_per_stability(financial_data)

    def check_dividend_growth(self, dividend_data: dict) -> bool:
        """Check if stock has consistent dividend growth.

        Args:
            dividend_data: Dictionary containing dividend history

        Returns:
            True if stock has at least min_growth_years of dividend growth
        """
        growth_years = self._calculate_dividend_growth_years(dividend_data)
        return growth_years >= self.config.min_growth_years

    def check_revenue_growth(self, financial_data: dict) -> bool:
        """Check if stock has consistent revenue growth.

        Args:
            financial_data: Dictionary containing financial statements

        Returns:
            True if stock has at least min_growth_years of revenue growth
        """
        growth_years = self._calculate_revenue_growth_years(financial_data)
        return growth_years >= self.config.min_growth_years

    def check_profit_growth(self, financial_data: dict) -> bool:
        """Check if stock has consistent profit growth.

        Args:
            financial_data: Dictionary containing financial statements

        Returns:
            True if stock has at least min_growth_years of profit growth
        """
        growth_years = self._calculate_profit_growth_years(financial_data)
        return growth_years >= self.config.min_growth_years

    def rank_stocks(self, candidates: List[ValueStock]) -> List[ValueStock]:
        """Rank stocks based on multiple criteria and assign scores.

        Args:
            candidates: List of ValueStock candidates

        Returns:
            List of ValueStock objects ranked by score (highest first)
        """
        for stock in candidates:
            score = 0.0

            # Basic criteria scoring (lower is better for PER/PBR, higher for dividend yield)
            score += max(
                0, (self.config.max_per - stock.per) / self.config.max_per * 25
            )  # 25 points max
            score += max(
                0, (self.config.max_pbr - stock.pbr) / self.config.max_pbr * 25
            )  # 25 points max
            score += min(
                stock.dividend_yield / self.config.min_dividend_yield * 20, 20
            )  # 20 points max

            # Growth criteria scoring
            score += min(
                stock.dividend_growth_years * 5, 15
            )  # 15 points max (3 years * 5)
            score += min(stock.revenue_growth_years * 5, 15)  # 15 points max
            score += min(stock.profit_growth_years * 5, 15)  # 15 points max

            # PER stability scoring (lower volatility is better)
            if stock.per_stability <= self.config.max_per_volatility:
                stability_score = max(
                    0,
                    (self.config.max_per_volatility - stock.per_stability)
                    / self.config.max_per_volatility
                    * 10,
                )
                score += stability_score  # 10 points max

            stock.score = score

        # Sort by score in descending order
        return sorted(candidates, key=lambda x: x.score, reverse=True)
