"""Data models and utilities for the stock value notifier system."""

from dataclasses import dataclass, field
from typing import Optional, List, Set, Dict, Any
from datetime import date, datetime


@dataclass
class ValueStock:
    """Data class representing a value stock with all relevant metrics."""

    code: str  # 銘柄コード
    name: str  # 企業名
    current_price: float  # 現在株価
    per: float  # PER
    pbr: float  # PBR
    dividend_yield: float  # 配当利回り
    dividend_growth_years: int  # 継続増配年数
    revenue_growth_years: int  # 継続増収年数
    profit_growth_years: int  # 継続増益年数
    per_stability: float  # PER変動係数
    score: float = 0.0  # 総合スコア
    sector_17: str = ""  # 17業種区分
    sector_33: str = ""  # 33業種区分
    market_category: str = ""  # 市場・商品区分
    size_category: str = ""  # 規模区分


@dataclass
class TSEStockInfo:
    """Data class representing TSE stock information from official data file."""

    code: str  # 銘柄コード
    name: str  # 銘柄名
    market_category: str  # 市場・商品区分
    sector_33_code: str  # 33業種コード
    sector_33_name: str  # 33業種区分
    sector_17_code: str  # 17業種コード
    sector_17_name: str  # 17業種区分
    size_code: str  # 規模コード
    size_category: str  # 規模区分
    date: str  # データ日付
    is_tradable: bool = True  # 取引可能フラグ
    is_investment_product: bool = False  # 投資商品フラグ（ETF等）


@dataclass
class ScreeningConfig:
    """Configuration for screening criteria."""

    max_per: float = 15.0
    max_pbr: float = 1.5
    min_dividend_yield: float = 2.0
    min_growth_years: int = 3
    max_per_volatility: float = 30.0
    use_tse_official_list: bool = True
    exclude_investment_products: bool = True
    target_markets: List[str] = field(
        default_factory=lambda: [
            "プライム（内国株式）",
            "スタンダード（内国株式）",
            "グロース（内国株式）",
        ]
    )


@dataclass
class SlackConfig:
    """Configuration for Slack notifications."""

    token: str
    channel: str
    username: str = "バリュー株通知Bot"
    icon_emoji: str = ":chart_with_upwards_trend:"


@dataclass
class RotationConfig:
    """Configuration for rotation functionality with TSE support."""

    enabled: bool = False
    total_groups: int = 5
    group_distribution_method: str = (
        "sector"  # "sector", "market_size", "mixed", "round_robin"
    )
    use_17_sector_classification: bool = True  # True: 17業種, False: 33業種
    balance_market_categories: bool = True  # 市場区分の均等配分
    use_tse_metadata: bool = True  # TSEメタデータを使用するかどうか
    auto_optimize_distribution: bool = False  # 自動的に最適な分散方法を選択
    sector_balance_weight: float = 0.3  # セクターバランスの重み（最適化時）
    size_balance_weight: float = 0.3  # サイズバランスの重み（最適化時）
    group_size_weight: float = 0.4  # グループサイズバランスの重み（最適化時）


@dataclass
class TSEDataConfig:
    """Configuration for TSE data management."""

    data_file_path: str = "stock_list/data_j.xls"
    cache_duration_hours: int = 24
    fallback_to_range_validation: bool = True
    excluded_market_categories: List[str] = field(
        default_factory=lambda: [
            "ETF・ETN",
            "REIT・ベンチャーファンド・カントリーファンド・インフラファンド",
            "出資証券",
        ]
    )
    target_market_categories: List[str] = field(
        default_factory=lambda: [
            "プライム（内国株式）",
            "スタンダード（内国株式）",
            "グロース（内国株式）",
            "PRO Market",
        ]
    )


class MarketCalendar:
    """
    Market calendar utility for determining Japanese stock market trading days.

    Handles:
    - Japanese national holidays
    - Market-specific closures (year-end, New Year)
    - Weekend detection
    - Trading day validation (要件 4.2)
    """

    def __init__(self):
        """Initialize MarketCalendar with Japanese holidays."""
        self._japanese_holidays_2024 = {
            date(2024, 1, 1),  # New Year's Day
            date(2024, 1, 8),  # Coming of Age Day
            date(2024, 2, 11),  # National Foundation Day
            date(2024, 2, 12),  # National Foundation Day (observed)
            date(2024, 2, 23),  # Emperor's Birthday
            date(2024, 3, 20),  # Vernal Equinox Day
            date(2024, 4, 29),  # Showa Day
            date(2024, 5, 3),  # Constitution Memorial Day
            date(2024, 5, 4),  # Greenery Day
            date(2024, 5, 5),  # Children's Day
            date(2024, 5, 6),  # Children's Day (observed)
            date(2024, 7, 15),  # Marine Day
            date(2024, 8, 11),  # Mountain Day
            date(2024, 8, 12),  # Mountain Day (observed)
            date(2024, 9, 16),  # Respect for the Aged Day
            date(2024, 9, 22),  # Autumnal Equinox Day
            date(2024, 9, 23),  # Autumnal Equinox Day (observed)
            date(2024, 10, 14),  # Health and Sports Day
            date(2024, 11, 3),  # Culture Day
            date(2024, 11, 4),  # Culture Day (observed)
            date(2024, 11, 23),  # Labor Thanksgiving Day
            date(2024, 12, 31),  # New Year's Eve (market closes early)
        }

        self._japanese_holidays_2025 = {
            date(2025, 1, 1),  # New Year's Day
            date(2025, 1, 13),  # Coming of Age Day
            date(2025, 2, 11),  # National Foundation Day
            date(2025, 2, 23),  # Emperor's Birthday
            date(2025, 2, 24),  # Emperor's Birthday (observed)
            date(2025, 3, 20),  # Vernal Equinox Day
            date(2025, 4, 29),  # Showa Day
            date(2025, 5, 3),  # Constitution Memorial Day
            date(2025, 5, 4),  # Greenery Day
            date(2025, 5, 5),  # Children's Day
            date(2025, 5, 6),  # Children's Day (observed)
            date(2025, 7, 21),  # Marine Day
            date(2025, 8, 11),  # Mountain Day
            date(2025, 9, 15),  # Respect for the Aged Day
            date(2025, 9, 23),  # Autumnal Equinox Day
            date(2025, 10, 13),  # Health and Sports Day
            date(2025, 11, 3),  # Culture Day
            date(2025, 11, 23),  # Labor Thanksgiving Day
            date(2025, 11, 24),  # Labor Thanksgiving Day (observed)
            date(2025, 12, 31),  # New Year's Eve (market closes early)
        }

        # Combine all holidays
        self._all_holidays = self._japanese_holidays_2024.union(
            self._japanese_holidays_2025
        )

    def is_market_open(self, check_date: date) -> bool:
        """
        Check if the Japanese stock market is open on the given date.

        Args:
            check_date: Date to check for market availability

        Returns:
            bool: True if market is open, False otherwise

        Note: Implements requirements 4.1, 4.2 - weekdays only, skip holidays and market closures
        """
        # Check if it's a weekend (Saturday=5, Sunday=6)
        if check_date.weekday() >= 5:
            return False

        # Check if it's a Japanese national holiday
        if check_date in self._all_holidays:
            return False

        # Check for year-end closure (Dec 30-31)
        if check_date.month == 12 and check_date.day >= 30:
            return False

        # Check for New Year closure (Jan 1-3)
        if check_date.month == 1 and check_date.day <= 3:
            return False

        return True

    def is_holiday(self, check_date: date) -> bool:
        """
        Check if the given date is a Japanese national holiday.

        Args:
            check_date: Date to check

        Returns:
            bool: True if it's a holiday, False otherwise
        """
        return check_date in self._all_holidays

    def is_weekend(self, check_date: date) -> bool:
        """
        Check if the given date is a weekend.

        Args:
            check_date: Date to check

        Returns:
            bool: True if it's a weekend, False otherwise
        """
        return check_date.weekday() >= 5

    def get_next_trading_day(self, from_date: date) -> date:
        """
        Get the next trading day after the given date.

        Args:
            from_date: Starting date

        Returns:
            date: Next trading day
        """
        current_date = from_date
        while True:
            current_date = date(
                current_date.year, current_date.month, current_date.day + 1
            )
            if self.is_market_open(current_date):
                return current_date

    def get_trading_days_in_month(self, year: int, month: int) -> List[date]:
        """
        Get all trading days in the specified month.

        Args:
            year: Year
            month: Month (1-12)

        Returns:
            List[date]: List of trading days in the month
        """
        from calendar import monthrange

        trading_days = []
        _, last_day = monthrange(year, month)

        for day in range(1, last_day + 1):
            check_date = date(year, month, day)
            if self.is_market_open(check_date):
                trading_days.append(check_date)

        return trading_days

    def get_holidays_in_year(self, year: int) -> Set[date]:
        """
        Get all holidays in the specified year.

        Args:
            year: Year to get holidays for

        Returns:
            Set[date]: Set of holidays in the year
        """
        if year == 2024:
            return self._japanese_holidays_2024.copy()
        elif year == 2025:
            return self._japanese_holidays_2025.copy()
        else:
            # For other years, return empty set (would need to be extended)
            return set()
