"""
Rotation manager module for efficient stock screening across multiple days.
Handles splitting stocks into groups and determining daily targets.
"""

import logging
import hashlib
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class RotationConfig:
    """Configuration for rotation functionality."""

    enabled: bool = False
    total_groups: int = 5
    group_distribution_method: str = "sector"  # "sector" or "market_cap"


class RotationManager:
    """
    Manages stock rotation for efficient daily screening.

    Handles:
    - Splitting all stocks into equal groups (要件 7.1, 7.3)
    - Determining daily target group based on weekday (要件 7.2, 7.4)
    - Ensuring even distribution across groups (要件 7.3, 7.7)
    - Tracking rotation progress (要件 7.5)
    """

    def __init__(self, total_groups: int = 5):
        """
        Initialize RotationManager.

        Args:
            total_groups: Number of groups to split stocks into (default: 5 for weekdays)
        """
        self.total_groups = total_groups
        self.logger = logging.getLogger(__name__)

        # Weekday mapping (Monday=0, Friday=4)
        self.weekday_names = {
            0: "月曜日",  # Monday
            1: "火曜日",  # Tuesday
            2: "水曜日",  # Wednesday
            3: "木曜日",  # Thursday
            4: "金曜日",  # Friday
        }

        self.weekday_names_en = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
        }

    def split_stocks_into_groups(self, all_stocks: List[str]) -> Dict[int, List[str]]:
        """
        Split all stocks into equal groups for rotation.

        Uses round-robin distribution to ensure even groups.
        This ensures reproducible grouping across different executions.

        Args:
            all_stocks: List of all stock symbols to split

        Returns:
            Dict[int, List[str]]: Dictionary mapping group index (0-4) to list of stocks

        Note: Implements requirements 7.1, 7.3 - equal distribution across groups
        """
        if not all_stocks:
            return {i: [] for i in range(self.total_groups)}

        # Sort stocks to ensure consistent ordering
        sorted_stocks = sorted(all_stocks)

        # Initialize groups
        groups = {i: [] for i in range(self.total_groups)}

        # Distribute stocks using round-robin for even distribution
        for i, stock in enumerate(sorted_stocks):
            group_index = i % self.total_groups
            groups[group_index].append(stock)

        # Log group distribution for monitoring
        self.logger.info(
            f"Split {len(all_stocks)} stocks into {self.total_groups} groups:"
        )
        for group_idx, stocks in groups.items():
            self.logger.info(f"  Group {group_idx}: {len(stocks)} stocks")

        # Verify even distribution (difference should be ≤ 1)
        group_sizes = [len(stocks) for stocks in groups.values()]
        max_size = max(group_sizes)
        min_size = min(group_sizes)

        if max_size - min_size > 1:
            self.logger.warning(
                f"Uneven group distribution detected: max={max_size}, min={min_size}"
            )
        else:
            self.logger.info(
                f"Even distribution achieved: max={max_size}, min={min_size}"
            )

        return groups

    def get_current_group_index(self, current_date: datetime) -> int:
        """
        Determine which group should be processed based on current weekday.

        Args:
            current_date: Current date/datetime

        Returns:
            int: Group index (0 to total_groups-1) corresponding to weekday

        Note: Implements requirement 7.4 - weekday-based group selection
        """
        # Get weekday (Monday=0, Sunday=6)
        weekday = current_date.weekday()

        # Map weekday to group index, considering total_groups
        if weekday < 5:  # Monday to Friday
            group_index = weekday % self.total_groups
        else:
            # Weekend - default to Monday group
            group_index = 0
            self.logger.warning(
                f"Weekend date provided ({current_date.strftime('%A')}), "
                f"defaulting to Monday group (0)"
            )

        self.logger.info(
            f"Date: {current_date.strftime('%Y-%m-%d %A')} -> Group {group_index}"
        )

        return group_index

    def get_stocks_for_today(
        self, all_stocks: List[str], current_date: Optional[datetime] = None
    ) -> List[str]:
        """
        Get the list of stocks to screen for today based on rotation schedule.

        Args:
            all_stocks: Complete list of all available stocks
            current_date: Current date (defaults to today)

        Returns:
            List[str]: List of stocks to screen today

        Note: Implements requirements 7.2, 7.4 - daily group selection
        """
        if current_date is None:
            current_date = datetime.now()

        # Split all stocks into groups
        groups = self.split_stocks_into_groups(all_stocks)

        # Get today's group index
        group_index = self.get_current_group_index(current_date)

        # Return today's stocks
        today_stocks = groups[group_index]

        self.logger.info(
            f"Selected {len(today_stocks)} stocks for "
            f"{self.weekday_names.get(group_index, 'Unknown')} "
            f"(Group {group_index})"
        )

        return today_stocks

    def get_group_info(self, current_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get information about current rotation group and progress.

        Args:
            current_date: Current date (defaults to today)

        Returns:
            Dict containing group information for notifications

        Note: Implements requirement 7.5 - progress tracking
        """
        if current_date is None:
            current_date = datetime.now()

        group_index = self.get_current_group_index(current_date)
        weekday = current_date.weekday()

        # Determine if it's a valid weekday
        is_weekday = weekday < 5

        group_info = {
            "group_index": group_index,
            "group_number": group_index + 1,  # 1-based for display
            "total_groups": self.total_groups,
            "weekday_jp": self.weekday_names.get(group_index, "不明"),
            "weekday_en": self.weekday_names_en.get(group_index, "Unknown"),
            "date": current_date.strftime("%Y-%m-%d"),
            "is_weekday": is_weekday,
            "progress_text_jp": f"{self.weekday_names.get(group_index, '不明')}グループ（{group_index + 1}/{self.total_groups}）",
            "progress_text_en": f"{self.weekday_names_en.get(group_index, 'Unknown')} Group ({group_index + 1}/{self.total_groups})",
        }

        return group_info

    def get_rotation_schedule(self) -> Dict[str, Any]:
        """
        Get the complete rotation schedule for reference.

        Returns:
            Dict containing the full weekly rotation schedule
        """
        schedule = {
            "total_groups": self.total_groups,
            "schedule": {},
            "description_jp": "週次ローテーションスケジュール",
            "description_en": "Weekly Rotation Schedule",
        }

        for i in range(self.total_groups):
            schedule["schedule"][i] = {
                "group_index": i,
                "group_number": i + 1,
                "weekday_jp": self.weekday_names.get(i, "不明"),
                "weekday_en": self.weekday_names_en.get(i, "Unknown"),
            }

        return schedule

    def validate_rotation_setup(self, all_stocks: List[str]) -> Dict[str, Any]:
        """
        Validate that rotation setup will work correctly with given stocks.

        Args:
            all_stocks: List of all stocks to validate against

        Returns:
            Dict containing validation results and statistics
        """
        if not all_stocks:
            return {
                "valid": False,
                "error": "No stocks provided for validation",
                "stats": {},
            }

        # Split stocks and analyze distribution
        groups = self.split_stocks_into_groups(all_stocks)

        group_sizes = [len(stocks) for stocks in groups.values()]
        total_stocks = sum(group_sizes)

        # Calculate statistics
        avg_size = total_stocks / self.total_groups
        max_size = max(group_sizes)
        min_size = min(group_sizes)
        size_difference = max_size - min_size

        # Validation criteria
        is_valid = size_difference <= 1  # Groups should differ by at most 1 stock

        validation_result = {
            "valid": is_valid,
            "total_stocks": total_stocks,
            "total_groups": self.total_groups,
            "stats": {
                "average_group_size": avg_size,
                "max_group_size": max_size,
                "min_group_size": min_size,
                "size_difference": size_difference,
                "group_sizes": group_sizes,
            },
            "estimated_daily_stocks": int(avg_size),
            "coverage_days": self.total_groups,
        }

        if not is_valid:
            validation_result["error"] = (
                f"Uneven distribution: max group has {max_size} stocks, "
                f"min group has {min_size} stocks (difference: {size_difference})"
            )

        return validation_result

    def get_next_group_preview(
        self, all_stocks: List[str], days_ahead: int = 1
    ) -> Dict[str, Any]:
        """
        Preview what stocks will be screened in upcoming days.

        Args:
            all_stocks: Complete list of all available stocks
            days_ahead: Number of days to look ahead (default: 1)

        Returns:
            Dict containing preview information for upcoming groups
        """
        from datetime import timedelta

        current_date = datetime.now()
        preview_date = current_date + timedelta(days=days_ahead)

        # Get stocks for preview date
        preview_stocks = self.get_stocks_for_today(all_stocks, preview_date)
        group_info = self.get_group_info(preview_date)

        return {
            "preview_date": preview_date.strftime("%Y-%m-%d"),
            "weekday_jp": group_info["weekday_jp"],
            "weekday_en": group_info["weekday_en"],
            "group_index": group_info["group_index"],
            "group_number": group_info["group_number"],
            "stock_count": len(preview_stocks),
            "stocks": preview_stocks[:10],  # First 10 stocks as preview
            "total_stocks": len(preview_stocks),
        }
