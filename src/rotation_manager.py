"""
Rotation manager module for efficient stock screening across multiple days.
Handles splitting stocks into groups and determining daily targets.
Enhanced with TSE metadata support for intelligent distribution.
"""

import logging
import hashlib
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from collections import defaultdict

from .tse_stock_list_manager import TSEStockListManager
from .models import RotationConfig


@dataclass
class RotationConfig:
    """Configuration for rotation functionality."""

    enabled: bool = False
    total_groups: int = 5
    group_distribution_method: str = "sector"  # "sector", "market_size", "mixed"
    use_17_sector_classification: bool = True  # True: 17業種, False: 33業種
    balance_market_categories: bool = True  # 市場区分の均等配分


class RotationManager:
    """
    Manages stock rotation for efficient daily screening with TSE metadata support.

    Handles:
    - Splitting all stocks into equal groups (要件 7.1, 7.3)
    - Determining daily target group based on weekday (要件 7.2, 7.4)
    - Ensuring even distribution across groups (要件 7.3, 7.7)
    - Tracking rotation progress (要件 7.5)
    - TSE metadata-based intelligent distribution (要件 7.8)
    - Sector-based distribution (17業種 or 33業種) (要件 7.3, 7.7)
    - Market size-based distribution (要件 7.3, 7.7)
    - Mixed distribution strategies (要件 7.8)
    """

    def __init__(
        self,
        total_groups: int = 5,
        tse_manager: Optional[TSEStockListManager] = None,
        config: Optional[RotationConfig] = None,
    ):
        """
        Initialize RotationManager with optional TSE support and configuration.

        Args:
            total_groups: Number of groups to split stocks into (default: 5 for weekdays)
            tse_manager: TSE Stock List Manager for metadata-based distribution
            config: Rotation configuration object
        """
        self.total_groups = total_groups
        self.tse_manager = tse_manager
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Override total_groups if provided in config
        if self.config and self.config.total_groups:
            self.total_groups = self.config.total_groups

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

    def split_stocks_into_groups(
        self, all_stocks: List[str], distribution_method: str = "round_robin"
    ) -> Dict[int, List[str]]:
        """
        Split all stocks into equal groups for rotation using specified distribution method.

        Args:
            all_stocks: List of all stock symbols to split
            distribution_method: Method to use for distribution
                - "round_robin": Simple round-robin distribution (default)
                - "sector": Distribute by sector classification
                - "market_size": Distribute by market size
                - "mixed": Mixed distribution using multiple criteria

        Returns:
            Dict[int, List[str]]: Dictionary mapping group index (0-4) to list of stocks

        Note: Implements requirements 7.1, 7.3, 7.7, 7.8 - equal distribution across groups
        """
        if not all_stocks:
            return {i: [] for i in range(self.total_groups)}

        # Choose distribution method
        if distribution_method == "sector" and self.tse_manager:
            return self.split_by_sector(all_stocks)
        elif distribution_method == "market_size" and self.tse_manager:
            return self.split_by_market_size(all_stocks)
        elif distribution_method == "mixed" and self.tse_manager:
            return self.split_by_mixed_criteria(all_stocks)
        else:
            # Fallback to round-robin distribution
            return self.split_by_round_robin(all_stocks)

    def split_by_round_robin(self, all_stocks: List[str]) -> Dict[int, List[str]]:
        """
        Split stocks using simple round-robin distribution.
        This is the original implementation for backward compatibility.

        Args:
            all_stocks: List of all stock symbols to split

        Returns:
            Dict[int, List[str]]: Dictionary mapping group index to list of stocks
        """
        # Sort stocks to ensure consistent ordering
        sorted_stocks = sorted(all_stocks)

        # Initialize groups
        groups = {i: [] for i in range(self.total_groups)}

        # Distribute stocks using round-robin for even distribution
        for i, stock in enumerate(sorted_stocks):
            group_index = i % self.total_groups
            groups[group_index].append(stock)

        self._log_group_distribution(groups, "round-robin")
        return groups

    def split_by_sector(
        self, all_stocks: List[str], use_17_sector: bool = True
    ) -> Dict[int, List[str]]:
        """
        Split stocks by sector classification for balanced sector representation.

        Args:
            all_stocks: List of all stock symbols to split
            use_17_sector: If True, use 17-sector classification; if False, use 33-sector

        Returns:
            Dict[int, List[str]]: Dictionary mapping group index to list of stocks

        Note: Implements requirements 7.3, 7.7, 7.8 - sector-based distribution
        """
        if not self.tse_manager:
            self.logger.warning(
                "TSE manager not available, falling back to round-robin"
            )
            return self.split_by_round_robin(all_stocks)

        try:
            # Get sector information for all stocks
            sector_groups = defaultdict(list)
            unclassified_stocks = []

            for stock in all_stocks:
                metadata = self.tse_manager.get_stock_metadata(stock)
                if metadata:
                    if use_17_sector:
                        sector = metadata.get("sector_17_name", "未分類")
                    else:
                        sector = metadata.get("sector_33_name", "未分類")

                    if sector and sector != "未分類" and sector != "-":
                        sector_groups[sector].append(stock)
                    else:
                        unclassified_stocks.append(stock)
                else:
                    unclassified_stocks.append(stock)

            # Initialize rotation groups
            groups = {i: [] for i in range(self.total_groups)}

            # Distribute each sector across all groups
            for sector, stocks in sector_groups.items():
                sorted_stocks = sorted(stocks)
                for i, stock in enumerate(sorted_stocks):
                    group_index = i % self.total_groups
                    groups[group_index].append(stock)

            # Distribute unclassified stocks using round-robin
            for i, stock in enumerate(sorted(unclassified_stocks)):
                group_index = i % self.total_groups
                groups[group_index].append(stock)

            # Log sector distribution statistics
            sector_classification = "17業種" if use_17_sector else "33業種"
            self.logger.info(
                f"Sector-based distribution ({sector_classification}): "
                f"{len(sector_groups)} sectors, {len(unclassified_stocks)} unclassified"
            )

            self._log_group_distribution(groups, f"sector-{sector_classification}")
            return groups

        except Exception as e:
            self.logger.error(f"Failed to split by sector: {e}")
            return self.split_by_round_robin(all_stocks)

    def split_by_market_size(self, all_stocks: List[str]) -> Dict[int, List[str]]:
        """
        Split stocks by market size category for balanced size representation.

        Args:
            all_stocks: List of all stock symbols to split

        Returns:
            Dict[int, List[str]]: Dictionary mapping group index to list of stocks

        Note: Implements requirements 7.3, 7.7, 7.8 - market size-based distribution
        """
        if not self.tse_manager:
            self.logger.warning(
                "TSE manager not available, falling back to round-robin"
            )
            return self.split_by_round_robin(all_stocks)

        try:
            # Get size category information for all stocks
            size_groups = defaultdict(list)
            unclassified_stocks = []

            for stock in all_stocks:
                metadata = self.tse_manager.get_stock_metadata(stock)
                if metadata:
                    size_category = metadata.get("size_category", "未分類")
                    if (
                        size_category
                        and size_category != "未分類"
                        and size_category != "-"
                    ):
                        size_groups[size_category].append(stock)
                    else:
                        unclassified_stocks.append(stock)
                else:
                    unclassified_stocks.append(stock)

            # Initialize rotation groups
            groups = {i: [] for i in range(self.total_groups)}

            # Distribute each size category across all groups
            for size_category, stocks in size_groups.items():
                sorted_stocks = sorted(stocks)
                for i, stock in enumerate(sorted_stocks):
                    group_index = i % self.total_groups
                    groups[group_index].append(stock)

            # Distribute unclassified stocks using round-robin
            for i, stock in enumerate(sorted(unclassified_stocks)):
                group_index = i % self.total_groups
                groups[group_index].append(stock)

            # Log size distribution statistics
            self.logger.info(
                f"Market size-based distribution: "
                f"{len(size_groups)} size categories, {len(unclassified_stocks)} unclassified"
            )

            self._log_group_distribution(groups, "market-size")
            return groups

        except Exception as e:
            self.logger.error(f"Failed to split by market size: {e}")
            return self.split_by_round_robin(all_stocks)

    def split_by_mixed_criteria(
        self, all_stocks: List[str], use_17_sector: bool = True
    ) -> Dict[int, List[str]]:
        """
        Split stocks using mixed criteria (sector + market size + market category).

        Args:
            all_stocks: List of all stock symbols to split
            use_17_sector: If True, use 17-sector classification; if False, use 33-sector

        Returns:
            Dict[int, List[str]]: Dictionary mapping group index to list of stocks

        Note: Implements requirements 7.3, 7.7, 7.8 - mixed distribution strategy
        """
        if not self.tse_manager:
            self.logger.warning(
                "TSE manager not available, falling back to round-robin"
            )
            return self.split_by_round_robin(all_stocks)

        try:
            # Create composite classification for each stock
            composite_groups = defaultdict(list)
            unclassified_stocks = []

            for stock in all_stocks:
                metadata = self.tse_manager.get_stock_metadata(stock)
                if metadata:
                    # Create composite key from sector, size, and market
                    sector = metadata.get(
                        "sector_17_name" if use_17_sector else "sector_33_name",
                        "未分類",
                    )
                    size = metadata.get("size_category", "未分類")
                    market = metadata.get("market_category", "未分類")

                    # Create a composite classification
                    if all(
                        x and x != "未分類" and x != "-" for x in [sector, size, market]
                    ):
                        # Use hash of composite key to ensure even distribution
                        composite_key = f"{sector}_{size}_{market}"
                        composite_groups[composite_key].append(stock)
                    else:
                        unclassified_stocks.append(stock)
                else:
                    unclassified_stocks.append(stock)

            # Initialize rotation groups
            groups = {i: [] for i in range(self.total_groups)}

            # Distribute each composite group across all rotation groups
            for composite_key, stocks in composite_groups.items():
                sorted_stocks = sorted(stocks)
                for i, stock in enumerate(sorted_stocks):
                    group_index = i % self.total_groups
                    groups[group_index].append(stock)

            # Distribute unclassified stocks using round-robin
            for i, stock in enumerate(sorted(unclassified_stocks)):
                group_index = i % self.total_groups
                groups[group_index].append(stock)

            # Log mixed distribution statistics
            sector_classification = "17業種" if use_17_sector else "33業種"
            self.logger.info(
                f"Mixed criteria distribution ({sector_classification}): "
                f"{len(composite_groups)} composite groups, {len(unclassified_stocks)} unclassified"
            )

            self._log_group_distribution(groups, f"mixed-{sector_classification}")
            return groups

        except Exception as e:
            self.logger.error(f"Failed to split by mixed criteria: {e}")
            return self.split_by_round_robin(all_stocks)

    def _log_group_distribution(
        self, groups: Dict[int, List[str]], method: str
    ) -> None:
        """
        Log group distribution statistics for monitoring.

        Args:
            groups: Dictionary mapping group index to list of stocks
            method: Distribution method used
        """
        total_stocks = sum(len(stocks) for stocks in groups.values())

        self.logger.info(
            f"Split {total_stocks} stocks into {self.total_groups} groups using {method}:"
        )

        for group_idx, stocks in groups.items():
            self.logger.info(f"  Group {group_idx}: {len(stocks)} stocks")

        # Verify even distribution (difference should be ≤ 1)
        group_sizes = [len(stocks) for stocks in groups.values()]
        max_size = max(group_sizes) if group_sizes else 0
        min_size = min(group_sizes) if group_sizes else 0

        if max_size - min_size > 1:
            self.logger.warning(
                f"Uneven group distribution detected: max={max_size}, min={min_size}"
            )
        else:
            self.logger.info(
                f"Even distribution achieved: max={max_size}, min={min_size}"
            )

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
        self,
        all_stocks: List[str],
        current_date: Optional[datetime] = None,
        distribution_method: str = "round_robin",
    ) -> List[str]:
        """
        Get the list of stocks to screen for today based on rotation schedule.

        Args:
            all_stocks: Complete list of all available stocks
            current_date: Current date (defaults to today)
            distribution_method: Method to use for stock distribution

        Returns:
            List[str]: List of stocks to screen today

        Note: Implements requirements 7.2, 7.4 - daily group selection
        """
        if current_date is None:
            current_date = datetime.now()

        # Split all stocks into groups using specified method
        groups = self.split_stocks_into_groups(all_stocks, distribution_method)

        # Get today's group index
        group_index = self.get_current_group_index(current_date)

        # Return today's stocks
        today_stocks = groups[group_index]

        self.logger.info(
            f"Selected {len(today_stocks)} stocks for "
            f"{self.weekday_names.get(group_index, 'Unknown')} "
            f"(Group {group_index}) using {distribution_method} distribution"
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

    def validate_rotation_setup(
        self, all_stocks: List[str], distribution_method: str = "round_robin"
    ) -> Dict[str, Any]:
        """
        Validate that rotation setup will work correctly with given stocks.

        Args:
            all_stocks: List of all stocks to validate against
            distribution_method: Distribution method to validate

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
        groups = self.split_stocks_into_groups(all_stocks, distribution_method)

        group_sizes = [len(stocks) for stocks in groups.values()]
        total_stocks = sum(group_sizes)

        # Calculate statistics
        avg_size = total_stocks / self.total_groups
        max_size = max(group_sizes) if group_sizes else 0
        min_size = min(group_sizes) if group_sizes else 0
        size_difference = max_size - min_size

        # Validation criteria
        is_valid = size_difference <= 1  # Groups should differ by at most 1 stock

        validation_result = {
            "valid": is_valid,
            "total_stocks": total_stocks,
            "total_groups": self.total_groups,
            "distribution_method": distribution_method,
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
        self,
        all_stocks: List[str],
        days_ahead: int = 1,
        distribution_method: str = "round_robin",
    ) -> Dict[str, Any]:
        """
        Preview what stocks will be screened in upcoming days.

        Args:
            all_stocks: Complete list of all available stocks
            days_ahead: Number of days to look ahead (default: 1)
            distribution_method: Distribution method to use

        Returns:
            Dict containing preview information for upcoming groups
        """
        from datetime import timedelta

        current_date = datetime.now()
        preview_date = current_date + timedelta(days=days_ahead)

        # Get stocks for preview date
        preview_stocks = self.get_stocks_for_today(
            all_stocks, preview_date, distribution_method
        )
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
            "distribution_method": distribution_method,
        }

    def get_tse_distribution_analysis(
        self, all_stocks: List[str], distribution_method: str = "sector"
    ) -> Dict[str, Any]:
        """
        Analyze TSE metadata distribution across rotation groups.

        Args:
            all_stocks: List of all stock symbols
            distribution_method: Distribution method to analyze

        Returns:
            Dict containing detailed distribution analysis

        Note: Implements requirement 7.8 - TSE metadata analysis
        """
        if not self.tse_manager:
            return {"error": "TSE manager not available"}

        try:
            groups = self.split_stocks_into_groups(all_stocks, distribution_method)

            analysis = {
                "distribution_method": distribution_method,
                "total_stocks": len(all_stocks),
                "total_groups": self.total_groups,
                "group_analysis": {},
                "sector_distribution": {},
                "size_distribution": {},
                "market_distribution": {},
            }

            # Analyze each group
            for group_idx, stocks in groups.items():
                group_analysis = {
                    "stock_count": len(stocks),
                    "sectors": defaultdict(int),
                    "sizes": defaultdict(int),
                    "markets": defaultdict(int),
                }

                for stock in stocks:
                    metadata = self.tse_manager.get_stock_metadata(stock)
                    if metadata:
                        # Count sectors (17業種)
                        sector = metadata.get("sector_17_name", "未分類")
                        if sector and sector != "-":
                            group_analysis["sectors"][sector] += 1

                        # Count sizes
                        size = metadata.get("size_category", "未分類")
                        if size and size != "-":
                            group_analysis["sizes"][size] += 1

                        # Count markets
                        market = metadata.get("market_category", "未分類")
                        if market and market != "-":
                            group_analysis["markets"][market] += 1

                analysis["group_analysis"][group_idx] = {
                    "stock_count": group_analysis["stock_count"],
                    "sectors": dict(group_analysis["sectors"]),
                    "sizes": dict(group_analysis["sizes"]),
                    "markets": dict(group_analysis["markets"]),
                }

            # Calculate overall distribution balance
            analysis["balance_metrics"] = self._calculate_balance_metrics(
                analysis["group_analysis"]
            )

            return analysis

        except Exception as e:
            self.logger.error(f"Failed to analyze TSE distribution: {e}")
            return {"error": str(e)}

    def _calculate_balance_metrics(
        self, group_analysis: Dict[int, Dict]
    ) -> Dict[str, float]:
        """
        Calculate balance metrics for distribution analysis.

        Args:
            group_analysis: Analysis data for each group

        Returns:
            Dict containing balance metrics
        """
        try:
            group_sizes = [data["stock_count"] for data in group_analysis.values()]

            if not group_sizes:
                return {}

            # Calculate coefficient of variation for group sizes
            import statistics

            mean_size = statistics.mean(group_sizes)
            std_size = statistics.stdev(group_sizes) if len(group_sizes) > 1 else 0
            cv_size = (std_size / mean_size) * 100 if mean_size > 0 else 0

            # Calculate sector balance (how evenly sectors are distributed)
            all_sectors = set()
            for data in group_analysis.values():
                all_sectors.update(data["sectors"].keys())

            sector_balance_scores = []
            for sector in all_sectors:
                sector_counts = [
                    data["sectors"].get(sector, 0) for data in group_analysis.values()
                ]
                if sum(sector_counts) > 0:
                    sector_mean = statistics.mean(sector_counts)
                    sector_std = (
                        statistics.stdev(sector_counts) if len(sector_counts) > 1 else 0
                    )
                    sector_cv = (
                        (sector_std / sector_mean) * 100 if sector_mean > 0 else 0
                    )
                    sector_balance_scores.append(sector_cv)

            avg_sector_balance = (
                statistics.mean(sector_balance_scores) if sector_balance_scores else 0
            )

            return {
                "group_size_cv": cv_size,
                "average_sector_balance_cv": avg_sector_balance,
                "min_group_size": min(group_sizes),
                "max_group_size": max(group_sizes),
                "size_difference": max(group_sizes) - min(group_sizes),
            }

        except Exception as e:
            self.logger.error(f"Failed to calculate balance metrics: {e}")
            return {}

    def get_optimal_distribution_method(self, all_stocks: List[str]) -> Dict[str, Any]:
        """
        Determine the optimal distribution method for given stocks.

        Args:
            all_stocks: List of all stock symbols

        Returns:
            Dict containing optimal method and analysis

        Note: Implements requirement 7.8 - intelligent distribution selection
        """
        if not self.tse_manager:
            return {
                "optimal_method": "round_robin",
                "reason": "TSE manager not available",
            }

        try:
            methods_to_test = ["round_robin", "sector", "market_size", "mixed"]
            results = {}

            for method in methods_to_test:
                analysis = self.get_tse_distribution_analysis(all_stocks, method)
                if "error" not in analysis:
                    balance_metrics = analysis.get("balance_metrics", {})
                    results[method] = {
                        "group_size_cv": balance_metrics.get("group_size_cv", 100),
                        "sector_balance_cv": balance_metrics.get(
                            "average_sector_balance_cv", 100
                        ),
                        "size_difference": balance_metrics.get("size_difference", 999),
                    }

            if not results:
                return {
                    "optimal_method": "round_robin",
                    "reason": "No valid analysis results",
                }

            # Score each method (lower is better)
            method_scores = {}
            for method, metrics in results.items():
                # Weight: size difference (40%), group size CV (30%), sector balance CV (30%)
                score = (
                    metrics["size_difference"] * 0.4
                    + metrics["group_size_cv"] * 0.3
                    + metrics["sector_balance_cv"] * 0.3
                )
                method_scores[method] = score

            # Find optimal method
            optimal_method = min(method_scores, key=method_scores.get)
            optimal_score = method_scores[optimal_method]

            return {
                "optimal_method": optimal_method,
                "optimal_score": optimal_score,
                "all_scores": method_scores,
                "analysis_results": results,
                "reason": f"Best balance of group size and sector distribution",
            }

        except Exception as e:
            self.logger.error(f"Failed to determine optimal distribution method: {e}")
            return {
                "optimal_method": "round_robin",
                "reason": f"Analysis failed: {e}",
            }

    def get_sector_coverage_report(
        self, all_stocks: List[str], distribution_method: str = "sector"
    ) -> Dict[str, Any]:
        """
        Generate a report on sector coverage across rotation groups.

        Args:
            all_stocks: List of all stock symbols
            distribution_method: Distribution method to analyze

        Returns:
            Dict containing sector coverage report

        Note: Implements requirement 7.8 - sector coverage analysis
        """
        if not self.tse_manager:
            return {"error": "TSE manager not available"}

        try:
            groups = self.split_stocks_into_groups(all_stocks, distribution_method)

            # Get all available sectors
            sector_classifications = self.tse_manager.get_sector_classifications()
            all_17_sectors = {
                s["name"] for s in sector_classifications.get("sector_17", [])
            }

            report = {
                "distribution_method": distribution_method,
                "total_sectors": len(all_17_sectors),
                "sector_coverage": {},
                "group_sector_counts": {},
                "uncovered_sectors": set(all_17_sectors),
            }

            # Analyze sector coverage for each group
            for group_idx, stocks in groups.items():
                group_sectors = set()
                sector_counts = defaultdict(int)

                for stock in stocks:
                    metadata = self.tse_manager.get_stock_metadata(stock)
                    if metadata:
                        sector = metadata.get("sector_17_name", "")
                        if sector and sector != "-" and sector in all_17_sectors:
                            group_sectors.add(sector)
                            sector_counts[sector] += 1
                            report["uncovered_sectors"].discard(sector)

                report["group_sector_counts"][group_idx] = dict(sector_counts)
                report["sector_coverage"][group_idx] = {
                    "covered_sectors": len(group_sectors),
                    "coverage_percentage": (
                        (len(group_sectors) / len(all_17_sectors)) * 100
                        if all_17_sectors
                        else 0
                    ),
                    "sectors": list(group_sectors),
                }

            # Calculate overall coverage statistics
            total_covered = len(all_17_sectors) - len(report["uncovered_sectors"])
            report["overall_coverage"] = {
                "covered_sectors": total_covered,
                "uncovered_sectors": len(report["uncovered_sectors"]),
                "coverage_percentage": (
                    (total_covered / len(all_17_sectors)) * 100 if all_17_sectors else 0
                ),
                "uncovered_sector_list": list(report["uncovered_sectors"]),
            }

            return report

        except Exception as e:
            self.logger.error(f"Failed to generate sector coverage report: {e}")
            return {"error": str(e)}

    def get_stocks_for_today_with_config(
        self, all_stocks: List[str], current_date: Optional[datetime] = None
    ) -> List[str]:
        """
        Get stocks for today using configuration-based distribution method.

        Args:
            all_stocks: Complete list of all available stocks
            current_date: Current date (defaults to today)

        Returns:
            List[str]: List of stocks to screen today

        Note: Implements requirement 7.6 - configuration-driven distribution
        """
        if current_date is None:
            current_date = datetime.now()

        # Determine distribution method from configuration
        distribution_method = self._get_effective_distribution_method(all_stocks)

        return self.get_stocks_for_today(all_stocks, current_date, distribution_method)

    def _get_effective_distribution_method(self, all_stocks: List[str]) -> str:
        """
        Determine the effective distribution method based on configuration.

        Args:
            all_stocks: List of all stocks (used for auto-optimization)

        Returns:
            str: Distribution method to use

        Note: Implements requirement 7.6 - intelligent method selection
        """
        if not self.config:
            return "round_robin"

        # Check if auto-optimization is enabled
        if self.config.auto_optimize_distribution and self.tse_manager:
            try:
                optimization_result = self.get_optimal_distribution_method(all_stocks)
                optimal_method = optimization_result.get(
                    "optimal_method", "round_robin"
                )
                self.logger.info(
                    f"Auto-optimization selected method: {optimal_method} "
                    f"(score: {optimization_result.get('optimal_score', 'N/A')})"
                )
                return optimal_method
            except Exception as e:
                self.logger.error(f"Auto-optimization failed: {e}")
                # Fall back to configured method

        # Use configured method
        method = self.config.group_distribution_method

        # Check if TSE metadata is required but not available
        if (
            method in ["sector", "market_size", "mixed"]
            and not self.config.use_tse_metadata
        ):
            self.logger.warning(
                f"Method {method} requires TSE metadata but USE_TSE_METADATA=false. "
                f"Falling back to round_robin."
            )
            return "round_robin"

        if method in ["sector", "market_size", "mixed"] and not self.tse_manager:
            self.logger.warning(
                f"Method {method} requires TSE manager but none provided. "
                f"Falling back to round_robin."
            )
            return "round_robin"

        return method

    def get_configuration_status(self) -> Dict[str, Any]:
        """
        Get current configuration status and capabilities.

        Returns:
            Dict containing configuration status information

        Note: Implements requirement 7.6 - configuration monitoring
        """
        status = {
            "config_available": self.config is not None,
            "tse_manager_available": self.tse_manager is not None,
            "total_groups": self.total_groups,
        }

        if self.config:
            status.update(
                {
                    "enabled": self.config.enabled,
                    "distribution_method": self.config.group_distribution_method,
                    "use_17_sector": self.config.use_17_sector_classification,
                    "balance_markets": self.config.balance_market_categories,
                    "use_tse_metadata": self.config.use_tse_metadata,
                    "auto_optimize": self.config.auto_optimize_distribution,
                    "optimization_weights": {
                        "sector": self.config.sector_balance_weight,
                        "size": self.config.size_balance_weight,
                        "group_size": self.config.group_size_weight,
                    },
                }
            )

            # Check method compatibility
            method = self.config.group_distribution_method
            tse_required_methods = ["sector", "market_size", "mixed"]

            if method in tse_required_methods:
                status["tse_required"] = True
                status["tse_compatible"] = (
                    self.config.use_tse_metadata and self.tse_manager is not None
                )
                if not status["tse_compatible"]:
                    status["warning"] = (
                        f"Method '{method}' requires TSE metadata but "
                        f"TSE manager is {'not available' if not self.tse_manager else 'disabled'}"
                    )
            else:
                status["tse_required"] = False
                status["tse_compatible"] = True

        else:
            status.update(
                {
                    "enabled": False,
                    "distribution_method": "round_robin",
                    "warning": "No configuration provided, using defaults",
                }
            )

        return status

    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate current configuration and report any issues.

        Returns:
            Dict containing validation results

        Note: Implements requirement 7.6 - configuration validation
        """
        validation = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "recommendations": [],
        }

        if not self.config:
            validation["errors"].append("No rotation configuration provided")
            validation["valid"] = False
            return validation

        # Validate basic settings
        if self.config.total_groups <= 0 or self.config.total_groups > 10:
            validation["errors"].append(
                f"Invalid total_groups: {self.config.total_groups} (must be 1-10)"
            )
            validation["valid"] = False

        # Validate distribution method
        valid_methods = ["round_robin", "sector", "market_size", "mixed"]
        if self.config.group_distribution_method not in valid_methods:
            validation["errors"].append(
                f"Invalid distribution method: {self.config.group_distribution_method}"
            )
            validation["valid"] = False

        # Check TSE requirements
        tse_methods = ["sector", "market_size", "mixed"]
        if self.config.group_distribution_method in tse_methods:
            if not self.config.use_tse_metadata:
                validation["warnings"].append(
                    f"Method '{self.config.group_distribution_method}' works best with TSE metadata enabled"
                )
                validation["recommendations"].append("Set USE_TSE_METADATA=true")

            if not self.tse_manager:
                validation["errors"].append(
                    f"Method '{self.config.group_distribution_method}' requires TSE manager"
                )
                validation["valid"] = False

        # Validate optimization weights
        if self.config.auto_optimize_distribution:
            total_weight = (
                self.config.sector_balance_weight
                + self.config.size_balance_weight
                + self.config.group_size_weight
            )
            if abs(total_weight - 1.0) > 0.1:
                validation["warnings"].append(
                    f"Optimization weights sum to {total_weight:.3f}, not 1.0"
                )
                validation["recommendations"].append(
                    "Adjust weights so they sum to 1.0 for proper optimization"
                )

        # Performance recommendations
        if self.config.total_groups > 7:
            validation["recommendations"].append(
                f"Consider reducing total_groups from {self.config.total_groups} "
                f"for better daily stock coverage"
            )

        if self.config.group_distribution_method == "round_robin" and self.tse_manager:
            validation["recommendations"].append(
                "Consider using 'sector' or 'mixed' distribution for better sector balance"
            )

        return validation
