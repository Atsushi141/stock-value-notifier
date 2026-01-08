"""
Property-based tests for rotation functionality.
Tests universal properties that should hold for all inputs.
"""

import pytest
from hypothesis import given, strategies as st, assume
from datetime import datetime, date, timedelta
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from rotation_manager import RotationManager
from models import RotationConfig


class TestRotationProperties:
    """Property-based tests for rotation functionality."""

    @given(
        stock_count=st.integers(min_value=1, max_value=10000),
        total_groups=st.integers(min_value=1, max_value=10),
    )
    def test_property_9_group_distribution_evenness(self, stock_count, total_groups):
        """
        **Feature: stock-value-notifier, Property 9: 銘柄グループ分割の均等性**

        For any list of stocks and any number of groups, when split into rotation groups,
        the difference between the largest and smallest group should be at most 1.

        **Validates: Requirements 7.3, 7.7**
        """
        # Generate test stocks
        test_stocks = [f"stock_{i:04d}.T" for i in range(stock_count)]

        # Create rotation manager
        rm = RotationManager(total_groups=total_groups)

        # Split stocks into groups
        groups = rm.split_stocks_into_groups(test_stocks)

        # Verify groups structure
        assert (
            len(groups) == total_groups
        ), f"Expected {total_groups} groups, got {len(groups)}"

        # Calculate group sizes
        group_sizes = [len(stocks) for stocks in groups.values()]

        # Verify all stocks are distributed
        total_distributed = sum(group_sizes)
        assert (
            total_distributed == stock_count
        ), f"Lost stocks: expected {stock_count}, got {total_distributed}"

        # Verify no duplicates across groups
        all_distributed_stocks = []
        for stocks in groups.values():
            all_distributed_stocks.extend(stocks)
        assert (
            len(set(all_distributed_stocks)) == stock_count
        ), "Duplicate stocks found across groups"

        # **Core Property: Even distribution (max difference ≤ 1)**
        max_size = max(group_sizes)
        min_size = min(group_sizes)
        size_difference = max_size - min_size

        assert size_difference <= 1, (
            f"Uneven distribution: group sizes {group_sizes}, "
            f"difference {size_difference} > 1"
        )

        # Verify expected distribution
        expected_min = stock_count // total_groups
        expected_max = expected_min + (1 if stock_count % total_groups > 0 else 0)

        assert (
            min_size == expected_min
        ), f"Min size {min_size} != expected {expected_min}"
        assert (
            max_size == expected_max
        ), f"Max size {max_size} != expected {expected_max}"

    def test_property_9_real_world_scenario(self):
        """
        **Feature: stock-value-notifier, Property 9: 銘柄グループ分割の均等性**

        Test with realistic Japanese stock market scenario (approximately 3800 stocks, 5 groups).

        **Validates: Requirements 7.3, 7.7**
        """
        # Simulate realistic stock count (TSE has ~3800 listed companies)
        stock_count = 3800
        total_groups = 5

        test_stocks = [f"{i:04d}.T" for i in range(1000, 1000 + stock_count)]

        rm = RotationManager(total_groups=total_groups)
        groups = rm.split_stocks_into_groups(test_stocks)

        # Verify structure
        assert len(groups) == 5

        # Calculate group sizes
        group_sizes = [len(stocks) for stocks in groups.values()]

        # Verify even distribution
        max_size = max(group_sizes)
        min_size = min(group_sizes)
        assert max_size - min_size <= 1

        # Verify expected sizes (3800 / 5 = 760 each)
        assert min_size == 760
        assert max_size == 760

        # Verify all stocks distributed
        total_distributed = sum(group_sizes)
        assert total_distributed == stock_count

        # Verify no duplicates
        all_stocks = []
        for stocks in groups.values():
            all_stocks.extend(stocks)
        assert len(set(all_stocks)) == stock_count


class TestWeekdayGroupSelectionProperties:
    """Property-based tests for weekday-based group selection."""

    @given(
        year=st.integers(min_value=2020, max_value=2030),
        month=st.integers(min_value=1, max_value=12),
        day=st.integers(min_value=1, max_value=28),  # Safe range for all months
        total_groups=st.integers(min_value=1, max_value=10),
    )
    def test_property_10_weekday_group_consistency(
        self, year, month, day, total_groups
    ):
        """
        **Feature: stock-value-notifier, Property 10: 曜日に基づくグループ選択の一貫性**

        For any date with the same weekday, the same group index should always be selected.

        **Validates: Requirements 7.4**
        """
        try:
            test_date = datetime(year, month, day)
        except ValueError:
            # Skip invalid dates
            assume(False)

        rm = RotationManager(total_groups=total_groups)

        # Get group index for this date
        group_index1 = rm.get_current_group_index(test_date)

        # Test the same date multiple times - should be consistent
        group_index2 = rm.get_current_group_index(test_date)
        assert group_index1 == group_index2, "Same date should return same group index"

        # Verify group index is within valid range
        assert (
            0 <= group_index1 < total_groups
        ), f"Group index {group_index1} out of range [0, {total_groups})"

        # For weekdays (Monday-Friday), group index should match weekday modulo total_groups
        weekday = test_date.weekday()  # Monday=0, Sunday=6

        if weekday < 5:  # Monday to Friday
            expected_group = weekday % total_groups
            assert (
                group_index1 == expected_group
            ), f"Weekday {weekday} should map to group {expected_group}, got {group_index1}"
        else:  # Weekend
            # Should default to Monday group (0)
            assert (
                group_index1 == 0
            ), f"Weekend should default to group 0, got {group_index1}"

    def test_property_10_weekend_handling(self):
        """
        **Feature: stock-value-notifier, Property 10: 曜日に基づくグループ選択の一貫性**

        Weekend dates should consistently map to the Monday group (group 0).

        **Validates: Requirements 7.4**
        """
        rm = RotationManager(total_groups=5)

        # Test various weekend dates
        weekend_dates = [
            datetime(2024, 1, 6),  # Saturday
            datetime(2024, 1, 7),  # Sunday
            datetime(2024, 6, 15),  # Saturday
            datetime(2024, 6, 16),  # Sunday
            datetime(2025, 12, 13),  # Saturday
            datetime(2025, 12, 14),  # Sunday
        ]

        for weekend_date in weekend_dates:
            group_index = rm.get_current_group_index(weekend_date)
            assert group_index == 0, (
                f"Weekend date {weekend_date.strftime('%Y-%m-%d %A')} should map to group 0, "
                f"got {group_index}"
            )

    @given(total_groups=st.integers(min_value=1, max_value=20))
    def test_property_10_all_weekdays_coverage(self, total_groups):
        """
        **Feature: stock-value-notifier, Property 10: 曜日に基づくグループ選択の一貫性**

        For any number of groups, all weekdays should map to valid group indices.

        **Validates: Requirements 7.4**
        """
        rm = RotationManager(total_groups=total_groups)

        # Test all 7 weekdays
        test_dates = [
            datetime(2024, 1, 1),  # Monday
            datetime(2024, 1, 2),  # Tuesday
            datetime(2024, 1, 3),  # Wednesday
            datetime(2024, 1, 4),  # Thursday
            datetime(2024, 1, 5),  # Friday
            datetime(2024, 1, 6),  # Saturday
            datetime(2024, 1, 7),  # Sunday
        ]

        for i, test_date in enumerate(test_dates):
            weekday = test_date.weekday()
            group_index = rm.get_current_group_index(test_date)

            # Verify group index is valid
            assert (
                0 <= group_index < total_groups
            ), f"Weekday {weekday} mapped to invalid group {group_index}"

            # Verify mapping logic
            if weekday < 5:  # Monday to Friday
                expected_group = weekday % total_groups
                assert (
                    group_index == expected_group
                ), f"Weekday {weekday} should map to group {expected_group}, got {group_index}"
            else:  # Weekend
                assert (
                    group_index == 0
                ), f"Weekend (weekday {weekday}) should map to group 0, got {group_index}"

    def test_property_10_real_world_business_days(self):
        """
        **Feature: stock-value-notifier, Property 10: 曜日に基づくグループ選択の一貫性**

        Test with real-world business day scenarios for Japanese stock market.

        **Validates: Requirements 7.4**
        """
        rm = RotationManager(total_groups=5)

        # Test a full business week
        business_week = [
            (datetime(2024, 1, 8), 0),  # Monday -> Group 0
            (datetime(2024, 1, 9), 1),  # Tuesday -> Group 1
            (datetime(2024, 1, 10), 2),  # Wednesday -> Group 2
            (datetime(2024, 1, 11), 3),  # Thursday -> Group 3
            (datetime(2024, 1, 12), 4),  # Friday -> Group 4
        ]

        for test_date, expected_group in business_week:
            group_index = rm.get_current_group_index(test_date)
            assert group_index == expected_group, (
                f"{test_date.strftime('%A')} should map to group {expected_group}, "
                f"got {group_index}"
            )

        # Test that the pattern repeats the following week
        next_week = [
            (datetime(2024, 1, 15), 0),  # Monday -> Group 0
            (datetime(2024, 1, 16), 1),  # Tuesday -> Group 1
            (datetime(2024, 1, 17), 2),  # Wednesday -> Group 2
            (datetime(2024, 1, 18), 3),  # Thursday -> Group 3
            (datetime(2024, 1, 19), 4),  # Friday -> Group 4
        ]

        for test_date, expected_group in next_week:
            group_index = rm.get_current_group_index(test_date)
            assert group_index == expected_group, (
                f"Next week {test_date.strftime('%A')} should map to group {expected_group}, "
                f"got {group_index}"
            )

    @given(total_groups=st.integers(min_value=1, max_value=7))
    def test_property_10_modulo_behavior(self, total_groups):
        """
        **Feature: stock-value-notifier, Property 10: 曜日に基づくグループ選択の一貫性**

        For any number of groups, weekday mapping should follow modulo arithmetic.

        **Validates: Requirements 7.4**
        """
        rm = RotationManager(total_groups=total_groups)

        # Test multiple weeks to verify modulo behavior
        base_monday = datetime(2024, 1, 8)  # A Monday

        for week in range(3):  # Test 3 weeks
            for day in range(5):  # Monday to Friday
                test_date = base_monday + timedelta(days=week * 7 + day)
                weekday = test_date.weekday()
                group_index = rm.get_current_group_index(test_date)

                expected_group = weekday % total_groups
                assert group_index == expected_group, (
                    f"Week {week}, day {day} (weekday {weekday}) should map to "
                    f"group {expected_group}, got {group_index}"
                )
