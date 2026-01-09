#!/usr/bin/env python3
"""
Test script to check rotation functionality
"""

import os
import sys
from datetime import datetime

# Add stock-value-notifier/src to path
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "stock-value-notifier", "src")
)

from data_fetcher import DataFetcher
from rotation_manager import RotationManager


def test_rotation():
    print("=== Testing Rotation Functionality ===")

    # Set environment variable
    os.environ["SCREENING_MODE"] = "rotation"

    # Initialize components
    print("Initializing components...")
    data_fetcher = DataFetcher()
    rotation_manager = RotationManager()

    # Get all stocks
    print("Getting all stocks...")
    all_stocks = data_fetcher.get_japanese_stock_list(mode="all")
    print(f"Total stocks: {len(all_stocks)}")

    # Test rotation for today (Friday)
    print("Testing rotation for today...")
    today_stocks = rotation_manager.get_stocks_for_today(all_stocks)
    print(f"Today stocks (Friday): {len(today_stocks)}")

    # Get rotation info
    rotation_info = rotation_manager.get_group_info()
    print(f"Rotation info: {rotation_info}")

    # Test validation
    print("Validating rotation setup...")
    validation = rotation_manager.validate_rotation_setup(all_stocks)
    print(f"Validation result: {validation}")

    # Test all weekdays
    print("\n=== Testing All Weekdays ===")
    for weekday in range(5):  # Monday to Friday
        test_date = datetime(2026, 1, 6 + weekday)  # Jan 6-10, 2026 (Mon-Fri)
        weekday_stocks = rotation_manager.get_stocks_for_today(all_stocks, test_date)
        weekday_info = rotation_manager.get_group_info(test_date)
        print(
            f"{weekday_info['weekday_jp']} ({test_date.strftime('%Y-%m-%d')}): {len(weekday_stocks)} stocks"
        )


if __name__ == "__main__":
    test_rotation()
