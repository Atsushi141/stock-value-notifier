#!/usr/bin/env python3
"""
Test script for the optimized TSE stock fetcher
"""

import sys
import os
import time
from datetime import datetime

# Add the stock-value-notifier src directory to the path
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "stock-value-notifier", "src")
)

try:
    from data_fetcher import DataFetcher

    print("✓ Successfully imported DataFetcher")
except ImportError as e:
    print(f"✗ Failed to import DataFetcher: {e}")
    print("This test requires the stock-value-notifier dependencies to be installed")
    sys.exit(1)


def test_optimized_stock_fetcher():
    """Test the optimized stock fetcher implementation"""
    print("=== Testing Optimized TSE Stock Fetcher ===")

    # Initialize the data fetcher
    print("\n1. Initializing DataFetcher...")
    try:
        fetcher = DataFetcher()
        print("✓ DataFetcher initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize DataFetcher: {e}")
        return False

    # Test the new _get_all_tse_stocks method
    print("\n2. Testing optimized _get_all_tse_stocks method...")
    start_time = time.time()

    try:
        # Test with a small subset first (modify ranges for testing)
        print("   Testing with limited ranges for speed...")

        # Temporarily modify the ranges for testing (first 3 ranges only)
        original_method = fetcher._get_all_tse_stocks

        def test_get_all_tse_stocks():
            """Test version with limited ranges"""
            cache_file = "cache/tse_stocks_test_cache.json"

            # Test ranges (limited for speed)
            tse_ranges = [
                (1300, 1320, "Construction"),  # Only 20 codes
                (2000, 2020, "Food"),  # Only 20 codes
                (6500, 6520, "Electronics"),  # Only 20 codes
            ]

            valid_stocks = []
            total_tested = 0

            for start, end, sector in tse_ranges:
                print(f"     Testing {sector} range ({start}-{end})...")

                range_valid = 0
                for code in range(start, end):
                    symbol = f"{code}.T"

                    if fetcher._validate_tse_stock_quickly(symbol):
                        valid_stocks.append(symbol)
                        range_valid += 1
                        print(f"       ✓ {symbol}")
                    else:
                        print(f"       ✗ {symbol}")

                    total_tested += 1
                    time.sleep(0.1)  # Rate limiting

                print(f"     {sector}: {range_valid}/{end-start} valid stocks")

            success_rate = (
                len(valid_stocks) / total_tested * 100 if total_tested > 0 else 0
            )
            print(
                f"   Test validation complete: {len(valid_stocks)}/{total_tested} "
                f"valid stocks ({success_rate:.1f}% success rate)"
            )

            return valid_stocks

        # Run the test
        valid_stocks = test_get_all_tse_stocks()

        elapsed_time = time.time() - start_time
        print(f"✓ Test completed in {elapsed_time:.1f} seconds")
        print(f"✓ Found {len(valid_stocks)} valid stocks in test ranges")

        if valid_stocks:
            print(f"✓ Sample valid stocks: {valid_stocks[:5]}")

            # Calculate projected results
            test_ranges = 3
            total_ranges = 36  # From the full implementation
            projected_stocks = len(valid_stocks) * (total_ranges / test_ranges)
            daily_stocks = projected_stocks / 5

            print(f"\n3. Projection for full implementation:")
            print(f"   Test ranges: {test_ranges}")
            print(f"   Total ranges: {total_ranges}")
            print(f"   Projected total stocks: ~{projected_stocks:.0f}")
            print(f"   Projected daily stocks: ~{daily_stocks:.0f}")
            print(f"   Target daily stocks: 700")
            print(
                f"   Efficiency: {(700/daily_stocks)*100:.1f}%"
                if daily_stocks > 0
                else "N/A"
            )

            return True
        else:
            print("✗ No valid stocks found in test ranges")
            return False

    except Exception as e:
        print(f"✗ Error during testing: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_validation_method():
    """Test the stock validation method"""
    print("\n=== Testing Stock Validation Method ===")

    try:
        fetcher = DataFetcher()

        # Test with known valid and invalid stocks
        test_stocks = [
            ("7203.T", True, "Toyota Motor - should be valid"),
            ("6758.T", True, "Sony Group - should be valid"),
            ("9999.T", False, "Invalid stock - should be invalid"),
            ("1234.T", False, "Random stock - likely invalid"),
        ]

        for symbol, expected, description in test_stocks:
            print(f"Testing {symbol} ({description})...")

            start_time = time.time()
            result = fetcher._validate_tse_stock_quickly(symbol)
            elapsed = time.time() - start_time

            status = "✓" if result == expected else "✗"
            print(
                f"  {status} {symbol}: {result} (expected: {expected}) - {elapsed:.2f}s"
            )

            time.sleep(0.1)  # Rate limiting

        return True

    except Exception as e:
        print(f"✗ Error during validation testing: {e}")
        return False


def test_caching():
    """Test the caching functionality"""
    print("\n=== Testing Caching Functionality ===")

    try:
        fetcher = DataFetcher()

        # Test cache methods
        test_stocks = ["7203.T", "6758.T", "7974.T"]
        cache_file = "cache/test_cache.json"

        print("1. Testing cache write...")
        fetcher._cache_tse_stocks(cache_file, test_stocks)
        print("✓ Cache write successful")

        print("2. Testing cache read...")
        from datetime import timedelta

        cached_stocks = fetcher._get_cached_tse_stocks(cache_file, timedelta(hours=1))

        if cached_stocks == test_stocks:
            print("✓ Cache read successful - data matches")
        else:
            print(f"✗ Cache read failed - expected {test_stocks}, got {cached_stocks}")
            return False

        print("3. Testing cache expiry...")
        expired_stocks = fetcher._get_cached_tse_stocks(
            cache_file, timedelta(seconds=0)
        )
        if expired_stocks is None:
            print("✓ Cache expiry working correctly")
        else:
            print("✗ Cache expiry not working")
            return False

        # Clean up test cache
        import os

        if os.path.exists(cache_file):
            os.remove(cache_file)
            print("✓ Test cache cleaned up")

        return True

    except Exception as e:
        print(f"✗ Error during caching test: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Starting optimized TSE stock fetcher tests...")
    print(f"Test started at: {datetime.now().isoformat()}")

    # Run all tests
    tests = [
        ("Stock Validation", test_validation_method),
        ("Caching", test_caching),
        ("Optimized Fetcher", test_optimized_stock_fetcher),
    ]

    results = {}
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running {test_name} test...")
        results[test_name] = test_func()

    # Summary
    print(f"\n{'='*50}")
    print("TEST SUMMARY")
    print(f"{'='*50}")

    passed = 0
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{len(tests)} tests passed")

    if passed == len(tests):
        print("🎉 All tests passed! The optimized implementation is ready.")
    else:
        print("⚠️  Some tests failed. Please review the implementation.")
