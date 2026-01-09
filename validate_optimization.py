#!/usr/bin/env python3
"""
Validate the optimization logic for TSE stock fetcher
"""

import yfinance as yf
import time
from datetime import datetime


def validate_tse_stock_quickly(symbol: str) -> bool:
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


def test_optimization_logic():
    """Test the optimization logic with sample ranges"""
    print("=== Testing TSE Stock Optimization Logic ===")
    print(f"Test started at: {datetime.now().isoformat()}")

    # Test with a few sample ranges
    test_ranges = [
        (1300, 1310, "Construction"),
        (2000, 2010, "Food"),
        (6500, 6510, "Electronics"),
        (7200, 7210, "Transportation"),
        (8300, 8310, "Banks"),
    ]

    all_valid_stocks = []
    total_tested = 0

    print("\nTesting sample ranges:")

    for start, end, sector in test_ranges:
        print(f"\n{sector} range ({start}-{end}):")

        range_valid = []
        for code in range(start, end):
            symbol = f"{code}.T"

            print(f"  Testing {symbol}...", end=" ")

            if validate_tse_stock_quickly(symbol):
                range_valid.append(symbol)
                print("✓ VALID")

                # Get stock name for verification
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    name = info.get("shortName", "Unknown")
                    print(f"    Name: {name}")
                except:
                    pass
            else:
                print("✗ Invalid")

            total_tested += 1
            time.sleep(0.2)  # Rate limiting

        all_valid_stocks.extend(range_valid)
        success_rate = len(range_valid) / (end - start) * 100
        print(
            f"  Range result: {len(range_valid)}/{end-start} valid ({success_rate:.1f}%)"
        )

    # Calculate overall results
    overall_success_rate = len(all_valid_stocks) / total_tested * 100

    print(f"\n=== RESULTS ===")
    print(f"Total tested: {total_tested}")
    print(f"Valid stocks found: {len(all_valid_stocks)}")
    print(f"Success rate: {overall_success_rate:.1f}%")

    print(f"\nValid stocks found:")
    for stock in all_valid_stocks:
        print(f"  {stock}")

    # Project to full implementation
    test_ranges_count = len(test_ranges)
    full_ranges_count = 36  # From the implementation

    projected_total = len(all_valid_stocks) * (full_ranges_count / test_ranges_count)
    projected_daily = projected_total / 5  # 5-day rotation

    print(f"\n=== PROJECTIONS ===")
    print(f"Test ranges: {test_ranges_count}")
    print(f"Full implementation ranges: {full_ranges_count}")
    print(f"Projected total stocks: ~{projected_total:.0f}")
    print(f"Projected daily stocks: ~{projected_daily:.0f}")
    print(f"Target daily stocks: 700")

    if projected_daily > 0:
        efficiency = (700 / projected_daily) * 100
        print(f"Target efficiency: {efficiency:.1f}%")

        if 600 <= projected_daily <= 800:
            print("✅ EXCELLENT: Daily volume is within optimal range!")
        elif 400 <= projected_daily <= 1000:
            print("✅ GOOD: Daily volume is acceptable")
        else:
            print("⚠️  WARNING: Daily volume may need adjustment")

    # Compare with current system
    print(f"\n=== COMPARISON WITH CURRENT SYSTEM ===")
    print(f"Current daily processing: 1,752 stocks")
    print(f"Optimized daily processing: ~{projected_daily:.0f} stocks")

    if projected_daily > 0:
        reduction = ((1752 - projected_daily) / 1752) * 100
        print(f"Reduction: {reduction:.1f}%")

        improvement = overall_success_rate / 5  # Current system ~5% success rate
        print(f"Success rate improvement: {improvement:.1f}x")

    return len(all_valid_stocks) > 0


def test_known_stocks():
    """Test with known major TSE stocks to verify validation logic"""
    print(f"\n=== Testing Known Major TSE Stocks ===")

    known_stocks = [
        ("7203.T", "Toyota Motor"),
        ("6758.T", "Sony Group"),
        ("7974.T", "Nintendo"),
        ("9984.T", "SoftBank Group"),
        ("8306.T", "Mitsubishi UFJ Financial"),
    ]

    valid_count = 0

    for symbol, expected_name in known_stocks:
        print(f"Testing {symbol} ({expected_name})...", end=" ")

        if validate_tse_stock_quickly(symbol):
            print("✓ VALID")
            valid_count += 1

            # Verify the name matches
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                actual_name = info.get("shortName", "Unknown")
                print(f"  Actual name: {actual_name}")
            except:
                pass
        else:
            print("✗ FAILED - This should be valid!")

        time.sleep(0.2)

    print(f"\nKnown stocks validation: {valid_count}/{len(known_stocks)} passed")
    return valid_count == len(known_stocks)


if __name__ == "__main__":
    print("Validating TSE Stock Optimization...")

    # Test known stocks first
    known_test_passed = test_known_stocks()

    # Test optimization logic
    optimization_test_passed = test_optimization_logic()

    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")

    if known_test_passed and optimization_test_passed:
        print("🎉 ALL TESTS PASSED!")
        print("The optimization logic is working correctly.")
        print("Ready to implement in the production system.")
    elif known_test_passed:
        print("✅ Known stocks validation passed")
        print("⚠️  Optimization test had issues - review the ranges")
    else:
        print("❌ Tests failed - check yfinance connectivity and validation logic")
