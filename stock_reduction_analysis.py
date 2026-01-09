#!/usr/bin/env python3
"""
Stock reduction logic analysis
"""


def analyze_current_logic():
    """Analyze current stock selection logic"""
    print("=== Current Logic Analysis ===")

    # Current logic
    count = 0
    for code in range(1000, 10000):
        if code < 1300:
            if code % 5 == 0:
                count += 1
        else:
            count += 1

    print(f"Current total: {count} stocks")

    # Breakdown
    low_range = sum(1 for code in range(1000, 1300) if code % 5 == 0)
    high_range = sum(1 for code in range(1300, 10000))

    print(f"  Low range (1000-1299): {low_range} stocks")
    print(f"  High range (1300-9999): {high_range} stocks")
    print(f"  Daily average (÷5): {count // 5} stocks")

    return count


def analyze_proposed_logic():
    """Analyze proposed optimized logic"""
    print("\n=== Proposed Logic Analysis ===")

    # Proposed logic
    count = 0
    for code in range(1000, 10000):
        if code < 1300:
            if code % 10 == 0:
                count += 1
        elif code < 5000:
            if code % 2 == 0:
                count += 1
        else:
            if code % 3 == 0:
                count += 1

    print(f"Proposed total: {count} stocks")

    # Breakdown
    low_range = sum(1 for code in range(1000, 1300) if code % 10 == 0)
    mid_range = sum(1 for code in range(1300, 5000) if code % 2 == 0)
    high_range = sum(1 for code in range(5000, 10000) if code % 3 == 0)

    print(f"  Low range (1000-1299): {low_range} stocks (÷10)")
    print(f"  Mid range (1300-4999): {mid_range} stocks (÷2)")
    print(f"  High range (5000-9999): {high_range} stocks (÷3)")
    print(f"  Daily average (÷5): {count // 5} stocks")

    return count


def compare_alternatives():
    """Compare different reduction strategies"""
    print("\n=== Alternative Strategies Comparison ===")

    strategies = [
        ("Every 2nd", lambda c: c % 2 == 0),
        ("Every 3rd", lambda c: c % 3 == 0),
        ("Every 4th", lambda c: c % 4 == 0),
        ("Every 5th", lambda c: c % 5 == 0),
        ("Every 10th", lambda c: c % 10 == 0),
    ]

    for name, logic in strategies:
        count = sum(1 for code in range(1000, 10000) if logic(code))
        daily = count // 5
        print(f"  {name:12}: {count:4} total, {daily:3} daily")


def analyze_distribution_quality():
    """Analyze quality of stock distribution"""
    print("\n=== Distribution Quality Analysis ===")

    # Proposed logic
    selected_stocks = []
    for code in range(1000, 10000):
        if code < 1300:
            if code % 10 == 0:
                selected_stocks.append(code)
        elif code < 5000:
            if code % 2 == 0:
                selected_stocks.append(code)
        else:
            if code % 3 == 0:
                selected_stocks.append(code)

    # Analyze distribution across ranges
    ranges = [
        (1000, 2000, "1000s"),
        (2000, 3000, "2000s"),
        (3000, 4000, "3000s"),
        (4000, 5000, "4000s"),
        (5000, 6000, "5000s"),
        (6000, 7000, "6000s"),
        (7000, 8000, "7000s"),
        (8000, 9000, "8000s"),
        (9000, 10000, "9000s"),
    ]

    print("Distribution by 1000s:")
    for start, end, name in ranges:
        count = sum(1 for stock in selected_stocks if start <= stock < end)
        percentage = (count / len(selected_stocks)) * 100
        print(f"  {name}: {count:3} stocks ({percentage:4.1f}%)")


def calculate_performance_impact():
    """Calculate performance impact of reduction"""
    print("\n=== Performance Impact Analysis ===")

    current_total = 8760
    proposed_total = 3547

    reduction_percentage = ((current_total - proposed_total) / current_total) * 100
    time_savings = reduction_percentage
    api_call_reduction = current_total - proposed_total

    print(f"Total reduction: {reduction_percentage:.1f}%")
    print(f"API calls saved: {api_call_reduction} per day")
    print(f"Estimated time savings: {time_savings:.1f}%")
    print(f"Weekly API call reduction: {api_call_reduction * 5}")


if __name__ == "__main__":
    current = analyze_current_logic()
    proposed = analyze_proposed_logic()

    print(f"\n=== Summary ===")
    print(f"Reduction: {current} → {proposed} stocks")
    print(f"Percentage: {((current - proposed) / current) * 100:.1f}% reduction")
    print(f"Daily: {current // 5} → {proposed // 5} stocks")

    compare_alternatives()
    analyze_distribution_quality()
    calculate_performance_impact()
