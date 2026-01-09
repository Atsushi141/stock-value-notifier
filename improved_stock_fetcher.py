#!/usr/bin/env python3
"""
Improved stock fetcher with smart range validation
"""

import yfinance as yf
import time
from typing import List, Dict, Any
from datetime import datetime, timedelta
import json
import os


class ImprovedTSEStockFetcher:
    """
    Improved TSE stock fetcher using smart range validation
    """

    def __init__(self):
        self.cache_file = "tse_stocks_cache.json"
        self.cache_duration = timedelta(hours=24)

        # Known TSE sector ranges with higher validity rates
        self.tse_sector_ranges = [
            (1300, 1400, "Construction"),
            (1800, 1900, "Construction"),
            (2000, 2100, "Food"),
            (2500, 2600, "Food & Beverages"),
            (2800, 2900, "Food"),
            (3000, 3100, "Textiles"),
            (3400, 3500, "Chemicals"),
            (3600, 3700, "Steel"),
            (3800, 3900, "Machinery"),
            (4000, 4100, "Chemicals"),
            (4500, 4600, "Pharmaceuticals"),
            (4900, 5000, "Chemicals"),
            (5000, 5100, "Steel"),
            (5400, 5500, "Steel"),
            (5700, 5800, "Glass & Ceramics"),
            (6000, 6100, "Machinery"),
            (6300, 6400, "Machinery"),
            (6500, 6600, "Electronics"),
            (6700, 6800, "Electronics"),
            (6900, 7000, "Electronics"),
            (7000, 7100, "Transportation Equipment"),
            (7200, 7300, "Transportation Equipment"),
            (7500, 7600, "Precision Instruments"),
            (7700, 7800, "Precision Instruments"),
            (7900, 8000, "Other Products"),
            (8000, 8100, "Trading Companies"),
            (8300, 8400, "Banks"),
            (8500, 8600, "Other Financing"),
            (8700, 8800, "Insurance"),
            (8800, 8900, "Real Estate"),
            (9000, 9100, "Transportation"),
            (9200, 9300, "Transportation"),
            (9400, 9500, "Information & Communication"),
            (9500, 9600, "Electric Power & Gas"),
            (9700, 9800, "Services"),
            (9900, 10000, "Services"),
        ]

    def validate_stock_quickly(self, symbol: str) -> bool:
        """
        Quick validation of stock symbol using yfinance

        Args:
            symbol: Stock symbol (e.g., "7203.T")

        Returns:
            bool: True if stock is valid and active
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # Validation criteria
            return (
                info
                and len(info) > 5  # Has substantial info
                and info.get("shortName")  # Has a name
                and info.get("exchange") == "JPX"  # Is on Japanese exchange
                and info.get("symbol")  # Has symbol info
            )
        except Exception:
            return False

    def get_valid_tse_stocks_smart_ranges(self, max_per_range: int = 30) -> List[str]:
        """
        Get valid TSE stocks using smart range validation

        Args:
            max_per_range: Maximum stocks to validate per range (to control API calls)

        Returns:
            List of valid TSE stock symbols
        """
        print("Getting valid TSE stocks using smart range validation...")

        valid_stocks = []
        total_tested = 0
        total_valid = 0

        for start, end, sector in self.tse_sector_ranges:
            print(f"Testing {sector} range ({start}-{end})...")

            range_valid = []
            range_tested = 0

            # Test stocks in this range
            for code in range(start, min(end, start + max_per_range)):
                symbol = f"{code}.T"

                if self.validate_stock_quickly(symbol):
                    range_valid.append(symbol)
                    print(f"  ✓ {symbol}")
                else:
                    print(f"  ✗ {symbol}")

                range_tested += 1
                total_tested += 1

                # Rate limiting
                time.sleep(0.1)

            valid_stocks.extend(range_valid)
            total_valid += len(range_valid)

            print(f"  Range result: {len(range_valid)}/{range_tested} valid")

        print(f"\nTotal result: {total_valid}/{total_tested} valid stocks")
        print(f"Success rate: {(total_valid/total_tested)*100:.1f}%")

        return valid_stocks

    def get_cached_stocks(self) -> List[str]:
        """Get stocks from cache if available and fresh"""
        if not os.path.exists(self.cache_file):
            return []

        try:
            with open(self.cache_file, "r") as f:
                cache_data = json.load(f)

            cache_time = datetime.fromisoformat(cache_data["timestamp"])
            if datetime.now() - cache_time < self.cache_duration:
                print(f"Using cached stocks: {len(cache_data['stocks'])} stocks")
                return cache_data["stocks"]
        except Exception as e:
            print(f"Cache read error: {e}")

        return []

    def save_stocks_to_cache(self, stocks: List[str]) -> None:
        """Save stocks to cache"""
        try:
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "stocks": stocks,
                "count": len(stocks),
            }

            with open(self.cache_file, "w") as f:
                json.dump(cache_data, f, indent=2)

            print(f"Cached {len(stocks)} stocks")
        except Exception as e:
            print(f"Cache write error: {e}")

    def get_valid_tse_stocks(self, force_refresh: bool = False) -> List[str]:
        """
        Get valid TSE stocks with caching

        Args:
            force_refresh: Force refresh even if cache is valid

        Returns:
            List of valid TSE stock symbols
        """
        if not force_refresh:
            cached_stocks = self.get_cached_stocks()
            if cached_stocks:
                return cached_stocks

        print("Fetching fresh TSE stock list...")
        valid_stocks = self.get_valid_tse_stocks_smart_ranges()

        if valid_stocks:
            self.save_stocks_to_cache(valid_stocks)

        return valid_stocks

    def analyze_results(self, stocks: List[str]) -> Dict[str, Any]:
        """Analyze the results"""
        if not stocks:
            return {"error": "No stocks found"}

        # Group by sector based on code ranges
        sector_distribution = {}
        for symbol in stocks:
            code = int(symbol.replace(".T", ""))

            # Find which sector this belongs to
            sector = "Unknown"
            for start, end, sector_name in self.tse_sector_ranges:
                if start <= code < end:
                    sector = sector_name
                    break

            if sector not in sector_distribution:
                sector_distribution[sector] = 0
            sector_distribution[sector] += 1

        # Calculate rotation metrics
        total_stocks = len(stocks)
        daily_stocks = total_stocks // 5  # 5-day rotation

        return {
            "total_stocks": total_stocks,
            "daily_stocks": daily_stocks,
            "sector_distribution": sector_distribution,
            "rotation_days": 5,
            "target_daily": 700,
            "efficiency": (
                f"{(700/daily_stocks)*100:.1f}%" if daily_stocks > 0 else "N/A"
            ),
        }


def test_improved_fetcher():
    """Test the improved fetcher"""
    print("=== Testing Improved TSE Stock Fetcher ===")

    fetcher = ImprovedTSEStockFetcher()

    # Test with small ranges first
    print("\n1. Testing with limited ranges (fast test):")
    fetcher.tse_sector_ranges = fetcher.tse_sector_ranges[:3]  # Only first 3 ranges

    stocks = fetcher.get_valid_tse_stocks(force_refresh=True)

    print(f"\nFound {len(stocks)} valid stocks:")
    for stock in stocks[:10]:  # Show first 10
        print(f"  {stock}")

    if len(stocks) > 10:
        print(f"  ... and {len(stocks) - 10} more")

    # Analyze results
    analysis = fetcher.analyze_results(stocks)
    print(f"\nAnalysis:")
    print(f"  Total stocks: {analysis['total_stocks']}")
    print(f"  Daily stocks (÷5): {analysis['daily_stocks']}")
    print(f"  Target efficiency: {analysis['efficiency']}")

    print(f"\nSector distribution:")
    for sector, count in analysis["sector_distribution"].items():
        print(f"  {sector}: {count}")


if __name__ == "__main__":
    test_improved_fetcher()
