#!/usr/bin/env python3
"""
Research external sources for valid TSE stock codes
"""

import requests
import pandas as pd
import json
import time
from typing import List, Dict, Any
import yfinance as yf


def test_jpx_official_source():
    """Test JPX (Japan Exchange Group) official data sources"""
    print("=== Testing JPX Official Sources ===")

    # JPX provides some public data
    jpx_urls = [
        "https://www.jpx.co.jp/english/",
        "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html",
        # Note: These might not be direct API endpoints
    ]

    print("JPX official website exists, but may not have direct API access")
    print("JPX typically provides data through:")
    print("- Market data vendors")
    print("- Official reports (PDF/Excel)")
    print("- Licensed data feeds")


def test_yahoo_finance_indices():
    """Test getting stock lists from major Japanese indices"""
    print("\n=== Testing Yahoo Finance Indices ===")

    # Major Japanese indices
    indices = {
        "^N225": "Nikkei 225",
        "^TOPX": "TOPIX",
        "^MOTHERS": "Mothers Index",
        "^JASDAQ": "JASDAQ Index",
    }

    all_stocks = set()

    for symbol, name in indices.items():
        print(f"\nTesting {name} ({symbol}):")

        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            print(f"  Index info available: {bool(info)}")

            # Check if constituents are available
            if hasattr(ticker, "constituents"):
                constituents = ticker.constituents
                if constituents is not None:
                    print(f"  Constituents found: {len(constituents)}")
                    all_stocks.update(constituents)
                else:
                    print("  No constituents data")
            else:
                print("  No constituents attribute")

        except Exception as e:
            print(f"  Error: {e}")

    print(f"\nTotal unique stocks from indices: {len(all_stocks)}")
    return list(all_stocks)


def test_pandas_datareader():
    """Test pandas-datareader for stock lists"""
    print("\n=== Testing pandas-datareader ===")

    try:
        import pandas_datareader as pdr

        print("pandas-datareader is available")

        # Try to get some Japanese market data
        # Note: This might not work for TSE directly
        print("pandas-datareader supports various sources but TSE access is limited")

    except ImportError:
        print("pandas-datareader not installed")
        print("Install with: pip install pandas-datareader")
    except Exception as e:
        print(f"pandas-datareader error: {e}")


def test_web_scraping_approaches():
    """Test web scraping approaches for stock lists"""
    print("\n=== Testing Web Scraping Approaches ===")

    # Potential sources (be careful about terms of service)
    sources = [
        {
            "name": "Yahoo Finance Japan Stock Screener",
            "url": "https://finance.yahoo.com/screener/",
            "note": "May have Japanese stocks section",
        },
        {
            "name": "Investing.com Japan Stocks",
            "url": "https://www.investing.com/equities/japan",
            "note": "Lists Japanese stocks with codes",
        },
        {
            "name": "MarketWatch Japan",
            "url": "https://www.marketwatch.com/investing/stock/country/japan",
            "note": "May have stock listings",
        },
    ]

    print("Potential web scraping sources:")
    for source in sources:
        print(f"  - {source['name']}: {source['note']}")

    print("\nNote: Web scraping requires:")
    print("- Respect for robots.txt")
    print("- Rate limiting")
    print("- Terms of service compliance")


def test_financial_apis():
    """Test financial APIs that might have Japanese stock data"""
    print("\n=== Testing Financial APIs ===")

    apis = [
        {
            "name": "Alpha Vantage",
            "url": "https://www.alphavantage.co/",
            "note": "Has some international stocks, requires API key",
        },
        {
            "name": "IEX Cloud",
            "url": "https://iexcloud.io/",
            "note": "Primarily US stocks, limited international",
        },
        {
            "name": "Quandl/Nasdaq Data Link",
            "url": "https://data.nasdaq.com/",
            "note": "Has some Japanese market data",
        },
        {
            "name": "Financial Modeling Prep",
            "url": "https://financialmodelingprep.com/",
            "note": "Has international stock lists",
        },
    ]

    print("Financial APIs with potential Japanese stock data:")
    for api in apis:
        print(f"  - {api['name']}: {api['note']}")


def create_static_tse_list():
    """Create a static list of known major TSE stocks"""
    print("\n=== Creating Static TSE Stock List ===")

    # Major TSE stocks by sector (manually curated)
    major_tse_stocks = {
        "Automotive": [
            "7203.T",  # Toyota Motor
            "7267.T",  # Honda Motor
            "7201.T",  # Nissan Motor
            "7269.T",  # Suzuki Motor
            "7270.T",  # Subaru
        ],
        "Technology": [
            "6758.T",  # Sony Group
            "6861.T",  # Keyence
            "6954.T",  # Fanuc
            "8035.T",  # Tokyo Electron
            "6981.T",  # Murata Manufacturing
        ],
        "Telecommunications": [
            "9984.T",  # SoftBank Group
            "9432.T",  # NTT
            "9434.T",  # SoftBank
        ],
        "Gaming/Entertainment": [
            "7974.T",  # Nintendo
            "9766.T",  # Konami
            "7832.T",  # Bandai Namco
        ],
        "Finance": [
            "8306.T",  # Mitsubishi UFJ Financial
            "8316.T",  # Sumitomo Mitsui Financial
            "8411.T",  # Mizuho Financial
        ],
        "Retail": [
            "9983.T",  # Fast Retailing (Uniqlo)
            "3382.T",  # Seven & i Holdings
        ],
        "Pharmaceuticals": [
            "4502.T",  # Takeda Pharmaceutical
            "4568.T",  # Daiichi Sankyo
            "4519.T",  # Chugai Pharmaceutical
        ],
        "Industrial": [
            "6367.T",  # Daikin Industries
            "6326.T",  # Kubota
            "6503.T",  # Mitsubishi Electric
        ],
    }

    # Flatten the list
    all_stocks = []
    for sector, stocks in major_tse_stocks.items():
        all_stocks.extend(stocks)

    print(f"Created static list with {len(all_stocks)} major TSE stocks")

    # Validate these stocks with yfinance
    print("Validating static list with yfinance...")
    valid_stocks = []

    for stock in all_stocks:
        try:
            ticker = yf.Ticker(stock)
            info = ticker.info

            if info and info.get("shortName"):
                valid_stocks.append(stock)
                print(f"  ✓ {stock}: {info.get('shortName')}")
            else:
                print(f"  ✗ {stock}: No valid info")
        except Exception as e:
            print(f"  ✗ {stock}: Error - {e}")

        time.sleep(0.1)  # Rate limiting

    print(f"\nValidated {len(valid_stocks)}/{len(all_stocks)} stocks")

    return valid_stocks, major_tse_stocks


def save_stock_list_to_file(
    stocks: List[str], filename: str = "tse_stocks_static.json"
):
    """Save stock list to JSON file"""
    data = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "source": "manually_curated_major_tse_stocks",
        "count": len(stocks),
        "stocks": stocks,
        "note": "Major TSE stocks validated with yfinance",
    }

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(stocks)} stocks to {filename}")


def suggest_best_approach():
    """Suggest the best approach for getting TSE stock lists"""
    print("\n=== Best Approach Recommendation ===")

    print(
        """
Based on research, here are the recommended approaches:

1. **Static List + Validation** (Recommended for immediate use):
   - Manually curate major TSE stocks (~100-200)
   - Validate with yfinance
   - Update quarterly
   - Pros: Fast, reliable, covers major stocks
   - Cons: May miss smaller stocks

2. **Smart Range Validation** (Our previous approach):
   - Use known TSE code ranges
   - Validate each code with yfinance
   - Cache results
   - Pros: Comprehensive, finds all valid stocks
   - Cons: Takes time for initial setup

3. **Hybrid Approach** (Best of both worlds):
   - Start with static list for immediate use
   - Run smart range validation weekly to expand
   - Combine both lists
   - Pros: Fast startup + comprehensive coverage

4. **External API Integration** (Future enhancement):
   - Use financial data APIs (Alpha Vantage, etc.)
   - Requires API keys and may have costs
   - More reliable and comprehensive

For your immediate needs, I recommend starting with approach #1 (Static List)
and then implementing #3 (Hybrid) for long-term solution.
"""
    )


if __name__ == "__main__":
    print("Researching external sources for TSE stock codes...")

    test_jpx_official_source()
    stocks_from_indices = test_yahoo_finance_indices()
    test_pandas_datareader()
    test_web_scraping_approaches()
    test_financial_apis()

    # Create and validate static list
    valid_stocks, stock_sectors = create_static_tse_list()

    if valid_stocks:
        save_stock_list_to_file(valid_stocks)

    suggest_best_approach()
