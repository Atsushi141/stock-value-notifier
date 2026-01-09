#!/usr/bin/env python3
"""
Test yfinance methods for getting valid stock lists
"""

import yfinance as yf
import pandas as pd
import time


def test_yfinance_ticker_methods():
    """Test various yfinance methods to get stock lists"""
    print("=== Testing yfinance methods for stock lists ===")

    # Test 1: Check if yfinance has built-in methods
    print("\n1. Checking yfinance built-in methods:")
    ticker_methods = [attr for attr in dir(yf) if not attr.startswith("_")]
    print(f"Available yfinance methods: {ticker_methods}")

    # Test 2: Try to get market data or indices
    print("\n2. Testing market indices:")
    try:
        # Try Nikkei 225 or TOPIX
        nikkei = yf.Ticker("^N225")  # Nikkei 225
        topix = yf.Ticker("^TOPX")  # TOPIX

        print("Nikkei 225 info available:", bool(nikkei.info))
        print("TOPIX info available:", bool(topix.info))

        # Check if they have constituent information
        if hasattr(nikkei, "constituents"):
            print("Nikkei constituents:", nikkei.constituents)
        if hasattr(topix, "constituents"):
            print("TOPIX constituents:", topix.constituents)

    except Exception as e:
        print(f"Error accessing indices: {e}")


def test_ticker_validation_methods():
    """Test different ways to validate if a ticker exists"""
    print("\n=== Testing ticker validation methods ===")

    test_symbols = [
        "7203.T",  # Toyota (should exist)
        "9999.T",  # Likely doesn't exist
        "1234.T",  # Likely doesn't exist
        "6758.T",  # Sony (should exist)
    ]

    for symbol in test_symbols:
        print(f"\nTesting {symbol}:")

        try:
            ticker = yf.Ticker(symbol)

            # Method 1: Check info
            info = ticker.info
            has_info = bool(info and len(info) > 1)
            print(f"  Has info: {has_info}")

            if has_info:
                print(f"  Name: {info.get('shortName', 'N/A')}")
                print(f"  Symbol: {info.get('symbol', 'N/A')}")
                print(f"  Exchange: {info.get('exchange', 'N/A')}")

            # Method 2: Check history
            try:
                hist = ticker.history(period="5d")
                has_history = not hist.empty
                print(f"  Has recent history: {has_history}")
            except:
                print(f"  Has recent history: False")

            # Method 3: Check if it's a valid ticker
            is_valid = has_info and info.get("symbol") == symbol.replace(".T", "")
            print(f"  Is valid: {is_valid}")

        except Exception as e:
            print(f"  Error: {e}")

        time.sleep(0.5)  # Rate limiting


def test_bulk_validation_approach():
    """Test bulk validation approach"""
    print("\n=== Testing bulk validation approach ===")

    # Test a small range to see the pattern
    test_range = range(7200, 7210)  # Small range for testing
    valid_stocks = []

    print(f"Testing range {min(test_range)}-{max(test_range)}:")

    for code in test_range:
        symbol = f"{code}.T"
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # Quick validation criteria
            if (
                info
                and len(info) > 5  # Has substantial info
                and info.get("symbol")
                and (info.get("shortName") or info.get("longName"))
            ):

                valid_stocks.append(symbol)
                print(f"  ✓ {symbol}: {info.get('shortName', 'N/A')}")
            else:
                print(f"  ✗ {symbol}: Invalid or insufficient data")

        except Exception as e:
            print(f"  ✗ {symbol}: Error - {e}")

        time.sleep(0.2)  # Rate limiting

    print(f"\nValid stocks found: {len(valid_stocks)}/{len(list(test_range))}")
    return valid_stocks


def test_alternative_data_sources():
    """Test alternative ways to get TSE stock lists"""
    print("\n=== Testing alternative data sources ===")

    # Test 1: Check if yfinance can access market screeners
    print("1. Testing market screener access:")
    try:
        # This might not work, but worth trying
        screener = yf.Screener()
        print("Screener available:", bool(screener))
    except Exception as e:
        print(f"Screener not available: {e}")

    # Test 2: Check pandas_datareader integration
    print("\n2. Testing pandas integration:")
    try:
        import pandas_datareader as pdr

        print("pandas_datareader available: True")

        # Try to get some market data
        # This is just a test, might not work for TSE

    except ImportError:
        print("pandas_datareader not available")
    except Exception as e:
        print(f"pandas_datareader error: {e}")


def suggest_practical_approach():
    """Suggest practical approaches based on findings"""
    print("\n=== Practical Approach Suggestions ===")

    print(
        """
Based on yfinance limitations, here are practical approaches:

1. **Validation-based approach** (Recommended):
   - Use known TSE code ranges
   - Validate each code with yfinance
   - Cache results for 24 hours
   - Pros: Accurate, up-to-date
   - Cons: Initial setup time

2. **Static list approach**:
   - Maintain a curated list of known TSE stocks
   - Update periodically (monthly/quarterly)
   - Pros: Fast, reliable
   - Cons: May miss new listings

3. **Hybrid approach**:
   - Start with static list
   - Validate and expand with yfinance
   - Best of both worlds

4. **External data source**:
   - Use TSE official data or financial APIs
   - More reliable but requires additional setup
   """
    )


if __name__ == "__main__":
    test_yfinance_ticker_methods()
    test_ticker_validation_methods()

    # Only run bulk test if user confirms (it takes time)
    response = input("\nRun bulk validation test? (y/n): ")
    if response.lower() == "y":
        test_bulk_validation_approach()

    test_alternative_data_sources()
    suggest_practical_approach()
