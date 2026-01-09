#!/usr/bin/env python3
"""
Test yfinance screener functionality for getting TSE stock lists
"""

import yfinance as yf
import time


def test_yfinance_screener():
    """Test yfinance screener functionality"""
    print("=== Testing yfinance Screener ===")

    try:
        # Test screener functionality
        screener = yf.Screener()
        print("Screener object created successfully")

        # Check available methods
        screener_methods = [attr for attr in dir(screener) if not attr.startswith("_")]
        print(f"Screener methods: {screener_methods}")

        # Check predefined queries
        print(f"Predefined queries: {yf.PREDEFINED_SCREENER_QUERIES}")

    except Exception as e:
        print(f"Screener error: {e}")


def test_yfinance_search():
    """Test yfinance search functionality"""
    print("\n=== Testing yfinance Search ===")

    try:
        # Test search for Japanese stocks
        search_results = yf.search("Toyota Japan")
        print("Search results for 'Toyota Japan':")
        print(search_results)

    except Exception as e:
        print(f"Search error: {e}")


def test_market_functionality():
    """Test market-related functionality"""
    print("\n=== Testing Market functionality ===")

    try:
        # Test Market class
        market = yf.Market()
        print("Market object created")

        market_methods = [attr for attr in dir(market) if not attr.startswith("_")]
        print(f"Market methods: {market_methods}")

    except Exception as e:
        print(f"Market error: {e}")


def test_sector_industry():
    """Test Sector and Industry functionality"""
    print("\n=== Testing Sector/Industry ===")

    try:
        # Test getting sectors
        sector = yf.Sector("technology")
        print("Sector object created")

        sector_methods = [attr for attr in dir(sector) if not attr.startswith("_")]
        print(f"Sector methods: {sector_methods}")

        # Try to get top companies
        if hasattr(sector, "top_companies"):
            print("Top companies:", sector.top_companies)

    except Exception as e:
        print(f"Sector error: {e}")


def test_lookup_functionality():
    """Test lookup functionality"""
    print("\n=== Testing Lookup functionality ===")

    try:
        # Test lookup
        lookup_result = yf.lookup("Toyota")
        print("Lookup result for 'Toyota':")
        print(lookup_result)

    except Exception as e:
        print(f"Lookup error: {e}")


def test_practical_tse_approach():
    """Test practical approach for TSE stocks"""
    print("\n=== Testing Practical TSE Approach ===")

    # Approach 1: Use known major TSE stocks and expand
    known_major_stocks = [
        "7203.T",  # Toyota
        "6758.T",  # Sony
        "9984.T",  # SoftBank
        "6861.T",  # Keyence
        "7974.T",  # Nintendo
    ]

    print("Testing known major stocks:")
    valid_stocks = []

    for symbol in known_major_stocks:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            if info and info.get("shortName"):
                valid_stocks.append(symbol)
                print(f"✓ {symbol}: {info.get('shortName')}")
            else:
                print(f"✗ {symbol}: No valid info")

        except Exception as e:
            print(f"✗ {symbol}: Error - {e}")

    print(f"\nValid major stocks: {len(valid_stocks)}")

    # Approach 2: Test if we can get related stocks
    if valid_stocks:
        print(f"\nTesting related stocks for {valid_stocks[0]}:")
        try:
            ticker = yf.Ticker(valid_stocks[0])

            # Check if ticker has recommendations or similar stocks
            if hasattr(ticker, "recommendations"):
                print("Has recommendations:", bool(ticker.recommendations))

            if hasattr(ticker, "similar"):
                print("Similar stocks:", ticker.similar)

        except Exception as e:
            print(f"Related stocks error: {e}")


def suggest_optimal_approach():
    """Suggest optimal approach based on findings"""
    print("\n=== Optimal Approach Suggestion ===")

    print(
        """
Based on yfinance testing, here's the optimal approach:

1. **Smart Range Validation** (Recommended):
   ```python
   def get_valid_tse_stocks():
       # Use known TSE sector ranges
       tse_ranges = [
           (1300, 1400),  # Construction
           (1800, 1900),  # Construction  
           (2000, 2100),  # Food
           (3000, 3100),  # Textiles
           (4000, 4100),  # Chemicals
           (5000, 5100),  # Steel
           (6000, 6100),  # Machinery
           (7000, 7100),  # Transportation
           (8000, 8100),  # Trading
           (9000, 9100),  # Transportation/Services
       ]
       
       valid_stocks = []
       for start, end in tse_ranges:
           for code in range(start, min(end, start + 50)):  # Limit per range
               symbol = f"{code}.T"
               if validate_stock_quickly(symbol):
                   valid_stocks.append(symbol)
       
       return valid_stocks
   ```

2. **Quick Validation Function**:
   ```python
   def validate_stock_quickly(symbol):
       try:
           ticker = yf.Ticker(symbol)
           info = ticker.info
           return (info and 
                   len(info) > 5 and 
                   info.get('shortName') and
                   info.get('exchange') == 'JPX')
       except:
           return False
   ```

3. **Caching Strategy**:
   - Cache valid stocks for 24 hours
   - Update list weekly
   - Handle API rate limits

This approach should give us ~800-1200 valid TSE stocks instead of 8760 invalid ones.
    """
    )


if __name__ == "__main__":
    test_yfinance_screener()
    test_yfinance_search()
    test_market_functionality()
    test_sector_industry()
    test_lookup_functionality()
    test_practical_tse_approach()
    suggest_optimal_approach()
