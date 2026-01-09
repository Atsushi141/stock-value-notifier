#!/usr/bin/env python3
"""
Simple Stock Data Retrieval Demonstration

This script demonstrates the capability to retrieve financial information
for Japanese stocks, showing the same data structure and metrics that
the full TSE integration system provides.
"""

import yfinance as yf
from typing import Dict, Any, List
from datetime import datetime


def format_financial_data(data: Dict[str, Any]) -> str:
    """Format financial data for display."""
    if not data:
        return "No data available"

    lines = []
    lines.append(f"  Symbol: {data.get('symbol', 'N/A')}")
    lines.append(f"  Company: {data.get('shortName', 'N/A')}")
    lines.append(f"  Full Name: {data.get('longName', 'N/A')}")
    lines.append(
        f"  Current Price: {data.get('currentPrice', 'N/A')} {data.get('currency', 'JPY')}"
    )
    lines.append(f"  Market Cap: {format_large_number(data.get('marketCap'))}")
    lines.append(f"  PER (Trailing): {data.get('trailingPE', 'N/A')}")
    lines.append(f"  PBR: {data.get('priceToBook', 'N/A')}")
    lines.append(f"  Dividend Yield: {format_percentage(data.get('dividendYield'))}")
    lines.append(f"  ROE: {format_percentage(data.get('returnOnEquity'))}")
    lines.append(f"  Sector: {data.get('sector', 'N/A')}")
    lines.append(f"  Industry: {data.get('industry', 'N/A')}")
    lines.append(f"  Exchange: {data.get('exchange', 'N/A')}")

    return "\n".join(lines)


def format_large_number(value) -> str:
    """Format large numbers for display."""
    if value is None:
        return "N/A"

    try:
        num = float(value)
        if num >= 1e12:
            return f"{num/1e12:.2f}T"
        elif num >= 1e9:
            return f"{num/1e9:.2f}B"
        elif num >= 1e6:
            return f"{num/1e6:.2f}M"
        else:
            return f"{num:,.0f}"
    except (ValueError, TypeError):
        return str(value)


def format_percentage(value) -> str:
    """Format percentage values for display."""
    if value is None:
        return "N/A"

    try:
        return f"{float(value)*100:.2f}%"
    except (ValueError, TypeError):
        return str(value)


def get_financial_info(symbol: str) -> Dict[str, Any]:
    """Get financial info using yfinance (same structure as full system)."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        if not info or len(info) == 0:
            raise Exception(f"No data found for {symbol}")

        # Extract relevant financial metrics (same structure as DataFetcher)
        financial_info = {
            "symbol": symbol,
            "shortName": info.get("shortName", ""),
            "longName": info.get("longName", ""),
            "currentPrice": info.get("currentPrice"),
            "previousClose": info.get("previousClose"),
            "marketCap": info.get("marketCap"),
            "trailingPE": info.get("trailingPE"),  # PER
            "forwardPE": info.get("forwardPE"),
            "priceToBook": info.get("priceToBook"),  # PBR
            "dividendYield": info.get("dividendYield"),  # Dividend yield as decimal
            "trailingAnnualDividendYield": info.get("trailingAnnualDividendYield"),
            "trailingAnnualDividendRate": info.get("trailingAnnualDividendRate"),
            "payoutRatio": info.get("payoutRatio"),
            "totalRevenue": info.get("totalRevenue"),
            "revenueGrowth": info.get("revenueGrowth"),
            "earningsGrowth": info.get("earningsGrowth"),
            "profitMargins": info.get("profitMargins"),
            "operatingMargins": info.get("operatingMargins"),
            "returnOnEquity": info.get("returnOnEquity"),
            "returnOnAssets": info.get("returnOnAssets"),
            "debtToEquity": info.get("debtToEquity"),
            "currency": info.get("currency", "JPY"),
            "exchange": info.get("exchange", ""),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
        }

        return financial_info

    except Exception as e:
        raise Exception(f"Failed to get financial info for {symbol}: {str(e)}")


def main():
    """Main demonstration function."""
    print("=" * 80)
    print("Japanese Stock Data Retrieval Demonstration")
    print("=" * 80)
    print()

    print("📊 TSE Integration System Summary:")
    print("  • Total available stocks: 3,701 (excluding ETFs/REITs)")
    print(
        "  • Market segments: Prime (1,586), Standard (1,537), Growth (506), PRO (72)"
    )
    print("  • Data source: Official TSE stock list (data_j.xls)")
    print("  • Investment products excluded: 402 (ETFs, REITs, etc.)")
    print()

    # Sample stocks from different categories
    sample_stocks = {
        "Large Cap (Prime Market)": [
            ("7203.T", "Toyota Motor"),
            ("6758.T", "Sony Group"),
            ("9984.T", "SoftBank Group"),
        ],
        "Technology Stocks": [
            ("4755.T", "Rakuten Group"),
            ("6861.T", "Keyence"),
            ("8035.T", "Tokyo Electron"),
        ],
        "Financial Sector": [
            ("8306.T", "Mitsubishi UFJ Financial"),
            ("8411.T", "Mizuho Financial"),
            ("8316.T", "Sumitomo Mitsui Financial"),
        ],
    }

    print("🎯 Demonstrating data retrieval for sample stocks:")
    print()

    successful_retrievals = 0
    failed_retrievals = 0

    for category, stocks in sample_stocks.items():
        print(f"📍 {category}:")
        print("-" * 60)

        for stock_code, company_name in stocks:
            try:
                print(f"🔍 Retrieving data for {stock_code} ({company_name})...")

                # Get financial information
                financial_data = get_financial_info(stock_code)

                print(f"✅ {stock_code} - Data Retrieved Successfully")
                print(format_financial_data(financial_data))
                print()

                successful_retrievals += 1

            except Exception as e:
                print(f"❌ {stock_code} - Failed: {str(e)}")
                print()
                failed_retrievals += 1

    # Summary
    print("=" * 80)
    print("📊 DEMONSTRATION SUMMARY")
    print("=" * 80)
    print(f"✅ Sample stocks tested: {successful_retrievals + failed_retrievals}")
    print(f"✅ Successful retrievals: {successful_retrievals}")
    print(f"❌ Failed retrievals: {failed_retrievals}")

    if successful_retrievals > 0:
        success_rate = (
            successful_retrievals / (successful_retrievals + failed_retrievals)
        ) * 100
        print(f"📈 Success rate: {success_rate:.1f}%")

    print()
    print("🔍 Available Financial Metrics:")
    print("  • Basic Info: Symbol, Company Name, Current Price")
    print("  • Valuation: Market Cap, PER (P/E Ratio), PBR (P/B Ratio)")
    print("  • Profitability: ROE, ROA, Profit Margins, Operating Margins")
    print("  • Dividends: Dividend Yield, Dividend Rate, Payout Ratio")
    print("  • Growth: Revenue Growth, Earnings Growth")
    print("  • Financial Health: Debt-to-Equity Ratio")
    print("  • Classification: Sector, Industry, Exchange")
    print()

    print("✨ SYSTEM CAPABILITIES:")
    print("  • The full TSE integration system can retrieve financial information")
    print("    for ANY of the 3,701 available stocks using the same method")
    print("  • Automatic filtering excludes ETFs, REITs, and investment products")
    print("  • Comprehensive error handling for delisted/invalid stocks")
    print("  • TSE metadata integration (sector classification, market category)")
    print("  • Caching and performance optimization")
    print("  • Fallback mechanisms for data reliability")
    print()

    print("📋 Complete Data Structure:")
    print("   Each stock returns a dictionary with these keys:")
    print("   - symbol, shortName, longName")
    print("   - currentPrice, previousClose, marketCap")
    print("   - trailingPE, forwardPE, priceToBook")
    print("   - dividendYield, trailingAnnualDividendYield, trailingAnnualDividendRate")
    print("   - payoutRatio, totalRevenue, revenueGrowth, earningsGrowth")
    print("   - profitMargins, operatingMargins, returnOnEquity, returnOnAssets")
    print("   - debtToEquity, currency, exchange, sector, industry")
    print()

    print("🎯 USAGE EXAMPLE:")
    print("   To get data for any stock from the 3,701 available:")
    print("   ```python")
    print("   data_fetcher = DataFetcher()")
    print("   financial_info = data_fetcher.get_financial_info('7203.T')  # Toyota")
    print("   print(f\"PER: {financial_info['trailingPE']}\")")
    print("   print(f\"PBR: {financial_info['priceToBook']}\")")
    print("   print(f\"Dividend Yield: {financial_info['dividendYield']*100:.2f}%\")")
    print("   ```")
    print()


if __name__ == "__main__":
    main()
