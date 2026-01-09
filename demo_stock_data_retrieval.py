#!/usr/bin/env python3
"""
Stock Data Retrieval Demonstration

This script demonstrates the capability to retrieve financial information
for any of the 3,701 available stocks from the TSE integration.

Shows:
- Sample stock selection from different market segments
- Financial data retrieval with complete metrics
- Error handling for invalid/delisted stocks
- Data structure and available fields
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any, List
import yfinance as yf

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

# Import with proper module path handling
try:
    # Try relative imports first
    from src.data_fetcher import DataFetcher
    from src.tse_stock_list_manager import TSEStockListManager
    from src.models import TSEDataConfig
except ImportError:
    # Fallback to direct yfinance for demonstration
    print(
        "Using direct yfinance for demonstration (TSE integration available in full system)"
    )
    DataFetcher = None
    TSEStockListManager = None
    TSEDataConfig = None


def setup_logging():
    """Setup logging for the demonstration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/demo_stock_retrieval.log"),
        ],
    )


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


def get_sample_stocks_by_market(
    tse_manager: TSEStockListManager,
) -> Dict[str, List[str]]:
    """Get sample stocks from different market segments."""
    try:
        # Load TSE data
        df = tse_manager.load_tse_stock_data()
        df = tse_manager.filter_tradable_stocks(df)
        df = tse_manager.exclude_investment_products(df)
        df = tse_manager.filter_target_markets(df)

        samples = {}

        # Get samples from each market category
        market_categories = df["市場・商品区分"].unique()

        for market in market_categories:
            market_stocks = df[df["市場・商品区分"] == market]
            # Get first 3 stocks from each market as samples
            sample_codes = market_stocks["コード"].head(3).astype(str).tolist()
            samples[market] = [f"{code}.T" for code in sample_codes]

        return samples

    except Exception as e:
        logging.error(f"Failed to get sample stocks: {e}")
        # Fallback to known good stocks
        return {
            "Prime Market": ["7203.T", "6758.T", "9984.T"],  # Toyota, Sony, SoftBank
            "Standard Market": ["1332.T", "1605.T", "1801.T"],  # Sample standard stocks
            "Growth Market": [
                "4755.T",
                "3765.T",
                "4689.T",
            ],  # Rakuten, Gung Ho, Z Holdings
        }


def demonstrate_stock_data_retrieval():
    """Main demonstration function."""
    print("=" * 80)
    print("TSE Stock Data Retrieval Demonstration")
    print("=" * 80)
    print()

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Initialize TSE manager and data fetcher
        print("🔧 Initializing TSE Stock List Manager and Data Fetcher...")
        tse_config = TSEDataConfig()
        tse_manager = TSEStockListManager(config=tse_config)
        data_fetcher = DataFetcher(tse_config=tse_config)

        # Get total stock count
        print("📊 Getting total available stock count...")
        all_stocks = tse_manager.get_all_tradable_stocks()
        total_count = len(all_stocks)
        print(f"✅ Total available stocks: {total_count:,}")
        print()

        # Get processing statistics
        print("📈 TSE Data Processing Statistics:")
        stats = tse_manager.get_processing_statistics()
        print(f"  • Total TSE records: {stats.get('total_records', 'N/A'):,}")
        print(f"  • Tradable stocks: {stats.get('tradable_stocks', 'N/A'):,}")
        print(
            f"  • Final stocks (excluding ETFs/REITs): {stats.get('final_stocks', 'N/A'):,}"
        )
        print(
            f"  • Excluded investment products: {stats.get('excluded_investment_products', 'N/A'):,}"
        )
        print()

        # Show market breakdown
        market_breakdown = stats.get("market_category_breakdown", {})
        if market_breakdown:
            print("🏢 Market Category Breakdown:")
            for market, count in market_breakdown.items():
                print(f"  • {market}: {count:,} stocks")
            print()

        # Get sample stocks from different markets
        print("🎯 Selecting sample stocks from different market segments...")
        sample_stocks = get_sample_stocks_by_market(tse_manager)

        # Demonstrate data retrieval for sample stocks
        print("💰 Retrieving financial data for sample stocks:")
        print()

        successful_retrievals = 0
        failed_retrievals = 0

        for market, stocks in sample_stocks.items():
            print(f"📍 {market} Stocks:")
            print("-" * 50)

            for stock_code in stocks:
                try:
                    print(f"🔍 Retrieving data for {stock_code}...")

                    # Get financial information
                    financial_data = data_fetcher.get_financial_info(stock_code)

                    # Get TSE metadata
                    metadata = tse_manager.get_stock_metadata(stock_code)

                    print(f"✅ {stock_code} - Data Retrieved Successfully")
                    print(format_financial_data(financial_data))

                    if metadata:
                        print(
                            f"  TSE Sector (17): {metadata.get('sector_17_name', 'N/A')}"
                        )
                        print(
                            f"  TSE Size Category: {metadata.get('size_category', 'N/A')}"
                        )

                    print()
                    successful_retrievals += 1

                except Exception as e:
                    print(f"❌ {stock_code} - Failed to retrieve data: {str(e)}")
                    print()
                    failed_retrievals += 1

        # Summary
        print("=" * 80)
        print("📊 DEMONSTRATION SUMMARY")
        print("=" * 80)
        print(f"✅ Total available stocks: {total_count:,}")
        print(f"🎯 Sample stocks tested: {successful_retrievals + failed_retrievals}")
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
        print("  • TSE Metadata: Sector Classification, Market Category, Size Category")
        print()

        print("✨ The system can retrieve financial information for any of the")
        print(
            f"   {total_count:,} available stocks using the same method demonstrated above."
        )
        print()

        # Show data structure example
        if successful_retrievals > 0:
            print("📋 Complete Data Structure Example:")
            print("   The financial data dictionary contains the following keys:")
            print("   - symbol, shortName, longName")
            print("   - currentPrice, previousClose, marketCap")
            print("   - trailingPE, forwardPE, priceToBook")
            print(
                "   - dividendYield, trailingAnnualDividendYield, trailingAnnualDividendRate"
            )
            print("   - payoutRatio, totalRevenue, revenueGrowth, earningsGrowth")
            print(
                "   - profitMargins, operatingMargins, returnOnEquity, returnOnAssets"
            )
            print("   - debtToEquity, currency, exchange, sector, industry")
            print()

        logger.info(
            f"Demonstration completed - {successful_retrievals} successful, {failed_retrievals} failed"
        )

    except Exception as e:
        print(f"❌ Demonstration failed: {str(e)}")
        logger.error(f"Demonstration failed: {e}")
        return False

    return True


if __name__ == "__main__":
    success = demonstrate_stock_data_retrieval()
    sys.exit(0 if success else 1)
