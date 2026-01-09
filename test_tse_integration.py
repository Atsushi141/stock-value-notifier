#!/usr/bin/env python3
"""
TSE Integration Test Script

This script performs comprehensive testing of the TSE integration functionality
as specified in task 16.1 "TSE統合の動作確認".

Test Coverage:
- 実際のdata_j.xlsファイルでの動作テスト
- ETF除外の動作確認（約370銘柄が除外されることを確認）
- 通常株式の抽出確認（約4,000銘柄が対象となることを確認）
- フォールバック機能の動作確認

Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src.tse_stock_list_manager import TSEStockListManager
from src.data_fetcher import DataFetcher
from src.models import TSEDataConfig


class TSEIntegrationTester:
    """Comprehensive TSE integration tester."""

    def __init__(self):
        """Initialize the tester."""
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        self.results = {}

        # Initialize TSE manager
        self.tse_config = TSEDataConfig(
            data_file_path="stock_list/data_j.xls", fallback_to_range_validation=True
        )
        self.tse_manager = TSEStockListManager(
            config=self.tse_config, logger=self.logger
        )

        # Initialize data fetcher
        self.data_fetcher = DataFetcher(tse_config=self.tse_config)

    def setup_logging(self):
        """Setup logging for the test."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler("logs/tse_integration_test.log"),
            ],
        )

    def test_data_file_existence(self) -> bool:
        """Test if the TSE data file exists."""
        self.logger.info("=" * 60)
        self.logger.info("TEST 1: TSE Data File Existence")
        self.logger.info("=" * 60)

        file_path = Path(self.tse_config.data_file_path)
        exists = file_path.exists()

        if exists:
            file_size = file_path.stat().st_size
            self.logger.info(f"✅ TSE data file found: {file_path}")
            self.logger.info(
                f"   File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)"
            )

            # Check file modification time
            mod_time = file_path.stat().st_mtime
            import datetime

            mod_datetime = datetime.datetime.fromtimestamp(mod_time)
            self.logger.info(f"   Last modified: {mod_datetime}")
        else:
            self.logger.error(f"❌ TSE data file not found: {file_path}")

        self.results["data_file_exists"] = exists
        return exists

    def test_tse_data_loading(self) -> Tuple[bool, Dict[str, Any]]:
        """Test TSE data loading functionality."""
        self.logger.info("=" * 60)
        self.logger.info("TEST 2: TSE Data Loading")
        self.logger.info("=" * 60)

        try:
            start_time = time.time()
            df = self.tse_manager.load_tse_stock_data()
            load_time = time.time() - start_time

            self.logger.info(f"✅ TSE data loaded successfully")
            self.logger.info(f"   Load time: {load_time:.2f} seconds")
            self.logger.info(f"   Total records: {len(df):,}")
            self.logger.info(f"   Columns: {list(df.columns)}")

            # Check required columns
            required_columns = [
                "日付",
                "コード",
                "銘柄名",
                "市場・商品区分",
                "33業種コード",
                "33業種区分",
                "17業種コード",
                "17業種区分",
                "規模コード",
                "規模区分",
            ]

            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"❌ Missing required columns: {missing_columns}")
                return False, {}
            else:
                self.logger.info("✅ All required columns present")

            # Sample data inspection
            self.logger.info("Sample records:")
            for i, row in df.head(3).iterrows():
                self.logger.info(
                    f"   {row['コード']}: {row['銘柄名']} ({row['市場・商品区分']})"
                )

            stats = {
                "total_records": len(df),
                "load_time": load_time,
                "columns": list(df.columns),
                "missing_columns": missing_columns,
            }

            self.results["data_loading"] = {"success": True, "stats": stats}

            return True, stats

        except Exception as e:
            self.logger.error(f"❌ TSE data loading failed: {e}")
            self.results["data_loading"] = {"success": False, "error": str(e)}
            return False, {}

    def test_etf_exclusion(self) -> Tuple[bool, Dict[str, Any]]:
        """Test ETF and investment product exclusion."""
        self.logger.info("=" * 60)
        self.logger.info("TEST 3: ETF and Investment Product Exclusion")
        self.logger.info("=" * 60)

        try:
            # Load raw data
            df = self.tse_manager.load_tse_stock_data()
            initial_count = len(df)

            # Filter tradable stocks
            tradable_df = self.tse_manager.filter_tradable_stocks(df)
            tradable_count = len(tradable_df)

            # Exclude investment products
            final_df = self.tse_manager.exclude_investment_products(tradable_df)
            final_count = len(final_df)

            excluded_count = tradable_count - final_count

            self.logger.info(f"Initial records: {initial_count:,}")
            self.logger.info(f"Tradable stocks: {tradable_count:,}")
            self.logger.info(f"After ETF exclusion: {final_count:,}")
            self.logger.info(f"Excluded investment products: {excluded_count:,}")

            # Analyze excluded categories
            excluded_df = tradable_df[~tradable_df.index.isin(final_df.index)]
            if len(excluded_df) > 0:
                excluded_breakdown = excluded_df["市場・商品区分"].value_counts()
                self.logger.info("Excluded categories breakdown:")
                for category, count in excluded_breakdown.items():
                    self.logger.info(f"   {category}: {count:,}")

            # Check if exclusion count is approximately 370 as expected
            expected_exclusion_range = (300, 450)  # Allow some variance
            exclusion_in_range = (
                expected_exclusion_range[0]
                <= excluded_count
                <= expected_exclusion_range[1]
            )

            if exclusion_in_range:
                self.logger.info(
                    f"✅ ETF exclusion count ({excluded_count:,}) is within expected range {expected_exclusion_range}"
                )
            else:
                self.logger.warning(
                    f"⚠️ ETF exclusion count ({excluded_count:,}) is outside expected range {expected_exclusion_range}"
                )

            # Verify no ETFs remain
            remaining_etfs = final_df[
                final_df["市場・商品区分"].str.contains("ETF", na=False)
            ]
            remaining_reits = final_df[
                final_df["市場・商品区分"].str.contains("REIT", na=False)
            ]

            if len(remaining_etfs) == 0 and len(remaining_reits) == 0:
                self.logger.info("✅ No ETFs or REITs remain in final dataset")
            else:
                self.logger.error(
                    f"❌ Found {len(remaining_etfs)} ETFs and {len(remaining_reits)} REITs in final dataset"
                )

            stats = {
                "initial_count": initial_count,
                "tradable_count": tradable_count,
                "final_count": final_count,
                "excluded_count": excluded_count,
                "exclusion_in_expected_range": exclusion_in_range,
                "remaining_etfs": len(remaining_etfs),
                "remaining_reits": len(remaining_reits),
                "excluded_breakdown": (
                    excluded_breakdown.to_dict() if len(excluded_df) > 0 else {}
                ),
            }

            success = (
                exclusion_in_range
                and len(remaining_etfs) == 0
                and len(remaining_reits) == 0
            )

            self.results["etf_exclusion"] = {"success": success, "stats": stats}

            return success, stats

        except Exception as e:
            self.logger.error(f"❌ ETF exclusion test failed: {e}")
            self.results["etf_exclusion"] = {"success": False, "error": str(e)}
            return False, {}

    def test_regular_stock_extraction(self) -> Tuple[bool, Dict[str, Any]]:
        """Test regular stock extraction (approximately 4,000 stocks)."""
        self.logger.info("=" * 60)
        self.logger.info("TEST 4: Regular Stock Extraction")
        self.logger.info("=" * 60)

        try:
            # Get all tradable stocks
            stocks = self.tse_manager.get_all_tradable_stocks()
            stock_count = len(stocks)

            self.logger.info(f"Total regular stocks extracted: {stock_count:,}")

            # Check if count is approximately 4,000 as expected
            expected_range = (3500, 4500)  # Allow some variance
            count_in_range = expected_range[0] <= stock_count <= expected_range[1]

            if count_in_range:
                self.logger.info(
                    f"✅ Stock count ({stock_count:,}) is within expected range {expected_range}"
                )
            else:
                self.logger.warning(
                    f"⚠️ Stock count ({stock_count:,}) is outside expected range {expected_range}"
                )

            # Verify all stocks have .T suffix
            all_have_suffix = all(stock.endswith(".T") for stock in stocks)
            if all_have_suffix:
                self.logger.info("✅ All stocks have .T suffix")
            else:
                invalid_stocks = [stock for stock in stocks if not stock.endswith(".T")]
                self.logger.error(
                    f"❌ Found {len(invalid_stocks)} stocks without .T suffix: {invalid_stocks[:5]}"
                )

            # Sample some stocks
            self.logger.info("Sample extracted stocks:")
            for stock in stocks[:10]:
                self.logger.info(f"   {stock}")

            # Get processing statistics
            processing_stats = self.tse_manager.get_processing_statistics()
            if processing_stats:
                self.logger.info("Processing statistics:")
                for key, value in processing_stats.items():
                    if isinstance(value, dict):
                        self.logger.info(f"   {key}:")
                        for sub_key, sub_value in value.items():
                            self.logger.info(f"     {sub_key}: {sub_value}")
                    else:
                        self.logger.info(f"   {key}: {value}")

            stats = {
                "stock_count": stock_count,
                "count_in_expected_range": count_in_range,
                "all_have_suffix": all_have_suffix,
                "sample_stocks": stocks[:10],
                "processing_stats": processing_stats,
            }

            success = count_in_range and all_have_suffix

            self.results["regular_stock_extraction"] = {
                "success": success,
                "stats": stats,
            }

            return success, stats

        except Exception as e:
            self.logger.error(f"❌ Regular stock extraction test failed: {e}")
            self.results["regular_stock_extraction"] = {
                "success": False,
                "error": str(e),
            }
            return False, {}

    def test_fallback_functionality(self) -> Tuple[bool, Dict[str, Any]]:
        """Test fallback functionality when TSE data loading fails."""
        self.logger.info("=" * 60)
        self.logger.info("TEST 5: Fallback Functionality")
        self.logger.info("=" * 60)

        try:
            # Test fallback stock list generation
            fallback_stocks = self.tse_manager.get_fallback_stock_list()
            fallback_count = len(fallback_stocks)

            self.logger.info(
                f"Fallback stock list generated: {fallback_count:,} stocks"
            )

            # Verify fallback stocks have .T suffix
            all_have_suffix = all(stock.endswith(".T") for stock in fallback_stocks)
            if all_have_suffix:
                self.logger.info("✅ All fallback stocks have .T suffix")
            else:
                self.logger.error("❌ Some fallback stocks missing .T suffix")

            # Test fallback with simulated failure
            original_path = self.tse_manager.config.data_file_path
            self.tse_manager.config.data_file_path = "nonexistent_file.xls"

            try:
                fallback_result = self.tse_manager.get_stocks_with_fallback()
                fallback_success = len(fallback_result) > 0

                if fallback_success:
                    self.logger.info(
                        f"✅ Fallback mechanism works: {len(fallback_result):,} stocks returned"
                    )
                else:
                    self.logger.error(
                        "❌ Fallback mechanism failed: no stocks returned"
                    )

            except Exception as fallback_error:
                self.logger.error(
                    f"❌ Fallback mechanism failed with error: {fallback_error}"
                )
                fallback_success = False
                fallback_result = []
            finally:
                # Restore original path
                self.tse_manager.config.data_file_path = original_path

            # Test with fallback disabled
            self.tse_manager.config.fallback_to_range_validation = False
            self.tse_manager.config.data_file_path = "nonexistent_file.xls"

            # Invalidate cache to force file loading
            self.tse_manager.invalidate_cache()

            try:
                self.tse_manager.get_stocks_with_fallback()
                fallback_disabled_works = False  # Should have raised an exception
                self.logger.error(
                    "❌ Fallback should have been disabled but didn't raise exception"
                )
            except Exception:
                fallback_disabled_works = True
                self.logger.info("✅ Fallback correctly disabled when configured")
            finally:
                # Restore original settings
                self.tse_manager.config.data_file_path = original_path
                self.tse_manager.config.fallback_to_range_validation = True

            stats = {
                "fallback_count": fallback_count,
                "all_have_suffix": all_have_suffix,
                "fallback_mechanism_works": fallback_success,
                "fallback_disabled_works": fallback_disabled_works,
                "sample_fallback_stocks": fallback_stocks[:10],
            }

            success = all_have_suffix and fallback_success and fallback_disabled_works

            self.results["fallback_functionality"] = {
                "success": success,
                "stats": stats,
            }

            return success, stats

        except Exception as e:
            self.logger.error(f"❌ Fallback functionality test failed: {e}")
            self.results["fallback_functionality"] = {"success": False, "error": str(e)}
            return False, {}

    def test_data_fetcher_integration(self) -> Tuple[bool, Dict[str, Any]]:
        """Test DataFetcher integration with TSE data."""
        self.logger.info("=" * 60)
        self.logger.info("TEST 6: DataFetcher TSE Integration")
        self.logger.info("=" * 60)

        try:
            # Test TSE official mode
            start_time = time.time()
            tse_stocks = self.data_fetcher.get_japanese_stock_list(mode="tse_official")
            fetch_time = time.time() - start_time

            self.logger.info(
                f"DataFetcher TSE mode: {len(tse_stocks):,} stocks in {fetch_time:.2f}s"
            )

            # Compare with direct TSE manager call
            direct_stocks = self.tse_manager.get_all_tradable_stocks()

            stocks_match = set(tse_stocks) == set(direct_stocks)
            if stocks_match:
                self.logger.info("✅ DataFetcher TSE mode matches direct TSE manager")
            else:
                self.logger.error(
                    "❌ DataFetcher TSE mode doesn't match direct TSE manager"
                )
                self.logger.error(
                    f"   DataFetcher: {len(tse_stocks):,}, Direct: {len(direct_stocks):,}"
                )

            # Test other modes for comparison
            curated_stocks = self.data_fetcher.get_japanese_stock_list(mode="curated")
            all_stocks = self.data_fetcher.get_japanese_stock_list(mode="all")

            self.logger.info(f"Mode comparison:")
            self.logger.info(f"   TSE Official: {len(tse_stocks):,}")
            self.logger.info(f"   Curated: {len(curated_stocks):,}")
            self.logger.info(f"   All: {len(all_stocks):,}")

            # Test TSE metadata functionality
            sample_stock = tse_stocks[0] if tse_stocks else None
            if sample_stock:
                metadata = self.data_fetcher.get_tse_stock_metadata(sample_stock)
                if metadata:
                    self.logger.info(f"✅ TSE metadata works for {sample_stock}:")
                    self.logger.info(f"   Name: {metadata.get('name', 'N/A')}")
                    self.logger.info(
                        f"   Market: {metadata.get('market_category', 'N/A')}"
                    )
                    self.logger.info(
                        f"   Sector: {metadata.get('sector_17_name', 'N/A')}"
                    )
                else:
                    self.logger.warning(f"⚠️ No TSE metadata found for {sample_stock}")

            stats = {
                "tse_stock_count": len(tse_stocks),
                "fetch_time": fetch_time,
                "stocks_match_direct": stocks_match,
                "curated_count": len(curated_stocks),
                "all_count": len(all_stocks),
                "metadata_available": bool(metadata) if sample_stock else False,
            }

            success = len(tse_stocks) > 0 and stocks_match

            self.results["data_fetcher_integration"] = {
                "success": success,
                "stats": stats,
            }

            return success, stats

        except Exception as e:
            self.logger.error(f"❌ DataFetcher integration test failed: {e}")
            self.results["data_fetcher_integration"] = {
                "success": False,
                "error": str(e),
            }
            return False, {}

    def test_data_integrity_validation(self) -> Tuple[bool, Dict[str, Any]]:
        """Test data integrity validation."""
        self.logger.info("=" * 60)
        self.logger.info("TEST 7: Data Integrity Validation")
        self.logger.info("=" * 60)

        try:
            validation_results = self.tse_manager.validate_data_integrity()

            is_valid = validation_results.get("is_valid", False)
            issues = validation_results.get("issues", [])
            total_records = validation_results.get("total_records", 0)

            self.logger.info(f"Data integrity validation:")
            self.logger.info(f"   Total records: {total_records:,}")
            self.logger.info(f"   Is valid: {is_valid}")
            self.logger.info(f"   Issues found: {len(issues)}")

            if issues:
                self.logger.info("Issues details:")
                for issue in issues:
                    self.logger.info(
                        f"   - {issue.get('type', 'unknown')}: {issue.get('count', 0)}"
                    )
            else:
                self.logger.info("✅ No data integrity issues found")

            # Test classification distribution
            distribution = self.tse_manager.get_classification_distribution()
            if distribution:
                self.logger.info("Classification distribution:")
                for category, dist in distribution.items():
                    if isinstance(dist, dict) and dist:
                        top_items = sorted(
                            dist.items(), key=lambda x: x[1], reverse=True
                        )[:5]
                        self.logger.info(f"   {category} (top 5):")
                        for item, count in top_items:
                            self.logger.info(f"     {item}: {count}")

            stats = {
                "is_valid": is_valid,
                "total_records": total_records,
                "issues_count": len(issues),
                "issues": issues,
                "distribution_available": bool(distribution),
            }

            success = is_valid and total_records > 0

            self.results["data_integrity"] = {"success": success, "stats": stats}

            return success, stats

        except Exception as e:
            self.logger.error(f"❌ Data integrity validation test failed: {e}")
            self.results["data_integrity"] = {"success": False, "error": str(e)}
            return False, {}

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all TSE integration tests."""
        self.logger.info("🚀 Starting TSE Integration Tests")
        self.logger.info("=" * 80)

        start_time = time.time()

        # Run all tests
        tests = [
            ("Data File Existence", self.test_data_file_existence),
            ("TSE Data Loading", self.test_tse_data_loading),
            ("ETF Exclusion", self.test_etf_exclusion),
            ("Regular Stock Extraction", self.test_regular_stock_extraction),
            ("Fallback Functionality", self.test_fallback_functionality),
            ("DataFetcher Integration", self.test_data_fetcher_integration),
            ("Data Integrity Validation", self.test_data_integrity_validation),
        ]

        passed_tests = 0
        failed_tests = 0

        for test_name, test_func in tests:
            try:
                if hasattr(test_func, "__call__"):
                    if test_func.__name__ == "test_data_file_existence":
                        result = test_func()
                        success = result
                    else:
                        success, _ = test_func()
                else:
                    success = test_func

                if success:
                    passed_tests += 1
                    self.logger.info(f"✅ {test_name}: PASSED")
                else:
                    failed_tests += 1
                    self.logger.error(f"❌ {test_name}: FAILED")

            except Exception as e:
                failed_tests += 1
                self.logger.error(f"❌ {test_name}: ERROR - {e}")

            self.logger.info("")  # Add spacing between tests

        total_time = time.time() - start_time

        # Summary
        self.logger.info("=" * 80)
        self.logger.info("🏁 TSE Integration Test Summary")
        self.logger.info("=" * 80)
        self.logger.info(f"Total tests: {len(tests)}")
        self.logger.info(f"Passed: {passed_tests}")
        self.logger.info(f"Failed: {failed_tests}")
        self.logger.info(f"Success rate: {passed_tests/len(tests)*100:.1f}%")
        self.logger.info(f"Total time: {total_time:.2f} seconds")

        # Overall result
        overall_success = failed_tests == 0
        if overall_success:
            self.logger.info(
                "🎉 ALL TESTS PASSED - TSE Integration is working correctly!"
            )
        else:
            self.logger.error("💥 SOME TESTS FAILED - Please review the issues above")

        # Save results
        self.results["summary"] = {
            "total_tests": len(tests),
            "passed": passed_tests,
            "failed": failed_tests,
            "success_rate": passed_tests / len(tests),
            "total_time": total_time,
            "overall_success": overall_success,
        }

        return self.results

    def generate_report(self) -> str:
        """Generate a detailed test report."""
        if not self.results:
            return "No test results available"

        report = []
        report.append("# TSE Integration Test Report")
        report.append("=" * 50)
        report.append("")

        summary = self.results.get("summary", {})
        report.append(
            f"**Overall Result**: {'✅ PASSED' if summary.get('overall_success') else '❌ FAILED'}"
        )
        report.append(
            f"**Tests Passed**: {summary.get('passed', 0)}/{summary.get('total_tests', 0)}"
        )
        report.append(f"**Success Rate**: {summary.get('success_rate', 0)*100:.1f}%")
        report.append(f"**Total Time**: {summary.get('total_time', 0):.2f} seconds")
        report.append("")

        # Detailed results
        for test_name, result in self.results.items():
            if test_name == "summary":
                continue

            report.append(f"## {test_name.replace('_', ' ').title()}")

            if isinstance(result, dict):
                success = result.get("success", False)
                report.append(f"**Status**: {'✅ PASSED' if success else '❌ FAILED'}")

                if "stats" in result:
                    report.append("**Statistics**:")
                    stats = result["stats"]
                    for key, value in stats.items():
                        if isinstance(value, dict):
                            report.append(f"- {key}:")
                            for sub_key, sub_value in value.items():
                                report.append(f"  - {sub_key}: {sub_value}")
                        else:
                            report.append(f"- {key}: {value}")

                if "error" in result:
                    report.append(f"**Error**: {result['error']}")
            else:
                report.append(f"**Status**: {'✅ PASSED' if result else '❌ FAILED'}")

            report.append("")

        return "\n".join(report)


def main():
    """Main function to run TSE integration tests."""
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    # Create and run tester
    tester = TSEIntegrationTester()
    results = tester.run_all_tests()

    # Generate and save report
    report = tester.generate_report()

    # Save report to file
    with open("logs/tse_integration_report.md", "w", encoding="utf-8") as f:
        f.write(report)

    print("\n" + "=" * 80)
    print("📊 Test report saved to: logs/tse_integration_report.md")
    print("📋 Detailed logs saved to: logs/tse_integration_test.log")
    print("=" * 80)

    # Return appropriate exit code
    overall_success = results.get("summary", {}).get("overall_success", False)
    return 0 if overall_success else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
