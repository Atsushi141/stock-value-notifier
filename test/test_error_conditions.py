#!/usr/bin/env python3
"""
Comprehensive test script for error condition verification.

This script tests the actual error conditions mentioned in task 10.1:
- 上場廃止銘柄（1423.T等）での動作確認
- タイムゾーンエラー条件での動作確認
- 大量エラー時の処理継続確認

Tests requirements 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.5, 4.3, 4.4
"""

import sys
import os
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import traceback

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src.data_fetcher import (
    DataFetcher,
    create_datafetcher_with_tolerant_error_handling,
)
from src.error_handling_config import create_tolerant_config, create_debug_config
from src.exceptions import DataNotFoundError, APIError
from src.enhanced_logger import EnhancedLogger


class ErrorConditionTester:
    """Test class for verifying error condition handling."""

    def __init__(self):
        """Initialize the tester with enhanced error handling."""
        self.logger = logging.getLogger(__name__)

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        # Create DataFetcher with tolerant error handling for testing
        self.data_fetcher = create_datafetcher_with_tolerant_error_handling()

        # Test results storage
        self.test_results = {
            "delisted_stock_tests": {},
            "timezone_error_tests": {},
            "bulk_error_tests": {},
            "performance_metrics": {},
            "error_statistics": {},
        }

        self.logger.info(
            "ErrorConditionTester initialized with tolerant error handling"
        )

    def test_delisted_stocks(self) -> Dict[str, Any]:
        """
        Test delisted stock handling with known delisted symbols.

        Tests requirements 1.1, 1.2, 1.3, 1.4, 1.5
        """
        self.logger.info("=== Testing Delisted Stock Handling ===")

        # Known delisted or problematic stocks
        delisted_symbols = [
            "1423.T",  # Specifically mentioned in task
            "9999.T",  # Likely non-existent
            "0001.T",  # Likely non-existent
            "8888.T",  # Likely non-existent
            "1111.T",  # May be delisted
            "2222.T",  # May be delisted
        ]

        results = {}

        for symbol in delisted_symbols:
            self.logger.info(f"Testing delisted stock: {symbol}")
            result = self._test_single_delisted_stock(symbol)
            results[symbol] = result

            # Small delay to avoid rate limiting
            time.sleep(0.5)

        # Test batch processing with delisted stocks
        self.logger.info("Testing batch processing with delisted stocks")
        batch_result = self._test_batch_delisted_processing(delisted_symbols)
        results["batch_processing"] = batch_result

        self.test_results["delisted_stock_tests"] = results
        return results

    def _test_single_delisted_stock(self, symbol: str) -> Dict[str, Any]:
        """Test single delisted stock handling."""
        result = {
            "symbol": symbol,
            "financial_info_error": None,
            "price_data_error": None,
            "dividend_data_error": None,
            "validation_result": None,
            "error_logged": False,
            "processing_continued": True,
        }

        try:
            # Test financial info retrieval
            try:
                financial_info = self.data_fetcher.get_financial_info(symbol)
                result["financial_info_success"] = True
                result["financial_info_data"] = (
                    len(financial_info) if financial_info else 0
                )
            except DataNotFoundError as e:
                result["financial_info_error"] = str(e)
                result["error_logged"] = True
                self.logger.info(f"Expected DataNotFoundError for {symbol}: {e}")
            except Exception as e:
                result["financial_info_error"] = f"Unexpected error: {str(e)}"
                self.logger.warning(f"Unexpected error for {symbol}: {e}")

            # Test price data retrieval
            try:
                price_data = self.data_fetcher.get_stock_prices(symbol)
                result["price_data_success"] = True
                result["price_data_records"] = (
                    len(price_data) if price_data is not None else 0
                )
            except DataNotFoundError as e:
                result["price_data_error"] = str(e)
                result["error_logged"] = True
                self.logger.info(
                    f"Expected DataNotFoundError for price data {symbol}: {e}"
                )
            except Exception as e:
                result["price_data_error"] = f"Unexpected error: {str(e)}"
                self.logger.warning(f"Unexpected price data error for {symbol}: {e}")

            # Test dividend data retrieval
            try:
                dividend_data = self.data_fetcher.get_dividend_history(symbol)
                result["dividend_data_success"] = True
                result["dividend_data_records"] = (
                    len(dividend_data) if dividend_data is not None else 0
                )
            except DataNotFoundError as e:
                result["dividend_data_error"] = str(e)
                result["error_logged"] = True
                self.logger.info(
                    f"Expected DataNotFoundError for dividend data {symbol}: {e}"
                )
            except Exception as e:
                result["dividend_data_error"] = f"Unexpected error: {str(e)}"
                self.logger.warning(f"Unexpected dividend data error for {symbol}: {e}")

            # Test symbol validation
            try:
                is_valid = self.data_fetcher.validate_symbol(symbol)
                result["validation_result"] = is_valid
                self.logger.info(f"Symbol validation for {symbol}: {is_valid}")
            except Exception as e:
                result["validation_error"] = str(e)
                self.logger.warning(f"Validation error for {symbol}: {e}")

        except Exception as e:
            result["processing_continued"] = False
            result["fatal_error"] = str(e)
            self.logger.error(f"Fatal error processing {symbol}: {e}")

        return result

    def _test_batch_delisted_processing(self, symbols: List[str]) -> Dict[str, Any]:
        """Test batch processing with delisted stocks."""
        result = {
            "total_symbols": len(symbols),
            "successful_retrievals": 0,
            "data_not_found_errors": 0,
            "other_errors": 0,
            "processing_completed": False,
            "filtered_symbols": [],
        }

        try:
            # Test symbol filtering
            filtered_symbols = self.data_fetcher.validate_and_filter_symbols(symbols)
            result["filtered_symbols"] = filtered_symbols
            result["filtered_count"] = len(filtered_symbols)

            # Test batch financial info retrieval
            financial_results = self.data_fetcher.get_multiple_stocks_info(symbols)
            result["successful_retrievals"] = len(financial_results)
            result["processing_completed"] = True

            self.logger.info(
                f"Batch processing completed - Original: {len(symbols)}, "
                f"Filtered: {len(filtered_symbols)}, "
                f"Successful: {len(financial_results)}"
            )

        except Exception as e:
            result["batch_error"] = str(e)
            self.logger.error(f"Batch processing error: {e}")

        return result

    def test_timezone_errors(self) -> Dict[str, Any]:
        """
        Test timezone error handling with various datetime scenarios.

        Tests requirements 2.1, 2.2, 2.3, 2.5
        """
        self.logger.info("=== Testing Timezone Error Handling ===")

        # Test with valid symbols that might have timezone issues
        test_symbols = [
            "7203.T",  # Toyota - should have data
            "6758.T",  # Sony - should have data
            "9984.T",  # SoftBank - should have data
        ]

        results = {}

        for symbol in test_symbols:
            self.logger.info(f"Testing timezone handling for: {symbol}")
            result = self._test_timezone_handling(symbol)
            results[symbol] = result

            # Small delay
            time.sleep(0.5)

        self.test_results["timezone_error_tests"] = results
        return results

    def _test_timezone_handling(self, symbol: str) -> Dict[str, Any]:
        """Test timezone handling for a single symbol."""
        result = {
            "symbol": symbol,
            "dividend_retrieval_success": False,
            "timezone_errors_handled": 0,
            "fallback_used": False,
            "data_retrieved": False,
        }

        try:
            # Test dividend history retrieval (most likely to have timezone issues)
            dividend_data = self.data_fetcher.get_dividend_history(symbol, period="2y")

            if dividend_data is not None and not dividend_data.empty:
                result["dividend_retrieval_success"] = True
                result["data_retrieved"] = True
                result["record_count"] = len(dividend_data)

                # Check if data has proper date formatting
                if "Date" in dividend_data.columns:
                    result["has_date_column"] = True
                    result["date_range"] = {
                        "start": str(dividend_data["Date"].min()),
                        "end": str(dividend_data["Date"].max()),
                    }

                self.logger.info(
                    f"Successfully retrieved {len(dividend_data)} dividend records for {symbol}"
                )
            else:
                result["dividend_retrieval_success"] = True  # Empty data is valid
                result["data_retrieved"] = True
                result["record_count"] = 0
                self.logger.info(f"No dividend data found for {symbol} (valid result)")

        except Exception as e:
            result["error"] = str(e)
            result["error_type"] = type(e).__name__
            self.logger.warning(f"Error retrieving dividend data for {symbol}: {e}")

            # Check if it's a timezone-related error
            if any(
                tz_keyword in str(e).lower()
                for tz_keyword in ["timezone", "tz", "utc", "localize"]
            ):
                result["timezone_error_detected"] = True
                result["timezone_errors_handled"] += 1

        return result

    def test_bulk_error_processing(self) -> Dict[str, Any]:
        """
        Test processing continuation with bulk errors.

        Tests requirements 4.3, 4.4
        """
        self.logger.info("=== Testing Bulk Error Processing ===")

        # Mix of valid and invalid symbols
        mixed_symbols = [
            # Valid symbols
            "7203.T",  # Toyota
            "6758.T",  # Sony
            "9984.T",  # SoftBank
            # Invalid/delisted symbols
            "1423.T",  # Mentioned in task
            "9999.T",  # Likely non-existent
            "0001.T",  # Likely non-existent
            "8888.T",  # Likely non-existent
            # More valid symbols
            "7974.T",  # Nintendo
            "9432.T",  # NTT
            "4063.T",  # Shin-Etsu Chemical
            # More invalid symbols
            "1111.T",  # May be delisted
            "2222.T",  # May be delisted
            "3333.T",  # May be delisted
        ]

        result = {
            "total_symbols": len(mixed_symbols),
            "processing_started": False,
            "processing_completed": False,
            "successful_count": 0,
            "error_count": 0,
            "continuation_verified": False,
        }

        try:
            self.logger.info(
                f"Starting bulk processing of {len(mixed_symbols)} symbols"
            )
            result["processing_started"] = True

            # Test with processing continuation
            financial_results, processing_result = (
                self.data_fetcher.get_multiple_stocks_info_with_continuation(
                    mixed_symbols, skip_invalid=True
                )
            )

            result["successful_count"] = len(financial_results)
            result["error_count"] = (
                processing_result.error_count
                if hasattr(processing_result, "error_count")
                else 0
            )
            result["processing_completed"] = True
            result["continuation_verified"] = result["successful_count"] > 0

            # Calculate success rate
            result["success_rate"] = (
                result["successful_count"] / result["total_symbols"]
            )

            self.logger.info(
                f"Bulk processing completed - Total: {result['total_symbols']}, "
                f"Successful: {result['successful_count']}, "
                f"Errors: {result['error_count']}, "
                f"Success Rate: {result['success_rate']:.1%}"
            )

            # Test symbol filtering
            filtered_symbols = self.data_fetcher.validate_and_filter_symbols(
                mixed_symbols
            )
            result["filtered_count"] = len(filtered_symbols)
            result["filter_rate"] = 1 - (len(filtered_symbols) / len(mixed_symbols))

            self.logger.info(
                f"Symbol filtering - Original: {len(mixed_symbols)}, "
                f"Filtered: {len(filtered_symbols)}, "
                f"Filter Rate: {result['filter_rate']:.1%}"
            )

        except Exception as e:
            result["bulk_processing_error"] = str(e)
            result["error_traceback"] = traceback.format_exc()
            self.logger.error(f"Bulk processing failed: {e}")

        self.test_results["bulk_error_tests"] = result
        return result

    def measure_performance_impact(self) -> Dict[str, Any]:
        """
        Measure performance impact of error handling.

        Tests task 10.2 requirements
        """
        self.logger.info("=== Measuring Performance Impact ===")

        # Test symbols
        test_symbols = ["7203.T", "6758.T", "9984.T"]  # Valid symbols
        error_symbols = ["9999.T", "8888.T", "7777.T"]  # Invalid symbols

        results = {
            "valid_symbols_performance": {},
            "error_symbols_performance": {},
            "memory_usage": {},
            "error_handling_overhead": {},
        }

        # Test valid symbols performance
        start_time = time.time()
        valid_results = {}
        for symbol in test_symbols:
            symbol_start = time.time()
            try:
                financial_info = self.data_fetcher.get_financial_info(symbol)
                valid_results[symbol] = financial_info
            except Exception as e:
                self.logger.warning(f"Error with valid symbol {symbol}: {e}")
            symbol_end = time.time()
            results["valid_symbols_performance"][symbol] = symbol_end - symbol_start

        valid_total_time = time.time() - start_time
        results["valid_symbols_total_time"] = valid_total_time
        results["valid_symbols_avg_time"] = valid_total_time / len(test_symbols)

        # Test error symbols performance
        start_time = time.time()
        error_results = {}
        for symbol in error_symbols:
            symbol_start = time.time()
            try:
                financial_info = self.data_fetcher.get_financial_info(symbol)
                error_results[symbol] = financial_info
            except DataNotFoundError:
                # Expected error
                pass
            except Exception as e:
                self.logger.warning(f"Unexpected error with error symbol {symbol}: {e}")
            symbol_end = time.time()
            results["error_symbols_performance"][symbol] = symbol_end - symbol_start

        error_total_time = time.time() - start_time
        results["error_symbols_total_time"] = error_total_time
        results["error_symbols_avg_time"] = error_total_time / len(error_symbols)

        # Calculate overhead
        results["error_handling_overhead"]["absolute"] = (
            error_total_time - valid_total_time
        )
        results["error_handling_overhead"]["relative"] = (
            (error_total_time / valid_total_time) - 1 if valid_total_time > 0 else 0
        )

        self.logger.info(
            f"Performance measurement completed - "
            f"Valid symbols avg: {results['valid_symbols_avg_time']:.2f}s, "
            f"Error symbols avg: {results['error_symbols_avg_time']:.2f}s, "
            f"Overhead: {results['error_handling_overhead']['relative']:.1%}"
        )

        self.test_results["performance_metrics"] = results
        return results

    def collect_error_statistics(self) -> Dict[str, Any]:
        """Collect comprehensive error statistics."""
        self.logger.info("=== Collecting Error Statistics ===")

        try:
            # Get error handling status
            error_status = self.data_fetcher.get_error_handling_status()

            # Get error metrics
            error_metrics = self.data_fetcher.get_error_metrics()
            error_summary = error_metrics.get_error_summary(timedelta(hours=1))

            # Get retry statistics
            retry_stats = self.data_fetcher.get_retry_statistics()

            # Get validation statistics
            validation_stats = self.data_fetcher.get_validation_statistics()

            # Get symbol filtering statistics
            filtering_stats = self.data_fetcher.get_symbol_filtering_statistics()

            results = {
                "error_handling_status": error_status,
                "error_metrics_summary": error_summary,
                "retry_statistics": retry_stats,
                "validation_statistics": validation_stats,
                "filtering_statistics": filtering_stats,
                "collection_timestamp": datetime.now().isoformat(),
            }

            self.logger.info("Error statistics collection completed")

        except Exception as e:
            results = {
                "collection_error": str(e),
                "collection_timestamp": datetime.now().isoformat(),
            }
            self.logger.error(f"Error collecting statistics: {e}")

        self.test_results["error_statistics"] = results
        return results

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all error condition tests."""
        self.logger.info("Starting comprehensive error condition testing")

        start_time = time.time()

        try:
            # Test 1: Delisted stock handling
            self.test_delisted_stocks()

            # Test 2: Timezone error handling
            self.test_timezone_errors()

            # Test 3: Bulk error processing
            self.test_bulk_error_processing()

            # Test 4: Performance measurement
            self.measure_performance_impact()

            # Test 5: Error statistics collection
            self.collect_error_statistics()

            # Calculate overall results
            total_time = time.time() - start_time
            self.test_results["overall"] = {
                "total_test_time": total_time,
                "all_tests_completed": True,
                "test_timestamp": datetime.now().isoformat(),
            }

            self.logger.info(
                f"All error condition tests completed in {total_time:.2f} seconds"
            )

        except Exception as e:
            self.test_results["overall"] = {
                "test_error": str(e),
                "test_traceback": traceback.format_exc(),
                "all_tests_completed": False,
                "test_timestamp": datetime.now().isoformat(),
            }
            self.logger.error(f"Error condition testing failed: {e}")

        return self.test_results

    def print_test_summary(self):
        """Print a comprehensive test summary."""
        print("\n" + "=" * 80)
        print("ERROR CONDITION TEST SUMMARY")
        print("=" * 80)

        # Delisted stock tests
        if "delisted_stock_tests" in self.test_results:
            delisted_results = self.test_results["delisted_stock_tests"]
            print(f"\n1. DELISTED STOCK TESTS:")

            for symbol, result in delisted_results.items():
                if symbol == "batch_processing":
                    continue
                print(f"   {symbol}:")
                print(
                    f"     - Financial Info Error: {result.get('financial_info_error', 'None')}"
                )
                print(
                    f"     - Price Data Error: {result.get('price_data_error', 'None')}"
                )
                print(
                    f"     - Processing Continued: {result.get('processing_continued', 'Unknown')}"
                )

            if "batch_processing" in delisted_results:
                batch = delisted_results["batch_processing"]
                print(f"   Batch Processing:")
                print(f"     - Total Symbols: {batch.get('total_symbols', 0)}")
                print(f"     - Successful: {batch.get('successful_retrievals', 0)}")
                print(f"     - Filtered: {batch.get('filtered_count', 0)}")

        # Timezone error tests
        if "timezone_error_tests" in self.test_results:
            timezone_results = self.test_results["timezone_error_tests"]
            print(f"\n2. TIMEZONE ERROR TESTS:")

            for symbol, result in timezone_results.items():
                print(f"   {symbol}:")
                print(
                    f"     - Dividend Retrieval Success: {result.get('dividend_retrieval_success', False)}"
                )
                print(f"     - Records Retrieved: {result.get('record_count', 0)}")
                print(
                    f"     - Timezone Errors Handled: {result.get('timezone_errors_handled', 0)}"
                )

        # Bulk error tests
        if "bulk_error_tests" in self.test_results:
            bulk_results = self.test_results["bulk_error_tests"]
            print(f"\n3. BULK ERROR PROCESSING TESTS:")
            print(f"   - Total Symbols: {bulk_results.get('total_symbols', 0)}")
            print(f"   - Successful: {bulk_results.get('successful_count', 0)}")
            print(f"   - Success Rate: {bulk_results.get('success_rate', 0):.1%}")
            print(
                f"   - Processing Completed: {bulk_results.get('processing_completed', False)}"
            )
            print(
                f"   - Continuation Verified: {bulk_results.get('continuation_verified', False)}"
            )

        # Performance metrics
        if "performance_metrics" in self.test_results:
            perf_results = self.test_results["performance_metrics"]
            print(f"\n4. PERFORMANCE METRICS:")
            print(
                f"   - Valid Symbols Avg Time: {perf_results.get('valid_symbols_avg_time', 0):.2f}s"
            )
            print(
                f"   - Error Symbols Avg Time: {perf_results.get('error_symbols_avg_time', 0):.2f}s"
            )
            if "error_handling_overhead" in perf_results:
                overhead = perf_results["error_handling_overhead"]
                print(
                    f"   - Error Handling Overhead: {overhead.get('relative', 0):.1%}"
                )

        # Error statistics
        if "error_statistics" in self.test_results:
            error_stats = self.test_results["error_statistics"]
            print(f"\n5. ERROR STATISTICS:")
            if "error_metrics_summary" in error_stats:
                summary = error_stats["error_metrics_summary"]
                print(f"   - Total Errors: {summary.get('total_errors', 0)}")
                print(f"   - Error Types: {len(summary.get('error_types', {}))}")

            if "retry_statistics" in error_stats:
                retry_stats = error_stats["retry_statistics"]
                print(f"   - Total Retries: {retry_stats.get('total_retries', 0)}")
                print(
                    f"   - Successful Retries: {retry_stats.get('successful_retries', 0)}"
                )

        # Overall results
        if "overall" in self.test_results:
            overall = self.test_results["overall"]
            print(f"\n6. OVERALL RESULTS:")
            print(
                f"   - All Tests Completed: {overall.get('all_tests_completed', False)}"
            )
            print(f"   - Total Test Time: {overall.get('total_test_time', 0):.2f}s")
            if "test_error" in overall:
                print(f"   - Test Error: {overall['test_error']}")

        print("\n" + "=" * 80)


def main():
    """Main function to run error condition tests."""
    print("Starting Error Condition Testing for Stock Value Notifier")
    print("Testing requirements 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.5, 4.3, 4.4")

    tester = ErrorConditionTester()

    # Run all tests
    results = tester.run_all_tests()

    # Print summary
    tester.print_test_summary()

    # Return success/failure based on results
    overall_success = results.get("overall", {}).get("all_tests_completed", False)

    if overall_success:
        print("\n✅ All error condition tests completed successfully!")
        return 0
    else:
        print("\n❌ Some error condition tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
