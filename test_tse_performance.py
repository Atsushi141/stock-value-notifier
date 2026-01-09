#!/usr/bin/env python3
"""
TSE Performance Test Script

This script performs performance testing of the TSE integration functionality
as specified in task 16.2 "パフォーマンステスト".

Test Coverage:
- TSEデータ読み込み時間の測定
- 既存の範囲ベース検証との比較
- メモリ使用量の確認

Requirements: Performance optimization and monitoring
"""

import sys
import os
import logging
import time
import psutil
import gc
from pathlib import Path
from typing import Dict, Any, List, Tuple
import tracemalloc

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src.tse_stock_list_manager import TSEStockListManager
from src.data_fetcher import DataFetcher
from src.models import TSEDataConfig


class TSEPerformanceTester:
    """TSE performance testing suite."""

    def __init__(self):
        """Initialize the performance tester."""
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
                logging.FileHandler("logs/tse_performance_test.log"),
            ],
        )

    def get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage statistics."""
        process = psutil.Process()
        memory_info = process.memory_info()

        return {
            "rss_mb": memory_info.rss / 1024 / 1024,  # Resident Set Size
            "vms_mb": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
            "percent": process.memory_percent(),
            "available_mb": psutil.virtual_memory().available / 1024 / 1024,
        }

    def measure_execution_time(
        self, func, *args, **kwargs
    ) -> Tuple[Any, float, Dict[str, float], Dict[str, float]]:
        """Measure execution time and memory usage of a function."""
        # Start memory tracking
        tracemalloc.start()
        gc.collect()  # Clean up before measurement

        memory_before = self.get_memory_usage()
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time

            memory_after = self.get_memory_usage()

            # Get tracemalloc statistics
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            memory_stats = {
                "peak_mb": peak / 1024 / 1024,
                "current_mb": current / 1024 / 1024,
                "rss_increase_mb": memory_after["rss_mb"] - memory_before["rss_mb"],
                "vms_increase_mb": memory_after["vms_mb"] - memory_before["vms_mb"],
            }

            return result, execution_time, memory_before, memory_after, memory_stats

        except Exception as e:
            tracemalloc.stop()
            raise e

    def test_tse_data_loading_performance(self) -> Dict[str, Any]:
        """Test TSE data loading performance."""
        self.logger.info("=" * 60)
        self.logger.info("PERFORMANCE TEST 1: TSE Data Loading")
        self.logger.info("=" * 60)

        results = {}

        try:
            # Test cold load (first time)
            self.logger.info("Testing cold load (first time)...")
            self.tse_manager.invalidate_cache()

            df, cold_time, mem_before, mem_after, mem_stats = (
                self.measure_execution_time(self.tse_manager.load_tse_stock_data)
            )

            self.logger.info(f"Cold load time: {cold_time:.3f} seconds")
            self.logger.info(f"Records loaded: {len(df):,}")
            self.logger.info(f"Memory usage: {mem_stats['peak_mb']:.2f} MB peak")
            self.logger.info(f"RSS increase: {mem_stats['rss_increase_mb']:.2f} MB")

            results["cold_load"] = {
                "time": cold_time,
                "records": len(df),
                "memory_peak_mb": mem_stats["peak_mb"],
                "memory_increase_mb": mem_stats["rss_increase_mb"],
            }

            # Test warm load (cached)
            self.logger.info("Testing warm load (cached)...")

            df_cached, warm_time, mem_before_warm, mem_after_warm, mem_stats_warm = (
                self.measure_execution_time(self.tse_manager.load_tse_stock_data)
            )

            self.logger.info(f"Warm load time: {warm_time:.3f} seconds")
            self.logger.info(f"Speed improvement: {cold_time/warm_time:.1f}x faster")

            results["warm_load"] = {
                "time": warm_time,
                "records": len(df_cached),
                "speed_improvement": cold_time / warm_time,
                "memory_peak_mb": mem_stats_warm["peak_mb"],
            }

            # Test multiple loads to check consistency
            self.logger.info("Testing load consistency (5 iterations)...")
            load_times = []

            for i in range(5):
                self.tse_manager.invalidate_cache()
                _, load_time, _, _, _ = self.measure_execution_time(
                    self.tse_manager.load_tse_stock_data
                )
                load_times.append(load_time)

            avg_time = sum(load_times) / len(load_times)
            min_time = min(load_times)
            max_time = max(load_times)
            std_dev = (
                sum((t - avg_time) ** 2 for t in load_times) / len(load_times)
            ) ** 0.5

            self.logger.info(f"Average load time: {avg_time:.3f} seconds")
            self.logger.info(f"Min/Max: {min_time:.3f}s / {max_time:.3f}s")
            self.logger.info(f"Standard deviation: {std_dev:.3f}s")
            self.logger.info(f"Coefficient of variation: {std_dev/avg_time*100:.1f}%")

            results["consistency"] = {
                "average_time": avg_time,
                "min_time": min_time,
                "max_time": max_time,
                "std_dev": std_dev,
                "coefficient_of_variation": std_dev / avg_time,
            }

        except Exception as e:
            self.logger.error(f"TSE data loading performance test failed: {e}")
            results["error"] = str(e)

        self.results["tse_loading_performance"] = results
        return results

    def test_stock_filtering_performance(self) -> Dict[str, Any]:
        """Test stock filtering performance."""
        self.logger.info("=" * 60)
        self.logger.info("PERFORMANCE TEST 2: Stock Filtering")
        self.logger.info("=" * 60)

        results = {}

        try:
            # Load data first
            df = self.tse_manager.load_tse_stock_data()
            initial_records = len(df)

            # Test tradable stock filtering
            self.logger.info("Testing tradable stock filtering...")
            tradable_df, filter_time, _, _, mem_stats = self.measure_execution_time(
                self.tse_manager.filter_tradable_stocks, df
            )

            self.logger.info(f"Tradable filtering time: {filter_time:.3f} seconds")
            self.logger.info(f"Records: {initial_records:,} → {len(tradable_df):,}")
            self.logger.info(
                f"Filtering rate: {initial_records/filter_time:.0f} records/second"
            )

            results["tradable_filtering"] = {
                "time": filter_time,
                "input_records": initial_records,
                "output_records": len(tradable_df),
                "filtering_rate": initial_records / filter_time,
                "memory_peak_mb": mem_stats["peak_mb"],
            }

            # Test investment product exclusion
            self.logger.info("Testing investment product exclusion...")
            final_df, exclusion_time, _, _, mem_stats_excl = (
                self.measure_execution_time(
                    self.tse_manager.exclude_investment_products, tradable_df
                )
            )

            excluded_count = len(tradable_df) - len(final_df)

            self.logger.info(f"Exclusion time: {exclusion_time:.3f} seconds")
            self.logger.info(f"Records: {len(tradable_df):,} → {len(final_df):,}")
            self.logger.info(f"Excluded: {excluded_count:,}")
            self.logger.info(
                f"Processing rate: {len(tradable_df)/exclusion_time:.0f} records/second"
            )

            results["investment_exclusion"] = {
                "time": exclusion_time,
                "input_records": len(tradable_df),
                "output_records": len(final_df),
                "excluded_records": excluded_count,
                "processing_rate": len(tradable_df) / exclusion_time,
                "memory_peak_mb": mem_stats_excl["peak_mb"],
            }

            # Test complete pipeline
            self.logger.info("Testing complete filtering pipeline...")

            def complete_pipeline():
                df = self.tse_manager.load_tse_stock_data()
                tradable = self.tse_manager.filter_tradable_stocks(df)
                final = self.tse_manager.exclude_investment_products(tradable)
                return self.tse_manager.get_stock_codes_with_suffix(final)

            stocks, pipeline_time, _, _, mem_stats_pipeline = (
                self.measure_execution_time(complete_pipeline)
            )

            self.logger.info(f"Complete pipeline time: {pipeline_time:.3f} seconds")
            self.logger.info(f"Final stocks: {len(stocks):,}")
            self.logger.info(
                f"Overall rate: {initial_records/pipeline_time:.0f} records/second"
            )

            results["complete_pipeline"] = {
                "time": pipeline_time,
                "input_records": initial_records,
                "output_stocks": len(stocks),
                "overall_rate": initial_records / pipeline_time,
                "memory_peak_mb": mem_stats_pipeline["peak_mb"],
            }

        except Exception as e:
            self.logger.error(f"Stock filtering performance test failed: {e}")
            results["error"] = str(e)

        self.results["filtering_performance"] = results
        return results

    def test_fallback_vs_tse_performance(self) -> Dict[str, Any]:
        """Compare TSE official data vs fallback range-based validation performance."""
        self.logger.info("=" * 60)
        self.logger.info("PERFORMANCE TEST 3: TSE vs Fallback Comparison")
        self.logger.info("=" * 60)

        results = {}

        try:
            # Test TSE official method
            self.logger.info("Testing TSE official method...")

            tse_stocks, tse_time, _, _, tse_mem_stats = self.measure_execution_time(
                self.tse_manager.get_all_tradable_stocks
            )

            self.logger.info(f"TSE official time: {tse_time:.3f} seconds")
            self.logger.info(f"TSE official stocks: {len(tse_stocks):,}")
            self.logger.info(f"TSE memory usage: {tse_mem_stats['peak_mb']:.2f} MB")

            results["tse_official"] = {
                "time": tse_time,
                "stock_count": len(tse_stocks),
                "memory_peak_mb": tse_mem_stats["peak_mb"],
                "memory_increase_mb": tse_mem_stats["rss_increase_mb"],
            }

            # Test fallback method
            self.logger.info("Testing fallback range-based method...")

            fallback_stocks, fallback_time, _, _, fallback_mem_stats = (
                self.measure_execution_time(self.tse_manager.get_fallback_stock_list)
            )

            self.logger.info(f"Fallback time: {fallback_time:.3f} seconds")
            self.logger.info(f"Fallback stocks: {len(fallback_stocks):,}")
            self.logger.info(
                f"Fallback memory usage: {fallback_mem_stats['peak_mb']:.2f} MB"
            )

            results["fallback"] = {
                "time": fallback_time,
                "stock_count": len(fallback_stocks),
                "memory_peak_mb": fallback_mem_stats["peak_mb"],
                "memory_increase_mb": fallback_mem_stats["rss_increase_mb"],
            }

            # Compare methods
            time_ratio = tse_time / fallback_time
            memory_ratio = tse_mem_stats["peak_mb"] / fallback_mem_stats["peak_mb"]
            accuracy_ratio = len(tse_stocks) / len(fallback_stocks)

            self.logger.info("Comparison results:")
            self.logger.info(f"Time ratio (TSE/Fallback): {time_ratio:.2f}x")
            self.logger.info(f"Memory ratio (TSE/Fallback): {memory_ratio:.2f}x")
            self.logger.info(f"Stock count ratio (TSE/Fallback): {accuracy_ratio:.2f}x")

            if time_ratio < 2.0:
                self.logger.info(
                    "✅ TSE method is reasonably fast compared to fallback"
                )
            else:
                self.logger.warning(
                    "⚠️ TSE method is significantly slower than fallback"
                )

            if memory_ratio < 3.0:
                self.logger.info("✅ TSE method has reasonable memory usage")
            else:
                self.logger.warning("⚠️ TSE method uses significantly more memory")

            results["comparison"] = {
                "time_ratio": time_ratio,
                "memory_ratio": memory_ratio,
                "accuracy_ratio": accuracy_ratio,
                "tse_reasonably_fast": time_ratio < 2.0,
                "tse_reasonable_memory": memory_ratio < 3.0,
            }

            # Test DataFetcher integration performance
            self.logger.info("Testing DataFetcher integration performance...")

            # TSE mode
            df_tse_stocks, df_tse_time, _, _, df_tse_mem = self.measure_execution_time(
                self.data_fetcher.get_japanese_stock_list, "tse_official"
            )

            # All mode (fallback-based)
            df_all_stocks, df_all_time, _, _, df_all_mem = self.measure_execution_time(
                self.data_fetcher.get_japanese_stock_list, "all"
            )

            # Curated mode
            df_curated_stocks, df_curated_time, _, _, df_curated_mem = (
                self.measure_execution_time(
                    self.data_fetcher.get_japanese_stock_list, "curated"
                )
            )

            self.logger.info("DataFetcher mode comparison:")
            self.logger.info(
                f"TSE Official: {len(df_tse_stocks):,} stocks in {df_tse_time:.3f}s"
            )
            self.logger.info(
                f"All (fallback): {len(df_all_stocks):,} stocks in {df_all_time:.3f}s"
            )
            self.logger.info(
                f"Curated: {len(df_curated_stocks):,} stocks in {df_curated_time:.3f}s"
            )

            results["datafetcher_modes"] = {
                "tse_official": {
                    "time": df_tse_time,
                    "stock_count": len(df_tse_stocks),
                    "memory_peak_mb": df_tse_mem["peak_mb"],
                },
                "all_fallback": {
                    "time": df_all_time,
                    "stock_count": len(df_all_stocks),
                    "memory_peak_mb": df_all_mem["peak_mb"],
                },
                "curated": {
                    "time": df_curated_time,
                    "stock_count": len(df_curated_stocks),
                    "memory_peak_mb": df_curated_mem["peak_mb"],
                },
            }

        except Exception as e:
            self.logger.error(f"TSE vs Fallback performance test failed: {e}")
            results["error"] = str(e)

        self.results["tse_vs_fallback"] = results
        return results

    def test_memory_usage_patterns(self) -> Dict[str, Any]:
        """Test memory usage patterns and potential memory leaks."""
        self.logger.info("=" * 60)
        self.logger.info("PERFORMANCE TEST 4: Memory Usage Patterns")
        self.logger.info("=" * 60)

        results = {}

        try:
            # Baseline memory
            gc.collect()
            baseline_memory = self.get_memory_usage()
            self.logger.info(f"Baseline memory: {baseline_memory['rss_mb']:.2f} MB RSS")

            # Test repeated loading
            self.logger.info("Testing repeated loading (10 iterations)...")
            memory_samples = []

            for i in range(10):
                self.tse_manager.invalidate_cache()
                df = self.tse_manager.load_tse_stock_data()
                stocks = self.tse_manager.get_all_tradable_stocks()

                current_memory = self.get_memory_usage()
                memory_samples.append(current_memory["rss_mb"])

                if i % 3 == 0:  # Log every 3rd iteration
                    self.logger.info(
                        f"Iteration {i+1}: {current_memory['rss_mb']:.2f} MB RSS"
                    )

                # Force cleanup
                del df, stocks
                gc.collect()

            # Analyze memory pattern
            max_memory = max(memory_samples)
            min_memory = min(memory_samples)
            avg_memory = sum(memory_samples) / len(memory_samples)
            memory_growth = memory_samples[-1] - memory_samples[0]

            self.logger.info(f"Memory analysis:")
            self.logger.info(f"  Baseline: {baseline_memory['rss_mb']:.2f} MB")
            self.logger.info(f"  Average: {avg_memory:.2f} MB")
            self.logger.info(f"  Min/Max: {min_memory:.2f} MB / {max_memory:.2f} MB")
            self.logger.info(f"  Growth: {memory_growth:.2f} MB")
            self.logger.info(
                f"  Peak increase: {max_memory - baseline_memory['rss_mb']:.2f} MB"
            )

            # Check for memory leaks
            memory_leak_threshold = 50  # MB
            has_memory_leak = memory_growth > memory_leak_threshold

            if has_memory_leak:
                self.logger.warning(
                    f"⚠️ Potential memory leak detected: {memory_growth:.2f} MB growth"
                )
            else:
                self.logger.info(
                    f"✅ No significant memory leak: {memory_growth:.2f} MB growth"
                )

            results["repeated_loading"] = {
                "baseline_mb": baseline_memory["rss_mb"],
                "average_mb": avg_memory,
                "min_mb": min_memory,
                "max_mb": max_memory,
                "memory_growth_mb": memory_growth,
                "peak_increase_mb": max_memory - baseline_memory["rss_mb"],
                "has_memory_leak": has_memory_leak,
                "memory_samples": memory_samples,
            }

            # Test cache efficiency
            self.logger.info("Testing cache efficiency...")

            # First load (cold)
            self.tse_manager.invalidate_cache()
            start_mem = self.get_memory_usage()
            df1 = self.tse_manager.load_tse_stock_data()
            after_load_mem = self.get_memory_usage()

            # Second load (cached)
            df2 = self.tse_manager.load_tse_stock_data()
            after_cached_mem = self.get_memory_usage()

            cache_memory_increase = (
                after_cached_mem["rss_mb"] - after_load_mem["rss_mb"]
            )

            self.logger.info(f"Cache memory efficiency:")
            self.logger.info(
                f"  Memory after first load: {after_load_mem['rss_mb']:.2f} MB"
            )
            self.logger.info(
                f"  Memory after cached load: {after_cached_mem['rss_mb']:.2f} MB"
            )
            self.logger.info(f"  Cache overhead: {cache_memory_increase:.2f} MB")

            results["cache_efficiency"] = {
                "first_load_mb": after_load_mem["rss_mb"],
                "cached_load_mb": after_cached_mem["rss_mb"],
                "cache_overhead_mb": cache_memory_increase,
            }

        except Exception as e:
            self.logger.error(f"Memory usage patterns test failed: {e}")
            results["error"] = str(e)

        self.results["memory_patterns"] = results
        return results

    def test_scalability_performance(self) -> Dict[str, Any]:
        """Test performance scalability with different data sizes."""
        self.logger.info("=" * 60)
        self.logger.info("PERFORMANCE TEST 5: Scalability")
        self.logger.info("=" * 60)

        results = {}

        try:
            # Load full dataset
            df = self.tse_manager.load_tse_stock_data()
            full_size = len(df)

            # Test with different data sizes
            test_sizes = [
                int(full_size * 0.1),  # 10%
                int(full_size * 0.25),  # 25%
                int(full_size * 0.5),  # 50%
                int(full_size * 0.75),  # 75%
                full_size,  # 100%
            ]

            scalability_results = []

            for size in test_sizes:
                self.logger.info(
                    f"Testing with {size:,} records ({size/full_size*100:.0f}%)..."
                )

                # Create subset
                subset_df = df.head(size)

                # Test filtering performance
                start_time = time.time()
                tradable = self.tse_manager.filter_tradable_stocks(subset_df)
                final = self.tse_manager.exclude_investment_products(tradable)
                stocks = self.tse_manager.get_stock_codes_with_suffix(final)
                end_time = time.time()

                processing_time = end_time - start_time
                records_per_second = (
                    size / processing_time if processing_time > 0 else 0
                )

                scalability_results.append(
                    {
                        "input_size": size,
                        "percentage": size / full_size,
                        "processing_time": processing_time,
                        "records_per_second": records_per_second,
                        "output_stocks": len(stocks),
                    }
                )

                self.logger.info(
                    f"  Time: {processing_time:.3f}s, Rate: {records_per_second:.0f} rec/s"
                )

            # Analyze scalability
            times = [r["processing_time"] for r in scalability_results]
            sizes = [r["input_size"] for r in scalability_results]

            # Check if scaling is linear (O(n))
            # Calculate correlation between size and time
            n = len(sizes)
            sum_x = sum(sizes)
            sum_y = sum(times)
            sum_xy = sum(x * y for x, y in zip(sizes, times))
            sum_x2 = sum(x * x for x in sizes)

            correlation = (n * sum_xy - sum_x * sum_y) / (
                (n * sum_x2 - sum_x * sum_x)
                * (n * sum(y * y for y in times) - sum_y * sum_y)
            ) ** 0.5

            self.logger.info(f"Scalability analysis:")
            self.logger.info(f"  Size-Time correlation: {correlation:.3f}")

            if correlation > 0.95:
                self.logger.info("✅ Excellent linear scalability")
            elif correlation > 0.85:
                self.logger.info("✅ Good scalability")
            else:
                self.logger.warning(
                    "⚠️ Poor scalability - may have performance issues with large datasets"
                )

            results["scalability_test"] = {
                "full_dataset_size": full_size,
                "test_results": scalability_results,
                "size_time_correlation": correlation,
                "scalability_rating": (
                    "excellent"
                    if correlation > 0.95
                    else "good" if correlation > 0.85 else "poor"
                ),
            }

        except Exception as e:
            self.logger.error(f"Scalability performance test failed: {e}")
            results["error"] = str(e)

        self.results["scalability"] = results
        return results

    def run_all_performance_tests(self) -> Dict[str, Any]:
        """Run all performance tests."""
        self.logger.info("🚀 Starting TSE Performance Tests")
        self.logger.info("=" * 80)

        start_time = time.time()

        # System information
        self.logger.info("System Information:")
        self.logger.info(f"  CPU Count: {psutil.cpu_count()}")
        self.logger.info(
            f"  Memory Total: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.1f} GB"
        )
        self.logger.info(f"  Python Version: {sys.version}")
        self.logger.info("")

        # Run all performance tests
        tests = [
            ("TSE Data Loading Performance", self.test_tse_data_loading_performance),
            ("Stock Filtering Performance", self.test_stock_filtering_performance),
            ("TSE vs Fallback Comparison", self.test_fallback_vs_tse_performance),
            ("Memory Usage Patterns", self.test_memory_usage_patterns),
            ("Scalability Performance", self.test_scalability_performance),
        ]

        for test_name, test_func in tests:
            try:
                self.logger.info(f"Running {test_name}...")
                test_func()
                self.logger.info(f"✅ {test_name}: COMPLETED")
            except Exception as e:
                self.logger.error(f"❌ {test_name}: FAILED - {e}")

            self.logger.info("")  # Add spacing between tests

        total_time = time.time() - start_time

        # Summary
        self.logger.info("=" * 80)
        self.logger.info("🏁 TSE Performance Test Summary")
        self.logger.info("=" * 80)
        self.logger.info(f"Total test time: {total_time:.2f} seconds")

        # Generate performance summary
        summary = self.generate_performance_summary()
        self.logger.info(summary)

        self.results["test_summary"] = {
            "total_time": total_time,
            "completed_tests": len(
                [
                    t
                    for t in self.results.values()
                    if not isinstance(t, dict) or "error" not in t
                ]
            ),
        }

        return self.results

    def generate_performance_summary(self) -> str:
        """Generate a performance summary."""
        summary = []
        summary.append("Performance Summary:")

        # TSE Loading Performance
        if "tse_loading_performance" in self.results:
            tse_perf = self.results["tse_loading_performance"]
            if "cold_load" in tse_perf:
                cold_time = tse_perf["cold_load"]["time"]
                records = tse_perf["cold_load"]["records"]
                summary.append(
                    f"  TSE Data Loading: {cold_time:.3f}s for {records:,} records"
                )

            if "warm_load" in tse_perf:
                warm_time = tse_perf["warm_load"]["time"]
                improvement = tse_perf["warm_load"]["speed_improvement"]
                summary.append(
                    f"  Cached Loading: {warm_time:.3f}s ({improvement:.1f}x faster)"
                )

        # Filtering Performance
        if "filtering_performance" in self.results:
            filter_perf = self.results["filtering_performance"]
            if "complete_pipeline" in filter_perf:
                pipeline_time = filter_perf["complete_pipeline"]["time"]
                input_records = filter_perf["complete_pipeline"]["input_records"]
                output_stocks = filter_perf["complete_pipeline"]["output_stocks"]
                rate = filter_perf["complete_pipeline"]["overall_rate"]
                summary.append(
                    f"  Complete Pipeline: {pipeline_time:.3f}s ({input_records:,} → {output_stocks:,} stocks)"
                )
                summary.append(f"  Processing Rate: {rate:.0f} records/second")

        # Memory Usage
        if "memory_patterns" in self.results:
            mem_patterns = self.results["memory_patterns"]
            if "repeated_loading" in mem_patterns:
                peak_increase = mem_patterns["repeated_loading"]["peak_increase_mb"]
                has_leak = mem_patterns["repeated_loading"]["has_memory_leak"]
                summary.append(f"  Peak Memory Usage: +{peak_increase:.1f} MB")
                summary.append(f"  Memory Leak: {'Detected' if has_leak else 'None'}")

        # TSE vs Fallback
        if "tse_vs_fallback" in self.results:
            comparison = self.results["tse_vs_fallback"]
            if "comparison" in comparison:
                time_ratio = comparison["comparison"]["time_ratio"]
                memory_ratio = comparison["comparison"]["memory_ratio"]
                accuracy_ratio = comparison["comparison"]["accuracy_ratio"]
                summary.append(
                    f"  TSE vs Fallback: {time_ratio:.1f}x time, {memory_ratio:.1f}x memory, {accuracy_ratio:.1f}x accuracy"
                )

        return "\n".join(summary)

    def generate_performance_report(self) -> str:
        """Generate a detailed performance report."""
        if not self.results:
            return "No performance test results available"

        report = []
        report.append("# TSE Performance Test Report")
        report.append("=" * 50)
        report.append("")

        # System info
        report.append("## System Information")
        report.append(f"- CPU Count: {psutil.cpu_count()}")
        report.append(
            f"- Memory Total: {psutil.virtual_memory().total / 1024 / 1024 / 1024:.1f} GB"
        )
        report.append(f"- Python Version: {sys.version}")
        report.append("")

        # Performance summary
        summary = self.generate_performance_summary()
        report.append("## Performance Summary")
        report.append(summary)
        report.append("")

        # Detailed results
        for test_name, result in self.results.items():
            if test_name == "test_summary":
                continue

            report.append(f"## {test_name.replace('_', ' ').title()}")

            if isinstance(result, dict) and "error" not in result:
                for key, value in result.items():
                    if isinstance(value, dict):
                        report.append(f"### {key.replace('_', ' ').title()}")
                        for sub_key, sub_value in value.items():
                            if isinstance(sub_value, (int, float)):
                                if "time" in sub_key.lower():
                                    report.append(f"- {sub_key}: {sub_value:.3f}s")
                                elif "mb" in sub_key.lower():
                                    report.append(f"- {sub_key}: {sub_value:.2f} MB")
                                elif "rate" in sub_key.lower():
                                    report.append(f"- {sub_key}: {sub_value:.0f}")
                                else:
                                    report.append(f"- {sub_key}: {sub_value}")
                            else:
                                report.append(f"- {sub_key}: {sub_value}")
                    else:
                        report.append(f"- {key}: {value}")
            elif isinstance(result, dict) and "error" in result:
                report.append(f"**Error**: {result['error']}")

            report.append("")

        return "\n".join(report)


def main():
    """Main function to run TSE performance tests."""
    # Ensure logs directory exists
    os.makedirs("logs", exist_ok=True)

    # Create and run performance tester
    tester = TSEPerformanceTester()
    results = tester.run_all_performance_tests()

    # Generate and save report
    report = tester.generate_performance_report()

    # Save report to file
    with open("logs/tse_performance_report.md", "w", encoding="utf-8") as f:
        f.write(report)

    print("\n" + "=" * 80)
    print("📊 Performance report saved to: logs/tse_performance_report.md")
    print("📋 Detailed logs saved to: logs/tse_performance_test.log")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
