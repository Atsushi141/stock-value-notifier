"""
Cache idempotency tests for a single stock symbol.

These tests verify that cache operations are idempotent - performing the same
operation multiple times produces the same result as performing it once.

**Feature: stock-value-notifier, Property: Cache Idempotency**
**Validates: Cache operations maintain consistency across repeated executions**
"""

import json
import os
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import pytest

# Add src to path for imports
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from cache_manager import CacheManager


class TestCacheIdempotency:
    """Test cache operations for idempotent behavior using a single test stock."""

    def setup_method(self):
        """Set up test environment with temporary cache directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_manager = CacheManager(cache_dir=self.temp_dir)

        # Test data for a single stock (Toyota: 7203.T)
        self.test_symbol = "7203.T"
        self.test_financial_data = {
            "symbol": "7203.T",
            "shortName": "Toyota Motor Corporation",
            "forwardPE": 12.5,
            "priceToBook": 1.2,
            "dividendYield": 0.025,
            "marketCap": 25000000000,
            "totalRevenue": 31000000000,
            "netIncome": 2800000000,
        }

        # Test dividend data
        self.test_dividend_data = pd.DataFrame(
            [
                {"Date": datetime(2021, 6, 30), "Dividends": 120.0, "Symbol": "7203.T"},
                {"Date": datetime(2022, 6, 30), "Dividends": 125.0, "Symbol": "7203.T"},
                {"Date": datetime(2023, 6, 30), "Dividends": 130.0, "Symbol": "7203.T"},
            ]
        )

    def teardown_method(self):
        """Clean up temporary cache directory."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_financial_cache_write_idempotency(self):
        """
        Property: Caching the same financial data multiple times produces identical cache state.

        Tests that caching Toyota (7203.T) financial data multiple times results in
        the same cache state, demonstrating idempotent behavior.
        """
        print(f"\n=== 財務データキャッシュ書き込み冪等性テスト ===")
        print(f"テスト銘柄: {self.test_symbol}")
        print(f"財務データ項目数: {len(self.test_financial_data)}")
        print(f"財務データ内容:")
        for key, value in self.test_financial_data.items():
            print(f"  - {key}: {value}")

        # Cache data once
        print(f"\n1回目のキャッシュ書き込み...")
        self.cache_manager.cache_financial_info(
            self.test_symbol, self.test_financial_data
        )
        first_cache_state = self._get_financial_cache_content()
        first_normalized = self._normalize_cache_data(first_cache_state)
        print(f"✓ キャッシュされた銘柄数: {len(first_cache_state)}")

        # Cache the same data again
        print(f"\n2回目のキャッシュ書き込み（同じデータ）...")
        self.cache_manager.cache_financial_info(
            self.test_symbol, self.test_financial_data
        )
        second_cache_state = self._get_financial_cache_content()
        second_normalized = self._normalize_cache_data(second_cache_state)
        print(f"✓ キャッシュされた銘柄数: {len(second_cache_state)}")

        # Cache it a third time
        print(f"\n3回目のキャッシュ書き込み（同じデータ）...")
        self.cache_manager.cache_financial_info(
            self.test_symbol, self.test_financial_data
        )
        third_cache_state = self._get_financial_cache_content()
        third_normalized = self._normalize_cache_data(third_cache_state)
        print(f"✓ キャッシュされた銘柄数: {len(third_cache_state)}")

        # Verify idempotency
        print(f"\n冪等性検証:")
        print(f"✓ 1回目と2回目のデータ一致: {first_normalized == second_normalized}")
        print(f"✓ 2回目と3回目のデータ一致: {second_normalized == third_normalized}")
        print(
            f"✓ 全てのキャッシュ状態が同一: {first_normalized == second_normalized == third_normalized}"
        )

        # All cache states should be identical (ignoring timestamps)
        assert first_normalized == second_normalized
        assert second_normalized == third_normalized

    def test_dividend_cache_write_idempotency(self):
        """
        Property: Caching the same dividend data multiple times produces identical cache state.

        Tests that caching Toyota (7203.T) dividend data multiple times results in
        the same cache state, demonstrating idempotent behavior.
        """
        print(f"\n=== 配当データキャッシュ書き込み冪等性テスト ===")
        print(f"テスト銘柄: {self.test_symbol}")
        print(f"配当データ件数: {len(self.test_dividend_data)}")
        print(f"配当データ内容:")
        for _, row in self.test_dividend_data.iterrows():
            print(f"  - {row['Date'].strftime('%Y-%m-%d')}: {row['Dividends']}円")

        # Cache data once
        print(f"\n1回目のキャッシュ書き込み...")
        self.cache_manager.cache_dividend_history(
            self.test_symbol, self.test_dividend_data
        )
        first_cache_state = self._get_dividend_cache_content()
        first_normalized = self._normalize_cache_data(first_cache_state)
        print(f"✓ キャッシュされた銘柄数: {len(first_cache_state)}")

        # Cache the same data again
        print(f"\n2回目のキャッシュ書き込み（同じデータ）...")
        self.cache_manager.cache_dividend_history(
            self.test_symbol, self.test_dividend_data
        )
        second_cache_state = self._get_dividend_cache_content()
        second_normalized = self._normalize_cache_data(second_cache_state)
        print(f"✓ キャッシュされた銘柄数: {len(second_cache_state)}")

        # Cache it a third time
        print(f"\n3回目のキャッシュ書き込み（同じデータ）...")
        self.cache_manager.cache_dividend_history(
            self.test_symbol, self.test_dividend_data
        )
        third_cache_state = self._get_dividend_cache_content()
        third_normalized = self._normalize_cache_data(third_cache_state)
        print(f"✓ キャッシュされた銘柄数: {len(third_cache_state)}")

        # Verify idempotency
        print(f"\n冪等性検証:")
        print(f"✓ 1回目と2回目のデータ一致: {first_normalized == second_normalized}")
        print(f"✓ 2回目と3回目のデータ一致: {second_normalized == third_normalized}")
        print(
            f"✓ 全てのキャッシュ状態が同一: {first_normalized == second_normalized == third_normalized}"
        )

        # All cache states should be identical (ignoring timestamps)
        assert first_normalized == second_normalized
        assert second_normalized == third_normalized

    def test_financial_cache_read_idempotency(self):
        """
        Property: Reading cached financial data multiple times returns identical results.

        Tests that reading Toyota (7203.T) financial data multiple times returns
        the same data without modifying the cache state.
        """
        print(f"\n=== 財務データキャッシュ読み込み冪等性テスト ===")
        print(f"テスト銘柄: {self.test_symbol}")

        # Cache the data first
        print(f"\n初期データをキャッシュ...")
        self.cache_manager.cache_financial_info(
            self.test_symbol, self.test_financial_data
        )
        initial_cache_state = self._get_financial_cache_content()
        print(f"✓ 初期キャッシュ銘柄数: {len(initial_cache_state)}")

        # Read the data multiple times
        print(f"\n1回目の読み込み...")
        first_read = self.cache_manager.get_cached_financial_info(self.test_symbol)
        print(f"✓ 読み込み成功: {first_read is not None}")
        if first_read:
            print(f"✓ 読み込みデータ項目数: {len(first_read)}")

        print(f"\n2回目の読み込み...")
        second_read = self.cache_manager.get_cached_financial_info(self.test_symbol)
        print(f"✓ 読み込み成功: {second_read is not None}")
        if second_read:
            print(f"✓ 読み込みデータ項目数: {len(second_read)}")

        print(f"\n3回目の読み込み...")
        third_read = self.cache_manager.get_cached_financial_info(self.test_symbol)
        print(f"✓ 読み込み成功: {third_read is not None}")
        if third_read:
            print(f"✓ 読み込みデータ項目数: {len(third_read)}")

        final_cache_state = self._get_financial_cache_content()

        # Verify idempotency
        print(f"\n冪等性検証:")
        print(f"✓ 1回目と2回目のデータ一致: {first_read == second_read}")
        print(f"✓ 2回目と3回目のデータ一致: {second_read == third_read}")
        print(f"✓ キャッシュ状態不変: {initial_cache_state == final_cache_state}")

        # All reads should return the same data
        assert first_read == second_read
        assert second_read == third_read

        # Cache state should remain unchanged
        assert initial_cache_state == final_cache_state

    def test_dividend_cache_read_idempotency(self):
        """
        Property: Reading cached dividend data multiple times returns identical results.

        Tests that reading Toyota (7203.T) dividend data multiple times returns
        equivalent DataFrames without modifying the cache state.
        """
        print(f"\n=== 配当データキャッシュ読み込み冪等性テスト ===")
        print(f"テスト銘柄: {self.test_symbol}")

        # Cache the data first
        print(f"\n初期データをキャッシュ...")
        self.cache_manager.cache_dividend_history(
            self.test_symbol, self.test_dividend_data
        )
        initial_cache_state = self._get_dividend_cache_content()
        print(f"✓ 初期キャッシュ銘柄数: {len(initial_cache_state)}")

        # Read the data multiple times
        print(f"\n1回目の読み込み...")
        first_read = self.cache_manager.get_cached_dividend_history(self.test_symbol)
        print(f"✓ 読み込み成功: {first_read is not None}")
        if first_read is not None:
            print(f"✓ 読み込み配当データ件数: {len(first_read)}")

        print(f"\n2回目の読み込み...")
        second_read = self.cache_manager.get_cached_dividend_history(self.test_symbol)
        print(f"✓ 読み込み成功: {second_read is not None}")
        if second_read is not None:
            print(f"✓ 読み込み配当データ件数: {len(second_read)}")

        print(f"\n3回目の読み込み...")
        third_read = self.cache_manager.get_cached_dividend_history(self.test_symbol)
        print(f"✓ 読み込み成功: {third_read is not None}")
        if third_read is not None:
            print(f"✓ 読み込み配当データ件数: {len(third_read)}")

        final_cache_state = self._get_dividend_cache_content()

        # Verify idempotency
        print(f"\n冪等性検証:")
        if (
            first_read is not None
            and second_read is not None
            and third_read is not None
        ):
            try:
                pd.testing.assert_frame_equal(first_read, second_read)
                pd.testing.assert_frame_equal(second_read, third_read)
                print(f"✓ 全ての読み込みデータが同一")
            except AssertionError as e:
                print(f"✗ データ不一致: {e}")
                raise
        else:
            print(f"✓ 全ての読み込みがNone（一致）")
            assert first_read is None and second_read is None and third_read is None

        print(f"✓ キャッシュ状態不変: {initial_cache_state == final_cache_state}")

        # Cache state should remain unchanged
        assert initial_cache_state == final_cache_state

    def test_cache_stats_read_idempotency(self):
        """
        Property: Getting cache statistics multiple times returns identical results.

        Tests that getting cache statistics for Toyota (7203.T) data multiple times
        returns the same statistics without modifying the cache.
        """
        print(f"\n=== キャッシュ統計読み込み冪等性テスト ===")
        print(f"テスト銘柄: {self.test_symbol}")

        # Populate cache with test data
        print(f"\nテストデータをキャッシュ...")
        self.cache_manager.cache_financial_info(
            self.test_symbol, self.test_financial_data
        )
        self.cache_manager.cache_dividend_history(
            self.test_symbol, self.test_dividend_data
        )

        initial_cache_state = self._get_all_cache_content()
        print(f"✓ 初期キャッシュ準備完了")

        # Get stats multiple times
        print(f"\n1回目の統計取得...")
        first_stats = self.cache_manager.get_cache_stats()
        print(f"✓ 財務情報キャッシュサイズ: {first_stats['financial_cache_size']}")
        print(f"✓ 配当履歴キャッシュサイズ: {first_stats['dividend_cache_size']}")
        print(f"✓ 総キャッシュサイズ: {first_stats['total_cache_size_mb']} MB")

        print(f"\n2回目の統計取得...")
        second_stats = self.cache_manager.get_cache_stats()
        print(f"✓ 財務情報キャッシュサイズ: {second_stats['financial_cache_size']}")
        print(f"✓ 配当履歴キャッシュサイズ: {second_stats['dividend_cache_size']}")
        print(f"✓ 総キャッシュサイズ: {second_stats['total_cache_size_mb']} MB")

        print(f"\n3回目の統計取得...")
        third_stats = self.cache_manager.get_cache_stats()
        print(f"✓ 財務情報キャッシュサイズ: {third_stats['financial_cache_size']}")
        print(f"✓ 配当履歴キャッシュサイズ: {third_stats['dividend_cache_size']}")
        print(f"✓ 総キャッシュサイズ: {third_stats['total_cache_size_mb']} MB")

        final_cache_state = self._get_all_cache_content()

        # Verify idempotency
        print(f"\n冪等性検証:")
        print(f"✓ 1回目と2回目の統計一致: {first_stats == second_stats}")
        print(f"✓ 2回目と3回目の統計一致: {second_stats == third_stats}")
        print(f"✓ キャッシュ状態不変: {initial_cache_state == final_cache_state}")

        # All stats should be identical
        assert first_stats == second_stats
        assert second_stats == third_stats

        # Cache state should remain unchanged
        assert initial_cache_state == final_cache_state

    def test_cleanup_idempotency(self):
        """
        Property: Running cache cleanup multiple times produces the same result.

        Tests that running cache cleanup on Toyota (7203.T) data multiple times
        produces the same final state.
        """
        print(f"\n=== キャッシュクリーンアップ冪等性テスト ===")
        print(f"テスト銘柄: {self.test_symbol}")

        # Cache some data
        print(f"\nテストデータをキャッシュ...")
        self.cache_manager.cache_financial_info(
            self.test_symbol, self.test_financial_data
        )
        initial_state = self._get_all_cache_content()
        print(f"✓ 初期キャッシュ準備完了")
        print(f"✓ 財務データ銘柄数: {len(initial_state['financial'])}")

        # Run cleanup once
        print(f"\n1回目のクリーンアップ...")
        self.cache_manager.cleanup_expired_cache()
        first_cleanup_state = self._get_all_cache_content()
        print(
            f"✓ クリーンアップ後財務データ銘柄数: {len(first_cleanup_state['financial'])}"
        )

        # Run cleanup again
        print(f"\n2回目のクリーンアップ...")
        self.cache_manager.cleanup_expired_cache()
        second_cleanup_state = self._get_all_cache_content()
        print(
            f"✓ クリーンアップ後財務データ銘柄数: {len(second_cleanup_state['financial'])}"
        )

        # Run cleanup a third time
        print(f"\n3回目のクリーンアップ...")
        self.cache_manager.cleanup_expired_cache()
        third_cleanup_state = self._get_all_cache_content()
        print(
            f"✓ クリーンアップ後財務データ銘柄数: {len(third_cleanup_state['financial'])}"
        )

        # Verify idempotency
        print(f"\n冪等性検証:")
        print(
            f"✓ 1回目と2回目のクリーンアップ結果一致: {first_cleanup_state == second_cleanup_state}"
        )
        print(
            f"✓ 2回目と3回目のクリーンアップ結果一致: {second_cleanup_state == third_cleanup_state}"
        )
        print(
            f"✓ 全てのクリーンアップ結果が同一: {first_cleanup_state == second_cleanup_state == third_cleanup_state}"
        )

        # All cleanup states should be identical
        assert first_cleanup_state == second_cleanup_state
        assert second_cleanup_state == third_cleanup_state

    def _get_financial_cache_content(self) -> Dict[str, Any]:
        """Get the current content of the financial cache file."""
        cache_file = self.cache_manager.financial_cache
        if not cache_file.exists():
            return {}

        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _get_dividend_cache_content(self) -> Dict[str, Any]:
        """Get the current content of the dividend cache file."""
        cache_file = self.cache_manager.dividend_cache
        if not cache_file.exists():
            return {}

        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _get_all_cache_content(self) -> Dict[str, Any]:
        """Get the content of all cache files."""
        return {
            "financial": self._get_financial_cache_content(),
            "dividend": self._get_dividend_cache_content(),
        }

    def _normalize_cache_data(self, cache_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize cache data by removing timestamps for comparison.

        This allows us to compare cache content while ignoring timestamp differences
        that occur due to the timing of cache operations.
        """
        normalized = {}
        for symbol, entry in cache_data.items():
            if isinstance(entry, dict) and "data" in entry:
                normalized[symbol] = entry["data"]
            else:
                normalized[symbol] = entry
        return normalized


# Unit tests for specific edge cases
class TestCacheIdempotencyEdgeCases:
    """Test specific edge cases for cache idempotency."""

    def setup_method(self):
        """Set up test environment with temporary cache directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_manager = CacheManager(cache_dir=self.temp_dir)

    def teardown_method(self):
        """Clean up temporary cache directory."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_empty_cache_read_idempotency(self):
        """Test that reading from empty cache is idempotent."""
        print(f"\n=== 空キャッシュ読み込み冪等性テスト ===")
        test_symbol = "NONEXISTENT.T"

        # Read from empty cache multiple times
        print(f"\n存在しない銘柄の読み込みテスト: {test_symbol}")

        print(f"1回目の読み込み...")
        first_read = self.cache_manager.get_cached_financial_info(test_symbol)
        print(f"✓ 読み込み結果: {first_read}")

        print(f"2回目の読み込み...")
        second_read = self.cache_manager.get_cached_financial_info(test_symbol)
        print(f"✓ 読み込み結果: {second_read}")

        print(f"3回目の読み込み...")
        third_read = self.cache_manager.get_cached_financial_info(test_symbol)
        print(f"✓ 読み込み結果: {third_read}")

        print(f"\n冪等性検証:")
        print(
            f"✓ 全ての読み込み結果がNone: {first_read is None and second_read is None and third_read is None}"
        )

        # All reads should return None
        assert first_read is None
        assert second_read is None
        assert third_read is None

    def test_empty_dataframe_cache_idempotency(self):
        """Test that caching empty DataFrames is idempotent."""
        print(f"\n=== 空DataFrame キャッシュ冪等性テスト ===")
        symbol = "TEST.T"
        empty_df = pd.DataFrame(columns=["Date", "Dividends", "Symbol"])

        print(f"テスト銘柄: {symbol}")
        print(f"空DataFrame行数: {len(empty_df)}")
        print(f"DataFrame列: {list(empty_df.columns)}")

        # Cache empty DataFrame multiple times
        print(f"\n1回目の空DataFrame キャッシュ...")
        self.cache_manager.cache_dividend_history(symbol, empty_df)
        first_state = self._get_dividend_cache_content()
        print(f"✓ キャッシュされた銘柄数: {len(first_state)}")

        print(f"\n2回目の空DataFrame キャッシュ...")
        self.cache_manager.cache_dividend_history(symbol, empty_df)
        second_state = self._get_dividend_cache_content()
        print(f"✓ キャッシュされた銘柄数: {len(second_state)}")

        # Verify idempotency
        print(f"\n冪等性検証:")
        normalized_first = self._normalize_cache_data(first_state)
        normalized_second = self._normalize_cache_data(second_state)
        print(f"✓ キャッシュ状態一致: {normalized_first == normalized_second}")

        # States should be identical (ignoring timestamps)
        assert normalized_first == normalized_second

    def test_cache_stats_empty_cache_idempotency(self):
        """Test that getting stats from empty cache is idempotent."""
        print(f"\n=== 空キャッシュ統計冪等性テスト ===")

        # Get stats from empty cache multiple times
        print(f"\n1回目の空キャッシュ統計取得...")
        first_stats = self.cache_manager.get_cache_stats()
        print(f"✓ 財務情報キャッシュサイズ: {first_stats['financial_cache_size']}")
        print(f"✓ 配当履歴キャッシュサイズ: {first_stats['dividend_cache_size']}")
        print(f"✓ 総キャッシュサイズ: {first_stats['total_cache_size_mb']} MB")

        print(f"\n2回目の空キャッシュ統計取得...")
        second_stats = self.cache_manager.get_cache_stats()
        print(f"✓ 財務情報キャッシュサイズ: {second_stats['financial_cache_size']}")
        print(f"✓ 配当履歴キャッシュサイズ: {second_stats['dividend_cache_size']}")
        print(f"✓ 総キャッシュサイズ: {second_stats['total_cache_size_mb']} MB")

        print(f"\n3回目の空キャッシュ統計取得...")
        third_stats = self.cache_manager.get_cache_stats()
        print(f"✓ 財務情報キャッシュサイズ: {third_stats['financial_cache_size']}")
        print(f"✓ 配当履歴キャッシュサイズ: {third_stats['dividend_cache_size']}")
        print(f"✓ 総キャッシュサイズ: {third_stats['total_cache_size_mb']} MB")

        # Verify idempotency
        print(f"\n冪等性検証:")
        print(f"✓ 1回目と2回目の統計一致: {first_stats == second_stats}")
        print(f"✓ 2回目と3回目の統計一致: {second_stats == third_stats}")
        print(f"✓ 空キャッシュ確認 - 財務: {first_stats['financial_cache_size'] == 0}")
        print(f"✓ 空キャッシュ確認 - 配当: {first_stats['dividend_cache_size'] == 0}")

        # All stats should be identical
        assert first_stats == second_stats
        assert second_stats == third_stats

        # Should indicate empty cache
        assert first_stats["financial_cache_size"] == 0
        assert first_stats["dividend_cache_size"] == 0

    def _get_dividend_cache_content(self) -> Dict[str, Any]:
        """Get the current content of the dividend cache file."""
        cache_file = self.cache_manager.dividend_cache
        if not cache_file.exists():
            return {}

        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _normalize_cache_data(self, cache_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize cache data by removing timestamps for comparison."""
        normalized = {}
        for symbol, entry in cache_data.items():
            if isinstance(entry, dict) and "data" in entry:
                normalized[symbol] = entry["data"]
            else:
                normalized[symbol] = entry
        return normalized
