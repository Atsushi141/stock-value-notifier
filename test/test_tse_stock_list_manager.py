"""Tests for TSE Stock List Manager."""

import pytest
import logging
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime

from src.tse_stock_list_manager import TSEStockListManager
from src.models import TSEDataConfig


class TestTSEStockListManager:
    """Test cases for TSE Stock List Manager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = TSEDataConfig(data_file_path="stock_list/data_j.xls")
        self.logger = logging.getLogger("test")
        self.manager = TSEStockListManager(self.config, self.logger)

    def test_initialization(self):
        """Test TSE Stock List Manager initialization."""
        assert self.manager.config is not None
        assert self.manager.logger is not None
        assert self.manager._cached_data is None
        assert self.manager._cache_timestamp is None

    def test_load_tse_stock_data_success(self):
        """Test successful TSE data loading."""
        # This test requires the actual data file to exist
        try:
            df = self.manager.load_tse_stock_data()
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert "コード" in df.columns
            assert "銘柄名" in df.columns
            assert "市場・商品区分" in df.columns
        except FileNotFoundError:
            pytest.skip("TSE data file not found")

    def test_filter_tradable_stocks(self):
        """Test filtering tradable stocks."""
        # Create sample data
        sample_data = pd.DataFrame(
            {
                "コード": ["1301", "1305", "ABCD", "1333"],
                "銘柄名": ["極洋", "ETF", "Invalid", "マルハニチロ"],
                "市場・商品区分": [
                    "プライム（内国株式）",
                    "ETF・ETN",
                    "プライム（内国株式）",
                    "プライム（内国株式）",
                ],
            }
        )

        filtered = self.manager.filter_tradable_stocks(sample_data)

        # Should exclude non-numeric codes
        assert len(filtered) == 3
        assert "ABCD" not in filtered["コード"].values

    def test_exclude_investment_products(self):
        """Test excluding investment products."""
        # Create sample data with investment products
        sample_data = pd.DataFrame(
            {
                "コード": ["1301", "1305", "1306", "1333"],
                "銘柄名": ["極洋", "ETF", "REIT", "マルハニチロ"],
                "市場・商品区分": [
                    "プライム（内国株式）",
                    "ETF・ETN",
                    "REIT・ベンチャーファンド・カントリーファンド・インフラファンド",
                    "プライム（内国株式）",
                ],
            }
        )

        filtered = self.manager.exclude_investment_products(sample_data)

        # Should exclude ETF and REIT
        assert len(filtered) == 2
        assert "1305" not in filtered["コード"].values
        assert "1306" not in filtered["コード"].values

    def test_get_stock_codes_with_suffix(self):
        """Test getting stock codes with .T suffix."""
        sample_data = pd.DataFrame(
            {
                "コード": ["1301", "1333", "7203"],
                "銘柄名": ["極洋", "マルハニチロ", "トヨタ"],
                "市場・商品区分": [
                    "プライム（内国株式）",
                    "プライム（内国株式）",
                    "プライム（内国株式）",
                ],
            }
        )

        codes = self.manager.get_stock_codes_with_suffix(sample_data)

        assert len(codes) == 3
        assert "1301.T" in codes
        assert "1333.T" in codes
        assert "7203.T" in codes

    def test_is_investment_product(self):
        """Test investment product detection."""
        # Test ETF
        etf_info = {"market_category": "ETF・ETN", "name": "TOPIX ETF"}
        assert self.manager.is_investment_product(etf_info) is True

        # Test REIT
        reit_info = {
            "market_category": "REIT・ベンチャーファンド・カントリーファンド・インフラファンド",
            "name": "Japan REIT",
        }
        assert self.manager.is_investment_product(reit_info) is True

        # Test regular stock
        stock_info = {"market_category": "プライム（内国株式）", "name": "トヨタ自動車"}
        assert self.manager.is_investment_product(stock_info) is False

        # Test name-based detection
        fund_info = {
            "market_category": "プライム（内国株式）",
            "name": "インデックスファンド",
        }
        assert self.manager.is_investment_product(fund_info) is True

    def test_cache_functionality(self):
        """Test caching functionality."""
        # Test cache validation
        assert self.manager._is_cache_valid() is False

        # Test cache invalidation
        self.manager._cached_data = pd.DataFrame()
        self.manager._cache_timestamp = datetime.now()
        assert self.manager._is_cache_valid() is True

        self.manager.invalidate_cache()
        assert self.manager._cached_data is None
        assert self.manager._cache_timestamp is None

    def test_fallback_stock_list(self):
        """Test fallback stock list generation."""
        fallback_stocks = self.manager.get_fallback_stock_list()

        assert isinstance(fallback_stocks, list)
        assert len(fallback_stocks) > 0
        assert all(stock.endswith(".T") for stock in fallback_stocks)
        assert "1000.T" in fallback_stocks
        assert "9999.T" in fallback_stocks

    @patch("os.path.exists")
    def test_get_stocks_with_fallback_file_not_found(self, mock_exists):
        """Test fallback when TSE file doesn't exist."""
        mock_exists.return_value = False

        # Should use fallback
        stocks = self.manager.get_stocks_with_fallback()
        assert isinstance(stocks, list)
        assert len(stocks) > 0
        assert all(stock.endswith(".T") for stock in stocks)

    def test_data_integrity_validation(self):
        """Test data integrity validation."""
        # This test requires the actual data file
        try:
            validation_results = self.manager.validate_data_integrity()
            assert "total_records" in validation_results
            assert "is_valid" in validation_results
            assert "validation_timestamp" in validation_results
            assert isinstance(validation_results["issues"], list)
        except FileNotFoundError:
            pytest.skip("TSE data file not found")


if __name__ == "__main__":
    pytest.main([__file__])
