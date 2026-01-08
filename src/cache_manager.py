"""
Cache manager for stock data to improve performance and reduce API calls.

This module handles:
- Persistent data caching using JSON files
- Cache validation and expiration
- Incremental data updates
- GitHub Actions artifact integration
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd


class CacheManager:
    """
    Manages persistent caching of stock data to reduce API calls and improve performance.

    Features:
    - JSON-based file storage
    - Configurable cache expiration
    - Incremental updates
    - GitHub Actions artifact support
    """

    def __init__(self, cache_dir: str = "cache"):
        """
        Initialize CacheManager with cache directory.

        Args:
            cache_dir: Directory for cache files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

        self.logger = logging.getLogger(__name__)

        # Cache file paths
        self.stock_data_cache = self.cache_dir / "stock_data.json"
        self.financial_cache = self.cache_dir / "financial_info.json"
        self.dividend_cache = self.cache_dir / "dividend_history.json"
        self.metadata_cache = self.cache_dir / "cache_metadata.json"

        # Cache expiration settings
        self.financial_cache_duration = timedelta(
            days=1
        )  # Financial data expires daily
        self.price_cache_duration = timedelta(
            hours=4
        )  # Price data expires every 4 hours
        self.dividend_cache_duration = timedelta(days=7)  # Dividend data expires weekly

    def get_cached_financial_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get cached financial information for a stock symbol.

        Args:
            symbol: Stock symbol (e.g., "7203.T")

        Returns:
            Cached financial info dict or None if not cached/expired
        """
        try:
            if not self.financial_cache.exists():
                return None

            with open(self.financial_cache, "r", encoding="utf-8") as f:
                cache_data = json.load(f)

            if symbol not in cache_data:
                return None

            cached_item = cache_data[symbol]
            cache_time = datetime.fromisoformat(cached_item["timestamp"])

            if datetime.now() - cache_time > self.financial_cache_duration:
                self.logger.debug(f"Financial cache expired for {symbol}")
                return None

            self.logger.debug(f"Using cached financial info for {symbol}")
            return cached_item["data"]

        except Exception as e:
            self.logger.warning(f"Error reading financial cache for {symbol}: {e}")
            return None

    def cache_financial_info(self, symbol: str, data: Dict[str, Any]) -> None:
        """
        Cache financial information for a stock symbol.

        Args:
            symbol: Stock symbol
            data: Financial information to cache
        """
        try:
            # Load existing cache or create new
            cache_data = {}
            if self.financial_cache.exists():
                with open(self.financial_cache, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)

            # Add/update entry
            cache_data[symbol] = {"timestamp": datetime.now().isoformat(), "data": data}

            # Save cache
            with open(self.financial_cache, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            self.logger.debug(f"Cached financial info for {symbol}")

        except Exception as e:
            self.logger.warning(f"Error caching financial info for {symbol}: {e}")

    def get_cached_dividend_history(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Get cached dividend history for a stock symbol.

        Args:
            symbol: Stock symbol

        Returns:
            Cached dividend DataFrame or None if not cached/expired
        """
        try:
            if not self.dividend_cache.exists():
                return None

            with open(self.dividend_cache, "r", encoding="utf-8") as f:
                cache_data = json.load(f)

            if symbol not in cache_data:
                return None

            cached_item = cache_data[symbol]
            cache_time = datetime.fromisoformat(cached_item["timestamp"])

            if datetime.now() - cache_time > self.dividend_cache_duration:
                self.logger.debug(f"Dividend cache expired for {symbol}")
                return None

            # Convert back to DataFrame
            dividend_data = cached_item["data"]
            if not dividend_data:
                return pd.DataFrame(columns=["Date", "Dividends", "Symbol"])

            df = pd.DataFrame(dividend_data)
            df["Date"] = pd.to_datetime(df["Date"])

            self.logger.debug(f"Using cached dividend history for {symbol}")
            return df

        except Exception as e:
            self.logger.warning(f"Error reading dividend cache for {symbol}: {e}")
            return None

    def cache_dividend_history(self, symbol: str, df: pd.DataFrame) -> None:
        """
        Cache dividend history for a stock symbol.

        Args:
            symbol: Stock symbol
            df: Dividend DataFrame to cache
        """
        try:
            # Load existing cache or create new
            cache_data = {}
            if self.dividend_cache.exists():
                with open(self.dividend_cache, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)

            # Convert DataFrame to serializable format
            if df.empty:
                dividend_data = []
            else:
                dividend_data = df.to_dict("records")
                # Convert datetime to string
                for record in dividend_data:
                    if "Date" in record:
                        record["Date"] = (
                            record["Date"].isoformat()
                            if hasattr(record["Date"], "isoformat")
                            else str(record["Date"])
                        )

            # Add/update entry
            cache_data[symbol] = {
                "timestamp": datetime.now().isoformat(),
                "data": dividend_data,
            }

            # Save cache
            with open(self.dividend_cache, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            self.logger.debug(f"Cached dividend history for {symbol}")

        except Exception as e:
            self.logger.warning(f"Error caching dividend history for {symbol}: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics and metadata.

        Returns:
            Dictionary with cache statistics
        """
        stats = {
            "cache_dir": str(self.cache_dir),
            "financial_cache_size": 0,
            "dividend_cache_size": 0,
            "cache_files": [],
        }

        try:
            if self.financial_cache.exists():
                with open(self.financial_cache, "r", encoding="utf-8") as f:
                    financial_data = json.load(f)
                    stats["financial_cache_size"] = len(financial_data)
                    stats["cache_files"].append("financial_info.json")

            if self.dividend_cache.exists():
                with open(self.dividend_cache, "r", encoding="utf-8") as f:
                    dividend_data = json.load(f)
                    stats["dividend_cache_size"] = len(dividend_data)
                    stats["cache_files"].append("dividend_history.json")

            # Calculate total cache size
            total_size = 0
            for cache_file in self.cache_dir.glob("*.json"):
                total_size += cache_file.stat().st_size
            stats["total_cache_size_mb"] = round(total_size / (1024 * 1024), 2)

        except Exception as e:
            self.logger.warning(f"Error calculating cache stats: {e}")

        return stats

    def cleanup_expired_cache(self) -> None:
        """Remove expired cache entries to keep cache size manageable."""
        try:
            # Clean financial cache
            if self.financial_cache.exists():
                with open(self.financial_cache, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)

                cleaned_data = {}
                for symbol, cached_item in cache_data.items():
                    cache_time = datetime.fromisoformat(cached_item["timestamp"])
                    if datetime.now() - cache_time <= self.financial_cache_duration:
                        cleaned_data[symbol] = cached_item

                if len(cleaned_data) != len(cache_data):
                    with open(self.financial_cache, "w", encoding="utf-8") as f:
                        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
                    self.logger.info(
                        f"Cleaned financial cache: {len(cache_data)} -> {len(cleaned_data)} entries"
                    )

            # Clean dividend cache
            if self.dividend_cache.exists():
                with open(self.dividend_cache, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)

                cleaned_data = {}
                for symbol, cached_item in cache_data.items():
                    cache_time = datetime.fromisoformat(cached_item["timestamp"])
                    if datetime.now() - cache_time <= self.dividend_cache_duration:
                        cleaned_data[symbol] = cached_item

                if len(cleaned_data) != len(cache_data):
                    with open(self.dividend_cache, "w", encoding="utf-8") as f:
                        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
                    self.logger.info(
                        f"Cleaned dividend cache: {len(cache_data)} -> {len(cleaned_data)} entries"
                    )

        except Exception as e:
            self.logger.warning(f"Error cleaning cache: {e}")

    def save_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Save cache metadata for tracking and debugging.

        Args:
            metadata: Metadata dictionary to save
        """
        try:
            metadata["last_updated"] = datetime.now().isoformat()

            with open(self.metadata_cache, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

        except Exception as e:
            self.logger.warning(f"Error saving cache metadata: {e}")

    def load_metadata(self) -> Dict[str, Any]:
        """
        Load cache metadata.

        Returns:
            Metadata dictionary or empty dict if not found
        """
        try:
            if self.metadata_cache.exists():
                with open(self.metadata_cache, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            self.logger.warning(f"Error loading cache metadata: {e}")

        return {}
