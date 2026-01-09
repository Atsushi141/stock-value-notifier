"""TSE Stock List Manager for handling official TSE stock data file."""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
import pandas as pd

from .models import TSEStockInfo, TSEDataConfig


class TSEStockListManager:
    """
    Manager for TSE official stock data file (data_j.xls).

    Handles:
    - Loading TSE stock data from Excel file
    - Filtering investment products (ETF, REIT, etc.)
    - Extracting tradable stocks
    - Providing stock metadata and classification

    Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9
    """

    def __init__(
        self,
        config: Optional[TSEDataConfig] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize TSE Stock List Manager.

        Args:
            config: TSE data configuration
            logger: Logger instance
        """
        self.config = config or TSEDataConfig()
        self.logger = logger or logging.getLogger(__name__)
        self._cached_data: Optional[pd.DataFrame] = None
        self._cache_timestamp: Optional[datetime] = None

    def load_tse_stock_data(self) -> pd.DataFrame:
        """
        Load TSE stock data from Excel file.

        Returns:
            pd.DataFrame: Raw TSE stock data

        Raises:
            FileNotFoundError: If data file doesn't exist
            Exception: If file reading fails

        Requirements: 8.1, 8.2
        """
        try:
            # Check if we have valid cached data
            if self._is_cache_valid():
                self.logger.info("Using cached TSE stock data")
                return self._cached_data.copy()

            # Check if file exists
            if not os.path.exists(self.config.data_file_path):
                raise FileNotFoundError(
                    f"TSE data file not found: {self.config.data_file_path}"
                )

            self.logger.info(
                f"Loading TSE stock data from {self.config.data_file_path}"
            )

            # Read Excel file using xlrd engine for .xls files
            df = pd.read_excel(self.config.data_file_path, engine="xlrd")

            # Validate required columns
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
                raise ValueError(f"Missing required columns: {missing_columns}")

            # Cache the data
            self._cached_data = df.copy()
            self._cache_timestamp = datetime.now()

            self.logger.info(
                f"Successfully loaded {len(df)} records from TSE data file"
            )
            return df.copy()

        except Exception as e:
            self.logger.error(f"Failed to load TSE stock data: {e}")
            raise

    def filter_tradable_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter to get only tradable stocks (exclude delisted, etc.).

        Args:
            df: Raw TSE stock data

        Returns:
            pd.DataFrame: Filtered tradable stocks

        Requirements: 8.5
        """
        try:
            # Filter out rows with missing essential data
            filtered_df = df.dropna(subset=["コード", "銘柄名", "市場・商品区分"])

            # Convert code to string and ensure it's numeric
            filtered_df = filtered_df.copy()
            filtered_df["コード"] = filtered_df["コード"].astype(str)

            # Filter out non-numeric codes (should be 4-digit stock codes)
            numeric_codes = filtered_df["コード"].str.match(r"^\d{4}$")
            filtered_df = filtered_df[numeric_codes]

            self.logger.info(
                f"Filtered to {len(filtered_df)} tradable stocks from {len(df)} total records"
            )
            return filtered_df

        except Exception as e:
            self.logger.error(f"Failed to filter tradable stocks: {e}")
            raise

    def exclude_investment_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Exclude investment products (ETF, REIT, etc.) to get only regular stocks.

        Args:
            df: TSE stock data

        Returns:
            pd.DataFrame: Data with investment products excluded

        Requirements: 8.3, 8.4
        """
        try:
            initial_count = len(df)

            # Exclude investment products based on market category
            excluded_categories = set(self.config.excluded_market_categories)

            # Additional patterns to identify investment products in stock names
            investment_patterns = [
                "ETF",
                "ETN",
                "REIT",
                "ファンド",
                "インデックス",
                "投信",
                "投資信託",
                "ベンチャー",
                "カントリー",
                "インフラ",
            ]

            # Filter out excluded categories
            mask = ~df["市場・商品区分"].isin(excluded_categories)

            # Additional filtering based on stock name patterns
            name_mask = True
            for pattern in investment_patterns:
                name_mask = name_mask & (~df["銘柄名"].str.contains(pattern, na=False))

            # Combine both filters
            combined_mask = mask & name_mask
            filtered_df = df[combined_mask].copy()

            excluded_count = initial_count - len(filtered_df)

            self.logger.info(
                f"Excluded {excluded_count} investment products, {len(filtered_df)} regular stocks remaining"
            )

            # Log excluded categories for transparency
            if excluded_count > 0:
                excluded_breakdown = df[~mask]["市場・商品区分"].value_counts()
                self.logger.info(
                    f"Excluded categories breakdown: {excluded_breakdown.to_dict()}"
                )

                # Log name-based exclusions
                name_excluded = df[mask & ~name_mask]
                if len(name_excluded) > 0:
                    self.logger.info(
                        f"Additional name-based exclusions: {len(name_excluded)} stocks"
                    )

            return filtered_df

        except Exception as e:
            self.logger.error(f"Failed to exclude investment products: {e}")
            raise

    def filter_target_markets(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter to include only target market categories (Prime, Standard, Growth, PRO Market).

        Args:
            df: TSE stock data

        Returns:
            pd.DataFrame: Data filtered to target markets only

        Requirements: 8.5
        """
        try:
            initial_count = len(df)

            # Filter by target market categories
            if self.config.target_market_categories:
                mask = df["市場・商品区分"].isin(self.config.target_market_categories)
                filtered_df = df[mask].copy()
            else:
                filtered_df = df.copy()

            filtered_count = len(filtered_df)
            excluded_count = initial_count - filtered_count

            self.logger.info(
                f"Filtered to target markets: {filtered_count} stocks, excluded {excluded_count} from other markets"
            )

            # Log market breakdown
            if filtered_count > 0:
                market_breakdown = filtered_df["市場・商品区分"].value_counts()
                self.logger.info(
                    f"Target market breakdown: {market_breakdown.to_dict()}"
                )

            return filtered_df

        except Exception as e:
            self.logger.error(f"Failed to filter target markets: {e}")
            raise

    def is_investment_product(self, stock_info: Dict[str, Any]) -> bool:
        """
        Check if a stock is an investment product (ETF, REIT, etc.).

        Args:
            stock_info: Stock information dictionary

        Returns:
            bool: True if it's an investment product, False otherwise

        Requirements: 8.3, 8.4
        """
        try:
            market_category = stock_info.get("market_category", "")
            stock_name = stock_info.get("name", "")

            # Check market category
            if market_category in self.config.excluded_market_categories:
                return True

            # Check name patterns
            investment_patterns = [
                "ETF",
                "ETN",
                "REIT",
                "ファンド",
                "インデックス",
                "投信",
                "投資信託",
                "ベンチャー",
                "カントリー",
                "インフラ",
            ]

            for pattern in investment_patterns:
                if pattern in stock_name:
                    return True

            return False

        except Exception as e:
            self.logger.error(f"Failed to check if stock is investment product: {e}")
            return False

    def get_stock_codes_with_suffix(self, df: pd.DataFrame) -> List[str]:
        """
        Get stock codes with .T suffix for yfinance compatibility.

        Args:
            df: Filtered TSE stock data

        Returns:
            List[str]: Stock codes with .T suffix

        Requirements: 8.5
        """
        try:
            # Get unique stock codes and add .T suffix
            codes = df["コード"].astype(str).unique()
            codes_with_suffix = [
                f"{code}.T" for code in codes if code.isdigit() and len(code) == 4
            ]

            self.logger.info(
                f"Generated {len(codes_with_suffix)} stock codes with .T suffix"
            )
            return sorted(codes_with_suffix)

        except Exception as e:
            self.logger.error(f"Failed to generate stock codes with suffix: {e}")
            raise

    def get_stocks_by_sector(
        self, sector_code: str, use_17_sector: bool = True
    ) -> List[str]:
        """
        Get stock codes filtered by sector classification.

        Args:
            sector_code: Sector code to filter by
            use_17_sector: If True, use 17-sector classification; if False, use 33-sector

        Returns:
            List[str]: Stock codes in the specified sector

        Requirements: 8.9
        """
        try:
            df = self.load_tse_stock_data()
            df = self.filter_tradable_stocks(df)
            df = self.exclude_investment_products(df)

            sector_column = "17業種コード" if use_17_sector else "33業種コード"

            # Filter by sector
            sector_stocks = df[df[sector_column] == sector_code]

            return self.get_stock_codes_with_suffix(sector_stocks)

        except Exception as e:
            self.logger.error(f"Failed to get stocks by sector {sector_code}: {e}")
            return []

    def get_stocks_by_market_size(self, size_category: str) -> List[str]:
        """
        Get stock codes filtered by market size category.

        Args:
            size_category: Size category to filter by (e.g., "TOPIX Small")

        Returns:
            List[str]: Stock codes in the specified size category

        Requirements: 8.9
        """
        try:
            df = self.load_tse_stock_data()
            df = self.filter_tradable_stocks(df)
            df = self.exclude_investment_products(df)

            # Filter by size category
            size_stocks = df[df["規模区分"] == size_category]

            return self.get_stock_codes_with_suffix(size_stocks)

        except Exception as e:
            self.logger.error(
                f"Failed to get stocks by market size {size_category}: {e}"
            )
            return []

    def get_stock_metadata(self, stock_code: str) -> Dict[str, Any]:
        """
        Get metadata for a specific stock code.

        Args:
            stock_code: Stock code (with or without .T suffix)

        Returns:
            Dict[str, Any]: Stock metadata including sector and size information

        Requirements: 8.9
        """
        try:
            # Remove .T suffix if present
            clean_code = stock_code.replace(".T", "")

            df = self.load_tse_stock_data()
            stock_info = df[df["コード"].astype(str) == clean_code]

            if stock_info.empty:
                return {}

            # Get the first matching record
            record = stock_info.iloc[0]

            return {
                "code": clean_code,
                "name": record["銘柄名"],
                "market_category": record["市場・商品区分"],
                "sector_33_code": record["33業種コード"],
                "sector_33_name": record["33業種区分"],
                "sector_17_code": record["17業種コード"],
                "sector_17_name": record["17業種区分"],
                "size_code": record["規模コード"],
                "size_category": record["規模区分"],
                "date": record["日付"],
            }

        except Exception as e:
            self.logger.error(f"Failed to get metadata for stock {stock_code}: {e}")
            return {}

    def get_all_tradable_stocks(self) -> List[str]:
        """
        Get all tradable stock codes (excluding investment products).

        Returns:
            List[str]: All tradable stock codes with .T suffix

        Requirements: 8.1, 8.3, 8.4, 8.5
        """
        try:
            df = self.load_tse_stock_data()
            df = self.filter_tradable_stocks(df)
            df = self.exclude_investment_products(df)
            df = self.filter_target_markets(df)

            return self.get_stock_codes_with_suffix(df)

        except Exception as e:
            self.logger.error(f"Failed to get all tradable stocks: {e}")
            raise

    def get_processing_statistics(self) -> Dict[str, Any]:
        """
        Get processing statistics for logging and monitoring.

        Returns:
            Dict[str, Any]: Processing statistics

        Requirements: 8.8
        """
        try:
            df = self.load_tse_stock_data()
            total_records = len(df)

            tradable_df = self.filter_tradable_stocks(df)
            tradable_count = len(tradable_df)

            final_df = self.exclude_investment_products(tradable_df)
            final_df = self.filter_target_markets(final_df)
            final_count = len(final_df)

            # Get breakdown by market category
            market_breakdown = final_df["市場・商品区分"].value_counts().to_dict()

            # Get breakdown by sector (17-sector classification)
            sector_breakdown = final_df["17業種区分"].value_counts().to_dict()

            # Get breakdown by size
            size_breakdown = final_df["規模区分"].value_counts().to_dict()

            excluded_count = tradable_count - final_count

            return {
                "total_records": total_records,
                "tradable_stocks": tradable_count,
                "final_stocks": final_count,
                "excluded_investment_products": excluded_count,
                "market_category_breakdown": market_breakdown,
                "sector_17_breakdown": sector_breakdown,
                "size_category_breakdown": size_breakdown,
                "processing_timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Failed to get processing statistics: {e}")
            return {}

    def get_fallback_stock_list(self) -> List[str]:
        """
        Get stock list using fallback range-based validation.
        Used when TSE data file loading fails.

        Returns:
            List[str]: Stock codes with .T suffix using range-based approach

        Requirements: 8.7
        """
        try:
            self.logger.warning("Using fallback range-based stock validation")

            # Generate stock codes in typical TSE ranges
            stock_codes = []

            # Main board ranges (approximate)
            ranges = [
                (1000, 2000),  # Basic materials, industrials
                (2000, 3000),  # Consumer goods, services
                (3000, 4000),  # Technology, telecommunications
                (4000, 5000),  # Healthcare, pharmaceuticals
                (5000, 6000),  # Energy, utilities
                (6000, 7000),  # Industrials, machinery
                (7000, 8000),  # Transportation, automotive
                (8000, 9000),  # Financial services
                (9000, 10000),  # Real estate, services
            ]

            for start, end in ranges:
                for code in range(start, end):
                    stock_codes.append(f"{code}.T")

            self.logger.info(
                f"Generated {len(stock_codes)} stock codes using fallback method"
            )
            return stock_codes

        except Exception as e:
            self.logger.error(f"Failed to generate fallback stock list: {e}")
            return []

    def get_stocks_with_fallback(self) -> List[str]:
        """
        Get stock list with automatic fallback to range-based validation.

        Returns:
            List[str]: Stock codes with .T suffix

        Requirements: 8.6, 8.7
        """
        try:
            # Try to get stocks using TSE official data
            return self.get_all_tradable_stocks()

        except Exception as e:
            self.logger.error(f"TSE data loading failed: {e}")

            if self.config.fallback_to_range_validation:
                self.logger.info("Falling back to range-based stock validation")
                return self.get_fallback_stock_list()
            else:
                self.logger.error("Fallback disabled, re-raising exception")
                raise

    def detect_file_update(self) -> bool:
        """
        Detect if TSE data file has been updated since last cache.

        Returns:
            bool: True if file has been updated, False otherwise

        Requirements: 8.6
        """
        try:
            if not os.path.exists(self.config.data_file_path):
                return False

            file_mtime = datetime.fromtimestamp(
                os.path.getmtime(self.config.data_file_path)
            )

            if self._cache_timestamp is None:
                return True

            return file_mtime > self._cache_timestamp

        except Exception as e:
            self.logger.error(f"Failed to detect file update: {e}")
            return True  # Assume update to be safe

    def refresh_if_updated(self) -> bool:
        """
        Refresh cache if TSE data file has been updated.

        Returns:
            bool: True if cache was refreshed, False otherwise

        Requirements: 8.6
        """
        try:
            if self.detect_file_update():
                self.logger.info("TSE data file updated, refreshing cache")
                self.invalidate_cache()
                # Trigger reload on next access
                self.load_tse_stock_data()
                return True

            return False

        except Exception as e:
            self.logger.error(f"Failed to refresh cache: {e}")
            return False

    def _is_cache_valid(self) -> bool:
        """
        Check if cached data is still valid.

        Returns:
            bool: True if cache is valid, False otherwise

        Requirements: 8.6
        """
        if self._cached_data is None or self._cache_timestamp is None:
            return False

        cache_age = datetime.now() - self._cache_timestamp
        max_age = timedelta(hours=self.config.cache_duration_hours)

        return cache_age < max_age

    def invalidate_cache(self) -> None:
        """
        Invalidate cached data to force reload on next access.

        Requirements: 8.6
        """
        self._cached_data = None
        self._cache_timestamp = None
        self.logger.info("TSE stock data cache invalidated")

    def get_sector_classifications(self) -> Dict[str, List[str]]:
        """
        Get available sector classifications.

        Returns:
            Dict[str, List[str]]: Available sector codes and names

        Requirements: 8.9
        """
        try:
            df = self.load_tse_stock_data()
            df = self.filter_tradable_stocks(df)
            df = self.exclude_investment_products(df)

            # Get unique 17-sector classifications
            sector_17 = df[["17業種コード", "17業種区分"]].drop_duplicates()
            sector_17 = sector_17[sector_17["17業種コード"] != "-"].sort_values(
                "17業種コード"
            )

            # Get unique 33-sector classifications
            sector_33 = df[["33業種コード", "33業種区分"]].drop_duplicates()
            sector_33 = sector_33[sector_33["33業種コード"] != "-"].sort_values(
                "33業種コード"
            )

            return {
                "sector_17": [
                    {"code": row["17業種コード"], "name": row["17業種区分"]}
                    for _, row in sector_17.iterrows()
                ],
                "sector_33": [
                    {"code": row["33業種コード"], "name": row["33業種区分"]}
                    for _, row in sector_33.iterrows()
                ],
            }

        except Exception as e:
            self.logger.error(f"Failed to get sector classifications: {e}")
            return {"sector_17": [], "sector_33": []}

    def get_size_categories(self) -> List[str]:
        """
        Get available size categories.

        Returns:
            List[str]: Available size categories

        Requirements: 8.9
        """
        try:
            df = self.load_tse_stock_data()
            df = self.filter_tradable_stocks(df)
            df = self.exclude_investment_products(df)

            # Get unique size categories
            size_categories = df["規模区分"].unique()
            size_categories = [cat for cat in size_categories if cat != "-"]

            return sorted(size_categories)

        except Exception as e:
            self.logger.error(f"Failed to get size categories: {e}")
            return []

    def get_stocks_by_classification(
        self, classification_type: str, classification_value: str
    ) -> List[str]:
        """
        Get stocks by various classification types.

        Args:
            classification_type: Type of classification ('sector_17', 'sector_33', 'size', 'market')
            classification_value: Value to filter by

        Returns:
            List[str]: Stock codes matching the classification

        Requirements: 8.9
        """
        try:
            df = self.load_tse_stock_data()
            df = self.filter_tradable_stocks(df)
            df = self.exclude_investment_products(df)

            if classification_type == "sector_17":
                filtered_df = df[df["17業種区分"] == classification_value]
            elif classification_type == "sector_33":
                filtered_df = df[df["33業種区分"] == classification_value]
            elif classification_type == "size":
                filtered_df = df[df["規模区分"] == classification_value]
            elif classification_type == "market":
                filtered_df = df[df["市場・商品区分"] == classification_value]
            else:
                self.logger.error(f"Unknown classification type: {classification_type}")
                return []

            return self.get_stock_codes_with_suffix(filtered_df)

        except Exception as e:
            self.logger.error(
                f"Failed to get stocks by classification {classification_type}={classification_value}: {e}"
            )
            return []

    def get_classification_distribution(self) -> Dict[str, Dict[str, int]]:
        """
        Get distribution of stocks across different classifications.

        Returns:
            Dict[str, Dict[str, int]]: Distribution statistics

        Requirements: 8.9
        """
        try:
            df = self.load_tse_stock_data()
            df = self.filter_tradable_stocks(df)
            df = self.exclude_investment_products(df)

            return {
                "sector_17_distribution": df["17業種区分"].value_counts().to_dict(),
                "sector_33_distribution": df["33業種区分"].value_counts().to_dict(),
                "size_distribution": df["規模区分"].value_counts().to_dict(),
                "market_distribution": df["市場・商品区分"].value_counts().to_dict(),
            }

        except Exception as e:
            self.logger.error(f"Failed to get classification distribution: {e}")
            return {}

    def get_stock_classification_summary(self, stock_code: str) -> Dict[str, Any]:
        """
        Get comprehensive classification summary for a stock.

        Args:
            stock_code: Stock code (with or without .T suffix)

        Returns:
            Dict[str, Any]: Complete classification information

        Requirements: 8.9
        """
        try:
            metadata = self.get_stock_metadata(stock_code)

            if not metadata:
                return {}

            return {
                "basic_info": {
                    "code": metadata["code"],
                    "name": metadata["name"],
                    "market_category": metadata["market_category"],
                },
                "sector_classification": {
                    "17_sector": {
                        "code": metadata["sector_17_code"],
                        "name": metadata["sector_17_name"],
                    },
                    "33_sector": {
                        "code": metadata["sector_33_code"],
                        "name": metadata["sector_33_name"],
                    },
                },
                "size_classification": {
                    "code": metadata["size_code"],
                    "category": metadata["size_category"],
                },
                "data_date": metadata["date"],
                "is_investment_product": self.is_investment_product(metadata),
            }

        except Exception as e:
            self.logger.error(
                f"Failed to get classification summary for {stock_code}: {e}"
            )
            return {}

    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Validate the integrity of TSE data and report any issues.

        Returns:
            Dict[str, Any]: Validation results

        Requirements: 8.8
        """
        try:
            df = self.load_tse_stock_data()

            validation_results = {
                "total_records": len(df),
                "validation_timestamp": datetime.now().isoformat(),
                "issues": [],
                "warnings": [],
            }

            # Check for missing essential data
            essential_columns = ["コード", "銘柄名", "市場・商品区分"]
            for col in essential_columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    validation_results["issues"].append(
                        {
                            "type": "missing_data",
                            "column": col,
                            "count": int(missing_count),
                        }
                    )

            # Check for non-standard stock codes (treat as warning, not error)
            # TSE data may contain codes like '130A', '131A' which are valid but non-standard
            non_standard_codes = df[
                ~df["コード"].astype(str).str.match(r"^\d{4}$", na=False)
            ]
            if len(non_standard_codes) > 0:
                validation_results["warnings"].append(
                    {
                        "type": "non_standard_stock_codes",
                        "count": len(non_standard_codes),
                        "examples": non_standard_codes["コード"].head(5).tolist(),
                        "description": "Codes that don't match standard 4-digit format (may be valid TSE codes)",
                    }
                )

            # Check for duplicate stock codes (this is a real issue)
            duplicate_codes = df[df["コード"].duplicated()]
            if len(duplicate_codes) > 0:
                validation_results["issues"].append(
                    {
                        "type": "duplicate_codes",
                        "count": len(duplicate_codes),
                        "examples": duplicate_codes["コード"].head(5).tolist(),
                    }
                )

            # Summary - only count real issues, not warnings
            validation_results["is_valid"] = len(validation_results["issues"]) == 0
            validation_results["issue_count"] = len(validation_results["issues"])
            validation_results["warning_count"] = len(validation_results["warnings"])

            if validation_results["is_valid"]:
                if validation_results["warning_count"] > 0:
                    self.logger.info(
                        f"TSE data validation passed with {validation_results['warning_count']} warnings"
                    )
                else:
                    self.logger.info("TSE data validation passed")
            else:
                self.logger.warning(
                    f"TSE data validation found {validation_results['issue_count']} issues and {validation_results['warning_count']} warnings"
                )

            return validation_results

        except Exception as e:
            self.logger.error(f"Failed to validate data integrity: {e}")
            return {
                "is_valid": False,
                "error": str(e),
                "validation_timestamp": datetime.now().isoformat(),
            }
