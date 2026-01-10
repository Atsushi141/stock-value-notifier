"""CSV export module for value stock data."""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

from .models import ValueStock


class CSVExporter:
    """Handles CSV export functionality for value stock data."""

    def __init__(self, output_dir: str = "."):
        """Initialize CSVExporter with output directory.

        Args:
            output_dir: Directory to save CSV files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)

        # TSE sector mapping from English to Japanese (17業種区分)
        self.sector_17_mapping = {
            "Consumer Cyclical": "小売業",
            "Technology": "情報・通信業",
            "Communication Services": "情報・通信業",
            "Healthcare": "医薬品",
            "Financial Services": "銀行業",
            "Consumer Defensive": "食料品",
            "Industrials": "機械",
            "Basic Materials": "化学",
            "Energy": "石油・石炭製品",
            "Utilities": "電気・ガス業",
            "Real Estate": "不動産業",
            "Transportation": "陸運業",
            "Construction": "建設業",
            "Mining": "鉱業",
            "Textiles": "繊維製品",
            "Paper & Pulp": "パルプ・紙",
            "Other": "その他製品",
        }

        # TSE sector mapping from English to Japanese (33業種区分)
        self.sector_33_mapping = {
            "Consumer Cyclical": "小売業",
            "Technology": "情報・通信業",
            "Communication Services": "情報・通信業",
            "Healthcare": "医薬品",
            "Financial Services": "銀行業",
            "Consumer Defensive": "食料品",
            "Industrials": "機械",
            "Basic Materials": "化学",
            "Energy": "石油・石炭製品",
            "Utilities": "電気・ガス業",
            "Real Estate": "不動産業",
            "Transportation": "陸運業",
            "Construction": "建設業",
            "Mining": "鉱業",
            "Textiles": "繊維製品",
            "Paper & Pulp": "パルプ・紙",
            "Other": "その他製品",
        }

    def export_all_csv_files(
        self, stocks: List[ValueStock], target_date: Optional[str] = None
    ) -> Dict[str, str]:
        """Export all 4 CSV files (main JP/EN + history JP/EN).

        Args:
            stocks: List of ValueStock objects to export
            target_date: Optional target date (YYYY-MM-DD format)

        Returns:
            Dict[str, str]: Dictionary mapping file types to file paths
        """
        if target_date is None:
            target_date = datetime.now().strftime("%Y%m%d")
        else:
            # Convert YYYY-MM-DD to YYYYMMDD
            target_date = target_date.replace("-", "")

        file_paths = {}

        try:
            # Export main CSV files (excluding score column per requirements)
            file_paths["main_jp"] = self.export_main_csv(
                stocks, target_date, language="jp"
            )
            file_paths["main_en"] = self.export_main_csv(
                stocks, target_date, language="en"
            )

            # Export history CSV files (actual yfinance data only)
            file_paths["history_jp"] = self.export_history_csv(
                stocks, target_date, language="jp"
            )
            file_paths["history_en"] = self.export_history_csv(
                stocks, target_date, language="en"
            )

            self.logger.info(
                f"Successfully exported all 4 CSV files for {len(stocks)} stocks"
            )
            return file_paths

        except Exception as e:
            self.logger.error(f"Failed to export CSV files: {str(e)}")
            raise

    def export_main_csv(
        self, stocks: List[ValueStock], target_date: str, language: str = "jp"
    ) -> str:
        """Export main CSV file with current stock data.

        Args:
            stocks: List of ValueStock objects to export
            target_date: Target date in YYYYMMDD format
            language: Language for headers ("jp" or "en")

        Returns:
            str: Path to the exported CSV file
        """
        suffix = "" if language == "jp" else "_en"
        filename = f"value_stocks_{target_date}{suffix}.csv"
        filepath = self.output_dir / filename

        # Define headers based on language (excluding score column per requirements)
        if language == "jp":
            headers = [
                "銘柄コード",
                "銘柄名",
                "現在株価",
                "PER",
                "PBR",
                "配当利回り",
                "連続増配年数",
                "連続増収年数",
                "連続増益年数",
                "PER変動係数",
                "業種区分",
                "市場区分",
                "規模区分",
                "取得日時",
            ]
        else:
            headers = [
                "Stock Code",
                "Company Name",
                "Current Price",
                "PER",
                "PBR",
                "Dividend Yield",
                "Consecutive Dividend Growth Years",
                "Consecutive Revenue Growth Years",
                "Consecutive Profit Growth Years",
                "PER Volatility Coefficient",
                "Sector",
                "Market Category",
                "Size Category",
                "Retrieved At",
            ]

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

                for stock in stocks:
                    # Use TSE sector names for Japanese CSV, English names for English CSV
                    if language == "jp":
                        # Use actual TSE sector names from stock data
                        sector_display = stock.sector_17 or stock.sector_33 or "その他"
                    else:
                        # For English, use the sector_17 or sector_33 field as-is, or translate if needed
                        sector_display = stock.sector_17 or stock.sector_33 or "Other"

                    row = [
                        stock.code,
                        stock.name,
                        stock.current_price,
                        stock.per,
                        stock.pbr,
                        stock.dividend_yield,
                        stock.dividend_growth_years,
                        stock.revenue_growth_years,
                        stock.profit_growth_years,
                        stock.per_stability,  # Changed from per_volatility to per_stability
                        sector_display,
                        stock.market_category,
                        stock.size_category,
                        (
                            stock.retrieved_at.strftime("%Y-%m-%d %H:%M:%S")
                            if stock.retrieved_at
                            else ""
                        ),
                    ]
                    writer.writerow(row)

            self.logger.info(f"Exported main CSV ({language}): {filepath}")
            return str(filepath)

        except Exception as e:
            self.logger.error(f"Failed to export main CSV ({language}): {str(e)}")
            raise

    def export_history_csv(
        self, stocks: List[ValueStock], target_date: str, language: str = "jp"
    ) -> str:
        """Export history CSV file with 10-year historical data.

        Args:
            stocks: List of ValueStock objects to export
            target_date: Target date in YYYYMMDD format
            language: Language for headers ("jp" or "en")

        Returns:
            str: Path to the exported CSV file
        """
        suffix = "_history" if language == "jp" else "_history_en"
        filename = f"value_stocks_{target_date}{suffix}.csv"
        filepath = self.output_dir / filename

        # Define headers based on language
        if language == "jp":
            headers = [
                "銘柄コード",
                "銘柄名",
                "データ種別",
                "単位",
                "2015",
                "2016",
                "2017",
                "2018",
                "2019",
                "2020",
                "2021",
                "2022",
                "2023",
                "2024",
            ]
        else:
            headers = [
                "Stock Code",
                "Company Name",
                "Data Type",
                "Unit",
                "2015",
                "2016",
                "2017",
                "2018",
                "2019",
                "2020",
                "2021",
                "2022",
                "2023",
                "2024",
            ]

        try:
            with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

                for stock in stocks:
                    # Export dividend history (actual yfinance data only)
                    if hasattr(stock, "dividend_history") and stock.dividend_history:
                        dividend_row = self._create_history_row(
                            stock, "dividend", language, stock.dividend_history
                        )
                        writer.writerow(dividend_row)

                    # Export revenue history (actual yfinance data only)
                    if hasattr(stock, "revenue_history") and stock.revenue_history:
                        revenue_row = self._create_history_row(
                            stock, "revenue", language, stock.revenue_history
                        )
                        writer.writerow(revenue_row)

                    # Export profit history (actual yfinance data only)
                    if hasattr(stock, "profit_history") and stock.profit_history:
                        profit_row = self._create_history_row(
                            stock, "profit", language, stock.profit_history
                        )
                        writer.writerow(profit_row)

                    # Export PER history (actual yfinance data only)
                    if hasattr(stock, "per_history") and stock.per_history:
                        per_row = self._create_history_row(
                            stock, "per", language, stock.per_history
                        )
                        writer.writerow(per_row)

            self.logger.info(f"Exported history CSV ({language}): {filepath}")
            return str(filepath)

        except Exception as e:
            self.logger.error(f"Failed to export history CSV ({language}): {str(e)}")
            raise

    def _create_history_row(
        self,
        stock: ValueStock,
        data_type: str,
        language: str,
        history_data: Dict[str, Any],
    ) -> List[Any]:
        """Create a history row for CSV export using only actual yfinance data.

        Args:
            stock: ValueStock object
            data_type: Type of data ("dividend", "revenue", "profit", "per")
            language: Language for data type labels ("jp" or "en")
            history_data: Historical data dictionary (actual yfinance data only)

        Returns:
            List[Any]: Row data for CSV
        """
        # Define data type labels and units
        if language == "jp":
            type_labels = {
                "dividend": "配当金",
                "revenue": "売上高",
                "profit": "純利益",
                "per": "PER",
            }
            units = {
                "dividend": "円/株",
                "revenue": "億円",
                "profit": "億円",
                "per": "倍",
            }
        else:
            type_labels = {
                "dividend": "Dividend",
                "revenue": "Revenue",
                "profit": "Net Profit",
                "per": "PER",
            }
            units = {
                "dividend": "JPY/Share",
                "revenue": "100M JPY",
                "profit": "100M JPY",
                "per": "Times",
            }

        # Create row with basic info
        row = [
            stock.code,
            stock.name,
            type_labels.get(data_type, data_type),
            units.get(data_type, ""),
        ]

        # Add historical data for years 2015-2024 (actual data only, no estimates)
        years = [
            "2015",
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
            "2022",
            "2023",
            "2024",
        ]
        for year in years:
            # Only use actual data from yfinance, leave empty if not available
            value = history_data.get(year, "")
            # Ensure we don't include any estimated or calculated values
            if value is not None and value != "" and not pd.isna(value):
                row.append(value)
            else:
                row.append("")  # Empty if no actual data available

        return row

    def get_csv_file_info(
        self, file_paths: Dict[str, str]
    ) -> Dict[str, Dict[str, Any]]:
        """Get information about exported CSV files.

        Args:
            file_paths: Dictionary mapping file types to file paths

        Returns:
            Dict[str, Dict[str, Any]]: File information including size, row count, etc.
        """
        file_info = {}

        for file_type, filepath in file_paths.items():
            try:
                path_obj = Path(filepath)
                if path_obj.exists():
                    # Get file size
                    file_size = path_obj.stat().st_size

                    # Count rows
                    with open(filepath, "r", encoding="utf-8") as f:
                        row_count = sum(1 for line in f)

                    file_info[file_type] = {
                        "filepath": filepath,
                        "filename": path_obj.name,
                        "size_bytes": file_size,
                        "size_kb": round(file_size / 1024, 2),
                        "row_count": row_count,
                        "data_rows": row_count - 1,  # Excluding header
                        "exists": True,
                    }
                else:
                    file_info[file_type] = {
                        "filepath": filepath,
                        "filename": Path(filepath).name,
                        "exists": False,
                        "error": "File not found",
                    }

            except Exception as e:
                file_info[file_type] = {
                    "filepath": filepath,
                    "filename": Path(filepath).name,
                    "exists": False,
                    "error": str(e),
                }

        return file_info
