"""
Main workflow runner module for stock value notifier system.

This module handles:
- Market trading day validation
- Daily screening execution
- Environment setup and configuration
- Main workflow orchestration
- Comprehensive logging management
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime, date
from typing import Optional, List
import pandas as pd
from pathlib import Path

from .config_manager import ConfigManager, Config
from .data_fetcher import DataFetcher
from .screening_engine import ScreeningEngine
from .slack_notifier import SlackNotifier
from .models import ValueStock


class LogManager:
    """
    Manages comprehensive logging for the stock value notifier system.

    Handles:
    - Detailed log recording (要件 6.1)
    - Error logging and alert notifications (要件 6.2)
    - Log file rotation and management
    - Health check logging (要件 6.4)
    """

    def __init__(self, log_dir: str = "logs"):
        """
        Initialize LogManager with log directory setup.

        Args:
            log_dir: Directory for log files
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        # Create separate log files for different purposes
        self.main_log_file = self.log_dir / "stock_notifier.log"
        self.error_log_file = self.log_dir / "errors.log"
        self.health_log_file = self.log_dir / "health.log"

        self.logger = logging.getLogger(__name__)
        self._setup_loggers()

    def _setup_loggers(self) -> None:
        """Setup comprehensive logging configuration with rotation."""

        # Main application logger
        main_handler = logging.handlers.RotatingFileHandler(
            self.main_log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
        )
        main_handler.setLevel(logging.INFO)
        main_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        main_handler.setFormatter(main_formatter)

        # Error logger
        error_handler = logging.handlers.RotatingFileHandler(
            self.error_log_file, maxBytes=5 * 1024 * 1024, backupCount=3  # 5MB
        )
        error_handler.setLevel(logging.ERROR)
        error_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s\n"
            "Exception: %(exc_info)s\n"
        )
        error_handler.setFormatter(error_formatter)

        # Health check logger
        health_handler = logging.handlers.RotatingFileHandler(
            self.health_log_file, maxBytes=2 * 1024 * 1024, backupCount=2  # 2MB
        )
        health_handler.setLevel(logging.INFO)
        health_formatter = logging.Formatter("%(asctime)s - HEALTH - %(message)s")
        health_handler.setFormatter(health_formatter)

        # Console handler for GitHub Actions
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(main_handler)
        root_logger.addHandler(error_handler)
        root_logger.addHandler(console_handler)

        # Create health logger
        self.health_logger = logging.getLogger("health")
        self.health_logger.addHandler(health_handler)
        self.health_logger.setLevel(logging.INFO)

        # Suppress noisy external library logs
        logging.getLogger("yfinance").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("slack_sdk").setLevel(logging.WARNING)

    def log_system_health(
        self, component: str, status: str, details: dict = None
    ) -> None:
        """
        Log system health check information.

        Args:
            component: Component name being checked
            status: Health status (HEALTHY, WARNING, ERROR)
            details: Additional health details
        """
        health_info = {
            "component": component,
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "details": details or {},
        }

        self.health_logger.info(f"{component}: {status} - {health_info}")

    def log_workflow_start(self, workflow_type: str) -> None:
        """Log workflow execution start."""
        self.logger.info(f"=== WORKFLOW START: {workflow_type} ===")
        self.log_system_health("workflow", "STARTED", {"type": workflow_type})

    def log_workflow_end(
        self, workflow_type: str, success: bool, duration: float = None
    ) -> None:
        """Log workflow execution end."""
        status = "SUCCESS" if success else "FAILED"
        details = {"type": workflow_type, "duration_seconds": duration}

        self.logger.info(f"=== WORKFLOW END: {workflow_type} - {status} ===")
        self.log_system_health("workflow", status, details)

    def log_critical_error(self, error: Exception, context: str) -> None:
        """
        Log critical errors that require immediate attention.

        Args:
            error: Exception that occurred
            context: Context where the error occurred
        """
        error_details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context,
            "timestamp": datetime.now().isoformat(),
        }

        self.logger.critical(
            f"CRITICAL ERROR in {context}: {error}",
            exc_info=True,
            extra={"error_details": error_details},
        )

        self.log_system_health("system", "CRITICAL_ERROR", error_details)

    def log_performance_metrics(self, metrics: dict) -> None:
        """
        Log performance metrics for monitoring.

        Args:
            metrics: Dictionary of performance metrics
        """
        self.logger.info(f"PERFORMANCE METRICS: {metrics}")
        self.log_system_health("performance", "MEASURED", metrics)

    def get_recent_errors(self, hours: int = 24) -> List[str]:
        """
        Get recent error log entries for health monitoring.

        Args:
            hours: Number of hours to look back

        Returns:
            List of recent error messages
        """
        try:
            if not self.error_log_file.exists():
                return []

            with open(self.error_log_file, "r") as f:
                lines = f.readlines()

            # Simple implementation - in production, would parse timestamps
            return lines[-10:] if lines else []

        except Exception as e:
            self.logger.error(f"Failed to read error log: {e}")
            return []


class WorkflowRunner:
    """
    Main workflow runner for the stock value notifier system.

    Handles:
    - Market trading day validation (要件 4.2)
    - Daily screening execution (要件 4.4)
    - Environment setup and configuration
    - Orchestration of all system components
    - Comprehensive logging and monitoring
    """

    def __init__(self):
        """Initialize WorkflowRunner with logging and monitoring setup."""
        # Initialize logging first
        self.log_manager = LogManager()
        self.logger = logging.getLogger(__name__)

        self.config_manager = ConfigManager()
        self.config: Optional[Config] = None

        # Components will be initialized after config is loaded
        self.data_fetcher: Optional[DataFetcher] = None
        self.screening_engine: Optional[ScreeningEngine] = None
        self.slack_notifier: Optional[SlackNotifier] = None

        # Performance tracking
        self.start_time: Optional[datetime] = None

    def main(self) -> None:
        """
        Main entry point for the workflow execution.

        Orchestrates the complete daily screening workflow:
        1. Setup environment and configuration
        2. Check if market is open
        3. Execute daily screening if market is open
        4. Handle any errors and send notifications
        """
        self.start_time = datetime.now()
        workflow_type = "daily_screening"

        try:
            self.log_manager.log_workflow_start(workflow_type)
            self.logger.info("Starting stock value notifier workflow")

            # Perform initial health check
            self._perform_health_check()

            # Setup environment and load configuration
            self.setup_environment()

            # Check if today is a market trading day
            today = date.today()
            if not self.is_market_open(today):
                self.logger.info(f"Market is closed on {today}. Skipping screening.")
                self._log_completion_metrics(skipped=True)
                return

            # Execute daily screening
            self.execute_daily_screening()

            # Log successful completion
            duration = (datetime.now() - self.start_time).total_seconds()
            self.log_manager.log_workflow_end(workflow_type, True, duration)
            self.logger.info("Workflow completed successfully")
            self._log_completion_metrics()

        except Exception as e:
            # Log critical error with full context
            duration = (
                (datetime.now() - self.start_time).total_seconds()
                if self.start_time
                else 0
            )
            self.log_manager.log_critical_error(e, "main_workflow")
            self.log_manager.log_workflow_end(workflow_type, False, duration)

            # Try to send error notification if Slack is configured
            if self.slack_notifier:
                try:
                    self.slack_notifier.send_error_notification(e)
                    self.logger.info("Error notification sent to Slack")
                except Exception as notification_error:
                    self.log_manager.log_critical_error(
                        notification_error, "error_notification"
                    )

            # Re-raise the exception for GitHub Actions to detect failure
            raise

    def is_market_open(self, check_date: date) -> bool:
        """
        Check if the market is open on the given date.

        Args:
            check_date: Date to check for market availability

        Returns:
            bool: True if market is open, False otherwise

        Note: This implements basic weekday checking. In production,
        this should be enhanced with Japanese market holiday calendar.
        """
        # 要件 4.1, 4.2: 平日（月曜日から金曜日）のみ実行、祝日や市場休場日はスキップ

        self.logger.info(f"Checking market status for {check_date}")

        # Check if it's a weekday (Monday=0, Sunday=6)
        if check_date.weekday() >= 5:  # Saturday or Sunday
            self.logger.info(f"{check_date} is a weekend. Market is closed.")
            self.log_manager.log_system_health(
                "market_calendar",
                "CLOSED",
                {"reason": "weekend", "date": str(check_date)},
            )
            return False

        # Basic Japanese market holidays (this should be enhanced with a proper calendar)
        japanese_holidays_2024 = [
            date(2024, 1, 1),  # New Year's Day
            date(2024, 1, 8),  # Coming of Age Day
            date(2024, 2, 11),  # National Foundation Day
            date(2024, 2, 12),  # National Foundation Day (observed)
            date(2024, 2, 23),  # Emperor's Birthday
            date(2024, 3, 20),  # Vernal Equinox Day
            date(2024, 4, 29),  # Showa Day
            date(2024, 5, 3),  # Constitution Memorial Day
            date(2024, 5, 4),  # Greenery Day
            date(2024, 5, 5),  # Children's Day
            date(2024, 5, 6),  # Children's Day (observed)
            date(2024, 7, 15),  # Marine Day
            date(2024, 8, 11),  # Mountain Day
            date(2024, 8, 12),  # Mountain Day (observed)
            date(2024, 9, 16),  # Respect for the Aged Day
            date(2024, 9, 22),  # Autumnal Equinox Day
            date(2024, 9, 23),  # Autumnal Equinox Day (observed)
            date(2024, 10, 14),  # Health and Sports Day
            date(2024, 11, 3),  # Culture Day
            date(2024, 11, 4),  # Culture Day (observed)
            date(2024, 11, 23),  # Labor Thanksgiving Day
            date(2024, 12, 31),  # New Year's Eve (market closes early)
        ]

        # Check if it's a Japanese holiday
        if check_date in japanese_holidays_2024:
            self.logger.info(f"{check_date} is a Japanese holiday. Market is closed.")
            self.log_manager.log_system_health(
                "market_calendar",
                "CLOSED",
                {"reason": "holiday", "date": str(check_date)},
            )
            return False

        # Additional market-specific closures (year-end, etc.)
        if check_date.month == 12 and check_date.day >= 30:
            self.logger.info(
                f"{check_date} is during year-end closure. Market is closed."
            )
            self.log_manager.log_system_health(
                "market_calendar",
                "CLOSED",
                {"reason": "year_end", "date": str(check_date)},
            )
            return False

        if check_date.month == 1 and check_date.day <= 3:
            self.logger.info(
                f"{check_date} is during New Year closure. Market is closed."
            )
            self.log_manager.log_system_health(
                "market_calendar",
                "CLOSED",
                {"reason": "new_year", "date": str(check_date)},
            )
            return False

        self.logger.info(f"{check_date} is a trading day. Market is open.")
        self.log_manager.log_system_health(
            "market_calendar", "OPEN", {"date": str(check_date)}
        )
        return True

    def execute_daily_screening(self) -> None:
        """
        Execute the daily stock screening workflow.

        This method:
        1. Fetches stock data from yfinance
        2. Runs screening analysis
        3. Sends results via Slack notification

        Raises:
            Exception: If any step in the screening process fails
        """
        self.logger.info("Starting daily screening execution")
        screening_start = datetime.now()

        try:
            # Get list of Japanese stocks to screen
            self.logger.info("Fetching Japanese stock list")
            stock_symbols = self.data_fetcher.get_japanese_stock_list()
            self.logger.info(f"Screening {len(stock_symbols)} stocks")

            # Log data fetching start
            data_fetch_start = datetime.now()

            # Collect stock data
            stock_data_list = []
            failed_symbols = []

            for i, symbol in enumerate(stock_symbols):
                try:
                    self.logger.debug(
                        f"Fetching data for {symbol} ({i+1}/{len(stock_symbols)})"
                    )

                    # Get financial info for each stock
                    financial_info = self.data_fetcher.get_financial_info(symbol)

                    # Get dividend history
                    dividend_history = self.data_fetcher.get_dividend_history(symbol)

                    # Get price history for PER stability calculation
                    price_history = self.data_fetcher.get_stock_prices(symbol)

                    # Prepare data for screening
                    stock_data = {
                        "code": symbol,
                        "name": financial_info.get(
                            "shortName", financial_info.get("longName", symbol)
                        ),
                        "current_price": financial_info.get("currentPrice", 0),
                        "per": financial_info.get("trailingPE", float("inf")),
                        "pbr": financial_info.get("priceToBook", float("inf")),
                        "dividend_yield": (financial_info.get("dividendYield", 0) or 0)
                        * 100,  # Convert to percentage
                        "financial_data": {
                            "statements": self._extract_financial_statements(
                                financial_info, price_history
                            )
                        },
                        "dividend_data": {
                            "dividends": self._extract_dividend_data(dividend_history)
                        },
                    }

                    stock_data_list.append(stock_data)

                except Exception as e:
                    self.logger.warning(f"Failed to fetch data for {symbol}: {str(e)}")
                    failed_symbols.append(symbol)
                    continue

            # Log data fetching metrics
            data_fetch_duration = (datetime.now() - data_fetch_start).total_seconds()
            fetch_metrics = {
                "total_symbols": len(stock_symbols),
                "successful_fetches": len(stock_data_list),
                "failed_fetches": len(failed_symbols),
                "fetch_duration_seconds": data_fetch_duration,
                "success_rate": len(stock_data_list) / len(stock_symbols) * 100,
            }
            self.log_manager.log_performance_metrics(fetch_metrics)

            if failed_symbols:
                self.logger.warning(
                    f"Failed to fetch data for {len(failed_symbols)} symbols: {failed_symbols}"
                )

            # Convert to DataFrame for screening
            if not stock_data_list:
                self.logger.warning("No stock data available for screening")
                self.log_manager.log_system_health(
                    "screening", "WARNING", {"reason": "no_data"}
                )
                self.slack_notifier.send_no_stocks_notification()
                return

            stock_df = pd.DataFrame(stock_data_list)

            # Run screening
            self.logger.info("Running stock screening analysis")
            screening_analysis_start = datetime.now()

            value_stocks = self.screening_engine.screen_value_stocks(stock_df)

            screening_analysis_duration = (
                datetime.now() - screening_analysis_start
            ).total_seconds()

            # Log screening metrics
            screening_metrics = {
                "input_stocks": len(stock_data_list),
                "value_stocks_found": len(value_stocks),
                "screening_duration_seconds": screening_analysis_duration,
                "hit_rate": (
                    len(value_stocks) / len(stock_data_list) * 100
                    if stock_data_list
                    else 0
                ),
            }
            self.log_manager.log_performance_metrics(screening_metrics)

            # Send notification
            notification_start = datetime.now()

            if value_stocks:
                self.logger.info(f"Found {len(value_stocks)} value stocks")
                # Log details of found stocks
                for stock in value_stocks:
                    self.logger.info(
                        f"Value stock: {stock.name} ({stock.code}) - "
                        f"Price: ¥{stock.current_price:.0f}, PER: {stock.per:.1f}, "
                        f"PBR: {stock.pbr:.1f}, Dividend: {stock.dividend_yield:.1f}%, "
                        f"Score: {stock.score:.1f}"
                    )

                success = self.slack_notifier.send_value_stocks_notification(
                    value_stocks
                )
                if not success:
                    self.logger.error("Failed to send value stocks notification")
                    self.log_manager.log_system_health(
                        "notification", "ERROR", {"type": "value_stocks"}
                    )
                else:
                    self.log_manager.log_system_health(
                        "notification",
                        "SUCCESS",
                        {"type": "value_stocks", "count": len(value_stocks)},
                    )
            else:
                self.logger.info("No value stocks found today")
                success = self.slack_notifier.send_no_stocks_notification()
                if not success:
                    self.logger.error("Failed to send no stocks notification")
                    self.log_manager.log_system_health(
                        "notification", "ERROR", {"type": "no_stocks"}
                    )
                else:
                    self.log_manager.log_system_health(
                        "notification", "SUCCESS", {"type": "no_stocks"}
                    )

            notification_duration = (
                datetime.now() - notification_start
            ).total_seconds()

            # Log overall screening completion
            total_screening_duration = (
                datetime.now() - screening_start
            ).total_seconds()
            completion_metrics = {
                "total_duration_seconds": total_screening_duration,
                "notification_duration_seconds": notification_duration,
                "stocks_processed": len(stock_data_list),
                "value_stocks_found": len(value_stocks),
            }
            self.log_manager.log_performance_metrics(completion_metrics)

        except Exception as e:
            self.log_manager.log_critical_error(e, "daily_screening")
            raise

    def setup_environment(self) -> None:
        """
        Setup environment and initialize all system components.

        This method:
        1. Configures logging
        2. Loads configuration from environment variables
        3. Validates configuration
        4. Initializes all system components

        Raises:
            ValueError: If configuration is invalid or missing
            Exception: If component initialization fails
        """
        self.logger.info("Setting up environment and configuration")

        try:
            # Load configuration from environment variables (GitHub Secrets)
            self.logger.info("Loading configuration from environment")
            self.config = self.config_manager.load_config_from_env()

            # Validate configuration
            self.logger.info("Validating configuration")
            if not self.config_manager.validate_config(self.config):
                raise ValueError("Configuration validation failed")

            self.log_manager.log_system_health("configuration", "VALID")

            # Initialize system components
            self.logger.info("Initializing system components")

            self.data_fetcher = DataFetcher()
            self.log_manager.log_system_health("data_fetcher", "INITIALIZED")

            self.screening_engine = ScreeningEngine(self.config.screening_config)
            self.log_manager.log_system_health("screening_engine", "INITIALIZED")

            self.slack_notifier = SlackNotifier(self.config.slack_config)
            self.log_manager.log_system_health("slack_notifier", "INITIALIZED")

            self.logger.info("Environment setup completed successfully")
            self.log_manager.log_system_health("environment", "READY")

        except Exception as e:
            self.log_manager.log_critical_error(e, "environment_setup")
            raise

    def _perform_health_check(self) -> None:
        """Perform initial system health check."""
        self.logger.info("Performing initial health check")

        # Check Python version
        python_version = sys.version_info
        self.log_manager.log_system_health(
            "python",
            "CHECKED",
            {
                "version": f"{python_version.major}.{python_version.minor}.{python_version.micro}"
            },
        )

        # Check environment variables
        required_env_vars = ["SLACK_BOT_TOKEN", "SLACK_CHANNEL"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]

        if missing_vars:
            self.log_manager.log_system_health(
                "environment_variables", "ERROR", {"missing": missing_vars}
            )
        else:
            self.log_manager.log_system_health("environment_variables", "COMPLETE")

        # Check disk space for logs
        try:
            import shutil

            disk_usage = shutil.disk_usage(self.log_manager.log_dir)
            free_gb = disk_usage.free / (1024**3)

            if free_gb < 1.0:  # Less than 1GB free
                self.log_manager.log_system_health(
                    "disk_space", "WARNING", {"free_gb": free_gb}
                )
            else:
                self.log_manager.log_system_health(
                    "disk_space", "HEALTHY", {"free_gb": free_gb}
                )
        except Exception as e:
            self.log_manager.log_system_health("disk_space", "ERROR", {"error": str(e)})

    def _log_completion_metrics(self, skipped: bool = False) -> None:
        """Log workflow completion metrics."""
        if not self.start_time:
            return

        duration = (datetime.now() - self.start_time).total_seconds()

        metrics = {
            "workflow_duration_seconds": duration,
            "skipped": skipped,
            "completion_time": datetime.now().isoformat(),
        }

        self.log_manager.log_performance_metrics(metrics)

    def _extract_financial_statements(
        self, financial_info: dict, price_history: pd.DataFrame
    ) -> List[dict]:
        """
        Extract financial statements data for screening analysis.

        Args:
            financial_info: Financial information from yfinance
            price_history: Historical price data

        Returns:
            List of financial statement dictionaries
        """
        statements = []

        # Since yfinance doesn't provide historical financial statements directly,
        # we'll create a simplified structure with available data
        current_year = datetime.now().year

        # Create entries for the past 3 years using available data
        for i in range(3):
            year = current_year - i
            statement = {
                "year": year,
                "revenue": financial_info.get("totalRevenue", 0),
                "net_income": (
                    financial_info.get("totalRevenue", 0)
                    * financial_info.get("profitMargins", 0)
                    if financial_info.get("profitMargins")
                    else 0
                ),
                "per": financial_info.get("trailingPE", 0),
            }
            statements.append(statement)

        return statements

    def _extract_dividend_data(self, dividend_history: pd.DataFrame) -> List[dict]:
        """
        Extract dividend data for screening analysis.

        Args:
            dividend_history: Dividend history DataFrame from yfinance

        Returns:
            List of dividend dictionaries
        """
        dividends = []

        if dividend_history.empty:
            return dividends

        # Group dividends by year and sum them
        dividend_history["Year"] = pd.to_datetime(dividend_history["Date"]).dt.year
        yearly_dividends = (
            dividend_history.groupby("Year")["Dividends"].sum().reset_index()
        )

        for _, row in yearly_dividends.iterrows():
            dividend_entry = {
                "year": int(row["Year"]),
                "dividend": float(row["Dividends"]),
            }
            dividends.append(dividend_entry)

        return sorted(dividends, key=lambda x: x["year"], reverse=True)
