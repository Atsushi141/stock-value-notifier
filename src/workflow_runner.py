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
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd
from pathlib import Path

from .config_manager import ConfigManager, Config
from .data_fetcher import DataFetcher
from .screening_engine import ScreeningEngine
from .slack_notifier import SlackNotifier
from .rotation_manager import RotationManager
from .models import ValueStock
from .error_handling_config import ErrorHandlingConfig, ErrorHandlingConfigManager
from .error_metrics import ErrorMetrics, AlertLevel


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
        """Initialize WorkflowRunner with enhanced logging, monitoring, and error handling setup."""
        # Initialize logging first
        self.log_manager = LogManager()
        self.logger = logging.getLogger(__name__)

        self.config_manager = ConfigManager()
        self.config: Optional[Config] = None

        # Enhanced error handling configuration
        self.error_config_manager = ErrorHandlingConfigManager()
        self.error_handling_config: Optional[ErrorHandlingConfig] = None

        # Components will be initialized after config is loaded
        self.data_fetcher: Optional[DataFetcher] = None
        self.screening_engine: Optional[ScreeningEngine] = None
        self.slack_notifier: Optional[SlackNotifier] = None
        self.rotation_manager: Optional[RotationManager] = None

        # Enhanced error metrics integration
        self.error_metrics: Optional[ErrorMetrics] = None

        # Performance tracking
        self.start_time: Optional[datetime] = None

    def main(self) -> None:
        """
        Main entry point for the workflow execution with enhanced error handling integration.

        Orchestrates the complete daily screening workflow:
        1. Setup environment and configuration
        2. Check if market is open (with optional date override)
        3. Execute daily screening if market is open or forced
        4. Monitor error metrics and send alerts if needed
        5. Handle any errors and send notifications
        """
        self.start_time = datetime.now()
        workflow_type = "daily_screening"

        try:
            self.log_manager.log_workflow_start(workflow_type)
            self.logger.info(
                "Starting stock value notifier workflow with enhanced error handling"
            )

            # Perform initial health check
            self._perform_health_check()

            # Setup environment and load configuration
            self.setup_environment()

            # Get target date and force execution flag from environment
            target_date_str = os.getenv("TARGET_DATE", "")
            force_execution = os.getenv("FORCE_EXECUTION", "false").lower() == "true"

            # Determine the date to check
            if target_date_str:
                try:
                    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
                    self.logger.info(f"Using specified target date: {target_date}")
                except ValueError as e:
                    self.logger.error(
                        f"Invalid target date format: {target_date_str}. Using today."
                    )
                    target_date = date.today()
            else:
                target_date = date.today()
                self.logger.info(f"Using current date: {target_date}")

            # Check if market is open on target date
            market_open = self.is_market_open(target_date)

            if not market_open and not force_execution:
                self.logger.info(
                    f"Market is closed on {target_date}. Skipping screening."
                )
                self.logger.info("Use FORCE_EXECUTION=true to run anyway.")
                self._log_completion_metrics(skipped=True)
                return
            elif not market_open and force_execution:
                self.logger.warning(
                    f"Market is closed on {target_date}, but force execution is enabled."
                )
                self.logger.warning("Running screening despite market closure.")

            # Log the execution context
            if target_date_str:
                self.logger.info(
                    f"Executing screening for historical date: {target_date}"
                )
            else:
                self.logger.info(f"Executing screening for current date: {target_date}")

            # Execute daily screening with error monitoring
            self.execute_daily_screening(target_date)

            # Check and send error alerts after screening
            self.check_and_send_error_alerts()

            # Log comprehensive error summary
            self.log_comprehensive_error_summary()

            # Log successful completion
            duration = (datetime.now() - self.start_time).total_seconds()
            self.log_manager.log_workflow_end(workflow_type, True, duration)
            self.logger.info(
                "Workflow completed successfully with enhanced error handling"
            )
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

            # Log comprehensive error summary even on failure
            try:
                self.log_comprehensive_error_summary()
            except Exception as summary_error:
                self.logger.error(f"Failed to log error summary: {summary_error}")

            # Try to send error notification if Slack is configured
            if self.slack_notifier:
                try:
                    # Send both the main error and error metrics summary
                    error_message = f"🚨 **Critical Workflow Error**\n\n{str(e)}"

                    if self.error_metrics:
                        error_summary = self.error_metrics.get_error_summary(
                            timedelta(hours=1)
                        )
                        error_message += (
                            f"\n\n**Recent Error Rate:** {error_summary['error_rate']*100:.1f}%\n"
                            f"**Failed Operations:** {error_summary['failed_operations']}\n"
                            f"**Total Operations:** {error_summary['total_operations']}"
                        )

                    self.slack_notifier.send_message(
                        error_message,
                        channel=None,
                        username="Workflow Monitor",
                        icon_emoji=":x:",
                    )
                    self.logger.info("Critical error notification sent to Slack")
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

    def execute_daily_screening(self, target_date: date) -> None:
        """
        Execute the daily stock screening workflow with enhanced error monitoring.

        Args:
            target_date: The date for which to execute the screening

        This method:
        1. Fetches stock data from yfinance with error tracking
        2. Runs screening analysis
        3. Monitors error rates and sends alerts if needed
        4. Sends results via Slack notification

        Raises:
            Exception: If any step in the screening process fails
        """
        self.logger.info(
            "Starting daily screening execution with enhanced error monitoring"
        )
        screening_start = datetime.now()

        try:
            # Reset error metrics for this screening session
            if self.error_metrics:
                self.error_metrics.reset_metrics()

            # Get list of Japanese stocks to screen
            self.logger.info("Fetching Japanese stock list")

            # Check if full screening mode is enabled via environment variable
            screening_mode = os.getenv(
                "SCREENING_MODE", "curated"
            )  # "curated", "all", or "rotation"
            analysis_period = os.getenv("ANALYSIS_PERIOD", "3y")  # "1y", "2y", "3y"

            if screening_mode == "all":
                self.logger.warning(
                    "Full screening mode enabled - this will analyze ~3800 stocks and may take several hours"
                )
            elif screening_mode == "rotation":
                self.logger.info(
                    "Rotation mode enabled - analyzing a subset of stocks based on weekday rotation"
                )

            # Get list of Japanese stocks to screen
            if screening_mode == "rotation":
                # For rotation mode, get all stocks first, then filter by rotation
                all_stock_symbols = self.data_fetcher.get_japanese_stock_list(
                    mode="tse_official"  # Use TSE official list for better coverage
                )

                # Use target_date for rotation if specified
                rotation_date = (
                    datetime.combine(target_date, datetime.min.time())
                    if target_date != date.today()
                    else None
                )

                stock_symbols = self.rotation_manager.get_stocks_for_today(
                    all_stock_symbols, rotation_date
                )

                # Get rotation info for notifications
                rotation_info = self.rotation_manager.get_group_info(rotation_date)
                self.logger.info(
                    f"Rotation mode: Processing {rotation_info['progress_text_jp']} "
                    f"({len(stock_symbols)} stocks) for date {target_date}"
                )
            else:
                stock_symbols = self.data_fetcher.get_japanese_stock_list(
                    mode=screening_mode
                )
            self.logger.info(
                f"Screening {len(stock_symbols)} stocks (mode: {screening_mode}, period: {analysis_period})"
            )

            # Send start notification
            if screening_mode == "rotation":
                rotation_info = self.rotation_manager.get_group_info(rotation_date)
                self.slack_notifier.send_analysis_start_notification(
                    len(stock_symbols), screening_mode, rotation_info
                )
            else:
                self.slack_notifier.send_analysis_start_notification(
                    len(stock_symbols), screening_mode
                )

            # Log data fetching start
            data_fetch_start = datetime.now()

            # Collect stock data with progress notifications and error monitoring
            stock_data_list = []
            failed_symbols = []
            batch_processed = []
            progress_interval = (
                100
                if screening_mode == "all"
                else 100 if screening_mode == "rotation" else 50
            )  # Progress notification interval

            for i, symbol in enumerate(stock_symbols):
                try:
                    self.logger.debug(
                        f"Fetching data for {symbol} ({i+1}/{len(stock_symbols)})"
                    )

                    # Get financial info for each stock
                    financial_info = self.data_fetcher.get_financial_info(symbol)

                    # Get dividend history (use shorter period for performance)
                    dividend_history = self.data_fetcher.get_dividend_history(
                        symbol, period=analysis_period
                    )

                    # Get price history for PER stability calculation (shorter period)
                    price_history = self.data_fetcher.get_stock_prices(
                        symbol, period=analysis_period
                    )

                    # Prepare data for screening with proper NaN handling
                    stock_data = {
                        "code": symbol,
                        "name": financial_info.get(
                            "shortName", financial_info.get("longName", symbol)
                        ),
                        "current_price": financial_info.get("currentPrice", 0) or 0,
                        "per": (
                            financial_info.get("trailingPE")
                            if financial_info.get("trailingPE") is not None
                            and not pd.isna(financial_info.get("trailingPE"))
                            else float("inf")
                        ),
                        "pbr": (
                            financial_info.get("priceToBook")
                            if financial_info.get("priceToBook") is not None
                            and not pd.isna(financial_info.get("priceToBook"))
                            else float("inf")
                        ),
                        "dividend_yield": (
                            financial_info.get("dividendYield", 0) or 0
                        ),  # Already in percentage format
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
                    batch_processed.append(financial_info.get("shortName", symbol))

                    # Record successful operation in error metrics
                    if self.error_metrics:
                        self.error_metrics.record_success(
                            symbol=symbol,
                            operation="stock_data_fetch",
                            additional_info={"screening_mode": screening_mode},
                        )

                    # Send progress notification
                    if (i + 1) % progress_interval == 0 or i + 1 == len(stock_symbols):
                        current_stock = financial_info.get("shortName", symbol)
                        recent_batch = batch_processed[
                            -min(9, len(batch_processed)) :
                        ]  # Last 9 stocks

                        self.slack_notifier.send_progress_notification(
                            i + 1, len(stock_symbols), current_stock, recent_batch
                        )

                        # Clear batch for next progress update
                        batch_processed = []

                        # Check error rate and send alert if needed during processing
                        if (
                            self.error_metrics
                            and (i + 1) % (progress_interval * 2) == 0
                        ):
                            if self.error_metrics.should_alert():
                                self.check_and_send_error_alerts()

                except Exception as e:
                    self.logger.warning(f"Failed to fetch data for {symbol}: {str(e)}")
                    failed_symbols.append(symbol)

                    # Record error in error metrics
                    if self.error_metrics:
                        from .error_metrics import ErrorType

                        error_type = ErrorType.from_exception(e)
                        self.error_metrics.record_error(
                            error_type=error_type,
                            symbol=symbol,
                            operation="stock_data_fetch",
                            details=str(e),
                            additional_info={"screening_mode": screening_mode},
                        )
                    continue

            # Log data fetching metrics with error information
            data_fetch_duration = (datetime.now() - data_fetch_start).total_seconds()
            fetch_metrics = {
                "total_symbols": len(stock_symbols),
                "successful_fetches": len(stock_data_list),
                "failed_fetches": len(failed_symbols),
                "fetch_duration_seconds": data_fetch_duration,
                "success_rate": len(stock_data_list) / len(stock_symbols) * 100,
            }

            # Add error metrics if available
            if self.error_metrics:
                error_summary = self.error_metrics.get_error_summary(
                    datetime.now() - data_fetch_start
                )
                fetch_metrics.update(
                    {
                        "error_rate": error_summary["error_rate"],
                        "error_by_type": error_summary["error_by_type"],
                    }
                )

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
                self.slack_notifier.send_no_stocks_notification([])
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

            # Extract all stock names for notification
            all_stock_names = [stock_data["name"] for stock_data in stock_data_list]

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

                # Include rotation info in notification if in rotation mode
                rotation_info = None
                if screening_mode == "rotation":
                    rotation_info = self.rotation_manager.get_group_info(rotation_date)

                # Get target date for notification
                target_date_str = os.getenv("TARGET_DATE", "")

                success = self.slack_notifier.send_value_stocks_notification(
                    value_stocks, all_stock_names, rotation_info, target_date_str
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

                # Include rotation info in notification if in rotation mode
                rotation_info = None
                if screening_mode == "rotation":
                    rotation_info = self.rotation_manager.get_group_info(rotation_date)

                # Get target date for notification
                target_date_str = os.getenv("TARGET_DATE", "")

                success = self.slack_notifier.send_no_stocks_notification(
                    all_stock_names, rotation_info, target_date
                )
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

            # Log overall screening completion with error metrics
            total_screening_duration = (
                datetime.now() - screening_start
            ).total_seconds()
            completion_metrics = {
                "total_duration_seconds": total_screening_duration,
                "notification_duration_seconds": notification_duration,
                "stocks_processed": len(stock_data_list),
                "value_stocks_found": len(value_stocks),
            }

            # Add final error metrics
            if self.error_metrics:
                final_error_summary = self.error_metrics.get_error_summary()
                completion_metrics.update(
                    {
                        "final_error_rate": final_error_summary["error_rate"],
                        "total_errors": final_error_summary["failed_operations"],
                        "should_alert": final_error_summary["should_alert"],
                    }
                )

            self.log_manager.log_performance_metrics(completion_metrics)

        except Exception as e:
            self.log_manager.log_critical_error(e, "daily_screening")

            # Record critical error in error metrics
            if self.error_metrics:
                from .error_metrics import ErrorType, AlertLevel

                self.error_metrics.record_error(
                    error_type=ErrorType.UNKNOWN,
                    symbol="SYSTEM",
                    operation="daily_screening",
                    details=str(e),
                    severity=AlertLevel.CRITICAL,
                )
            raise

    def setup_environment(self) -> None:
        """
        Setup environment and initialize all system components with enhanced error handling.

        This method:
        1. Configures logging
        2. Loads configuration from environment variables
        3. Loads enhanced error handling configuration
        4. Validates configuration
        5. Initializes all system components with integrated error handling

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

            # Load enhanced error handling configuration
            self.logger.info("Loading enhanced error handling configuration")
            try:
                self.error_handling_config = (
                    self.error_config_manager.load_config_from_env()
                )

                if self.error_config_manager.validate_config(
                    self.error_handling_config
                ):
                    self.log_manager.log_system_health("error_handling_config", "VALID")
                    self.logger.info(
                        f"Error handling mode: {self.error_handling_config.mode.value}, "
                        f"Continue on error: {self.error_handling_config.continue_on_individual_error}"
                    )
                else:
                    self.logger.warning(
                        "Error handling configuration validation failed, using defaults"
                    )
                    self.error_handling_config = None

            except Exception as e:
                self.logger.warning(
                    f"Failed to load error handling configuration: {e}, using defaults"
                )
                self.error_handling_config = None

            # Initialize system components with enhanced error handling
            self.logger.info(
                "Initializing system components with enhanced error handling"
            )

            # Initialize DataFetcher with enhanced error handling
            if self.error_handling_config:
                self.data_fetcher = DataFetcher(
                    error_handling_config=self.error_handling_config,
                    enable_enhanced_features=True,
                )
                self.logger.info("DataFetcher initialized with enhanced error handling")
            else:
                self.data_fetcher = DataFetcher()
                self.logger.info("DataFetcher initialized with default configuration")

            self.log_manager.log_system_health("data_fetcher", "INITIALIZED")

            # Get error metrics from DataFetcher for integration
            self.error_metrics = self.data_fetcher.get_error_metrics()
            self.log_manager.log_system_health("error_metrics", "INTEGRATED")

            self.screening_engine = ScreeningEngine(self.config.screening_config)
            self.log_manager.log_system_health("screening_engine", "INITIALIZED")

            self.slack_notifier = SlackNotifier(self.config.slack_config)
            self.log_manager.log_system_health("slack_notifier", "INITIALIZED")

            self.rotation_manager = RotationManager()
            self.log_manager.log_system_health("rotation_manager", "INITIALIZED")

            self.logger.info(
                "Environment setup completed successfully with enhanced error handling"
            )
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

    def get_error_handling_status(self) -> Dict[str, Any]:
        """
        Get comprehensive error handling status from all components.

        Returns:
            Dictionary with error handling status from all integrated components
        """
        status = {
            "workflow_runner": {
                "error_handling_config_loaded": self.error_handling_config is not None,
                "error_metrics_integrated": self.error_metrics is not None,
            }
        }

        if self.data_fetcher:
            status["data_fetcher"] = self.data_fetcher.get_error_handling_status()

        if self.error_metrics:
            status["error_metrics"] = {
                "summary": self.error_metrics.get_error_summary(timedelta(hours=1)),
                "should_alert": self.error_metrics.should_alert(),
                "recent_errors": len(self.error_metrics.get_recent_errors(10)),
            }

        return status

    def check_and_send_error_alerts(self) -> bool:
        """
        Check error metrics and send alerts if thresholds are exceeded.

        Returns:
            True if alerts were sent, False otherwise
        """
        if not self.error_metrics or not self.slack_notifier:
            return False

        if self.error_metrics.should_alert():
            try:
                error_summary = self.error_metrics.get_error_summary(timedelta(hours=1))

                alert_message = (
                    f"🚨 **Error Alert - Stock Value Notifier**\n\n"
                    f"**Error Rate:** {error_summary['error_rate']*100:.1f}% "
                    f"(Threshold: {self.error_metrics.error_threshold*100:.1f}%)\n"
                    f"**Total Operations:** {error_summary['total_operations']}\n"
                    f"**Failed Operations:** {error_summary['failed_operations']}\n"
                    f"**Time Window:** {error_summary['time_window_hours']:.1f} hours\n\n"
                )

                if error_summary["error_by_type"]:
                    alert_message += "**Error Breakdown:**\n"
                    for error_type, count in error_summary["error_by_type"].items():
                        alert_message += f"• {error_type}: {count}\n"

                if error_summary["top_problematic_symbols"]:
                    alert_message += "\n**Most Problematic Symbols:**\n"
                    for symbol, count in list(
                        error_summary["top_problematic_symbols"].items()
                    )[:5]:
                        alert_message += f"• {symbol}: {count} errors\n"

                # Send alert via Slack
                success = self.slack_notifier.send_message(
                    alert_message,
                    channel=None,  # Use default channel
                    username="Error Monitor",
                    icon_emoji=":warning:",
                )

                if success:
                    self.logger.warning("Error alert sent successfully")
                    self.log_manager.log_system_health(
                        "error_alerting",
                        "ALERT_SENT",
                        {"error_rate": error_summary["error_rate"]},
                    )
                else:
                    self.logger.error("Failed to send error alert")
                    self.log_manager.log_system_health("error_alerting", "ALERT_FAILED")

                return success

            except Exception as e:
                self.logger.error(f"Error sending alert: {e}")
                self.log_manager.log_critical_error(e, "error_alerting")
                return False

        return False

    def log_comprehensive_error_summary(self) -> None:
        """
        Log a comprehensive error summary for monitoring and debugging.
        """
        if not self.error_metrics:
            return

        try:
            # Get error summary for different time windows
            summary_1h = self.error_metrics.get_error_summary(timedelta(hours=1))
            summary_24h = self.error_metrics.get_error_summary(timedelta(hours=24))

            self.logger.info("=== COMPREHENSIVE ERROR SUMMARY ===")
            self.logger.info(
                f"1-Hour Window: {summary_1h['error_rate']*100:.1f}% error rate, "
                f"{summary_1h['total_operations']} operations"
            )
            self.logger.info(
                f"24-Hour Window: {summary_24h['error_rate']*100:.1f}% error rate, "
                f"{summary_24h['total_operations']} operations"
            )

            if summary_1h["error_by_type"]:
                self.logger.info("Recent Error Types:")
                for error_type, count in summary_1h["error_by_type"].items():
                    self.logger.info(f"  {error_type}: {count}")

            if summary_1h["top_problematic_symbols"]:
                self.logger.info("Problematic Symbols (1h):")
                for symbol, count in list(
                    summary_1h["top_problematic_symbols"].items()
                )[:5]:
                    self.logger.info(f"  {symbol}: {count} errors")

            # Log to health system
            self.log_manager.log_system_health(
                "error_summary",
                "LOGGED",
                {
                    "1h_error_rate": summary_1h["error_rate"],
                    "24h_error_rate": summary_24h["error_rate"],
                    "1h_operations": summary_1h["total_operations"],
                    "24h_operations": summary_24h["total_operations"],
                },
            )

        except Exception as e:
            self.logger.error(f"Error logging comprehensive summary: {e}")

    def reset_error_metrics(self) -> None:
        """
        Reset error metrics for fresh monitoring period.
        """
        if self.error_metrics:
            self.error_metrics.reset_metrics()
            self.logger.info("Error metrics reset for fresh monitoring period")

        if self.data_fetcher:
            self.data_fetcher.reset_retry_statistics()
            self.data_fetcher.reset_error_handling_state()
            self.logger.info("DataFetcher error handling state reset")


# Convenience factory functions for creating WorkflowRunner instances


def create_workflow_runner_with_enhanced_error_handling() -> WorkflowRunner:
    """
    Create WorkflowRunner with enhanced error handling enabled.

    Returns:
        WorkflowRunner configured for enhanced error handling
    """
    runner = WorkflowRunner()
    # Enhanced error handling will be configured during setup_environment()
    return runner


def create_workflow_runner_for_testing(
    error_handling_config: Optional[ErrorHandlingConfig] = None,
) -> WorkflowRunner:
    """
    Create WorkflowRunner for testing with optional error handling configuration.

    Args:
        error_handling_config: Optional error handling configuration for testing

    Returns:
        WorkflowRunner configured for testing
    """
    runner = WorkflowRunner()

    if error_handling_config:
        runner.error_handling_config = error_handling_config

    return runner
