"""Slack notification module for value stock alerts."""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from .models import ValueStock, SlackConfig


class SlackNotifier:
    """Handles Slack notifications for value stock alerts."""

    def __init__(self, config: SlackConfig):
        """Initialize SlackNotifier with configuration.

        Args:
            config: SlackConfig containing token, channel, and other settings
        """
        self.config = config
        self.client = WebClient(token=config.token)
        self.logger = logging.getLogger(__name__)

    def send_value_stocks_notification(
        self,
        stocks: List[ValueStock],
        all_stocks: List[str] = None,
        group_info: dict = None,
        target_date: str = None,
        csv_files: Dict[str, str] = None,
    ) -> bool:
        """Send notification about found value stocks with optional CSV files.

        Args:
            stocks: List of ValueStock objects to notify about
            all_stocks: List of all stock names that were analyzed
            group_info: Optional rotation group information for progress display
            target_date: Optional target date for analysis (YYYY-MM-DD format)
            csv_files: Optional dictionary mapping file types to file paths

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        if not stocks:
            return self.send_no_stocks_notification(
                all_stocks, group_info, target_date, csv_files
            )

        try:
            message = self.format_value_stocks_message_bilingual(
                stocks, all_stocks, group_info, target_date
            )

            # Send main message first
            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=message,
                username=self.config.username,
                icon_emoji=self.config.icon_emoji,
            )

            # Upload CSV files if provided
            if csv_files:
                self._upload_csv_files(csv_files, stocks, target_date)

            self.logger.info(
                f"Successfully sent value stocks notification to {self.config.channel}"
            )
            return True

        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
            return self._handle_slack_error(e)
        except Exception as e:
            self.logger.error(f"Unexpected error sending notification: {str(e)}")
            return False

    def send_no_stocks_notification(
        self,
        all_stocks: List[str] = None,
        group_info: dict = None,
        target_date: str = None,
        csv_files: Dict[str, str] = None,
    ) -> bool:
        """Send notification when no value stocks are found.

        Args:
            all_stocks: List of all stock names that were analyzed
            group_info: Optional rotation group information for progress display
            target_date: Optional target date for analysis (YYYY-MM-DD format)
            csv_files: Optional dictionary mapping file types to file paths

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        try:
            message = self.format_no_stocks_message_bilingual(
                all_stocks, group_info, target_date
            )

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=message,
                username=self.config.username,
                icon_emoji=self.config.icon_emoji,
            )

            # Upload CSV files if provided
            if csv_files:
                self._upload_csv_files(csv_files, [], target_date)

            self.logger.info(
                f"Successfully sent no stocks notification to {self.config.channel}"
            )
            return True

        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
            return self._handle_slack_error(e)
        except Exception as e:
            self.logger.error(
                f"Unexpected error sending no stocks notification: {str(e)}"
            )
            return False

    def format_value_stocks_message_bilingual(
        self,
        stocks: List[ValueStock],
        all_stocks: List[str] = None,
        group_info: dict = None,
        target_date: str = None,
    ) -> str:
        """Format value stocks message in both Japanese and English.

        Args:
            stocks: List of ValueStock objects to format
            all_stocks: List of all stock names that were analyzed
            group_info: Optional rotation group information for progress display
            target_date: Optional target date for analysis (YYYY-MM-DD format)

        Returns:
            str: Formatted bilingual message with Japanese first, then English
        """
        # Japanese message first (as per requirement 3.3)
        japanese_msg = self._format_japanese_stocks_message(
            stocks, all_stocks, group_info, target_date
        )

        # English message second
        english_msg = self._format_english_stocks_message(
            stocks, all_stocks, group_info, target_date
        )

        # Combine with clear separator
        return japanese_msg + "\n" + "─" * 50 + "\n\n" + english_msg

    def _format_japanese_stocks_message(
        self,
        stocks: List[ValueStock],
        all_stocks: List[str] = None,
        group_info: dict = None,
        target_date: str = None,
    ) -> str:
        """Format Japanese stocks message with readable formatting.

        Args:
            stocks: List of ValueStock objects to format
            all_stocks: List of all stock names that were analyzed
            group_info: Optional rotation group information for progress display
            target_date: Optional target date for analysis (YYYY-MM-DD format)
            all_stocks: List of all stock names that were analyzed
            group_info: Optional rotation group information for progress display

        Returns:
            str: Formatted Japanese message
        """
        # Add rotation group info if provided (要件 7.5)
        if group_info:
            title = f"🎯 **本日のバリュー銘柄** - {group_info['progress_text_jp']}"
        else:
            title = "🎯 **本日のバリュー銘柄**"

        # Add date information if target date is specified
        if target_date:
            from datetime import datetime

            try:
                date_obj = datetime.strptime(target_date, "%Y-%m-%d")
                date_str = date_obj.strftime("%Y年%m月%d日")
                title += f" ({date_str})"
            except ValueError:
                title += f" ({target_date})"

        msg = title + "\n\n"

        for i, stock in enumerate(stocks, 1):
            msg += f"**{i}. {stock.name} ({stock.code})**\n"
            msg += f"┌─ 株価情報\n"
            msg += f"│  現在株価: ¥{stock.current_price:,.0f}\n"
            msg += f"│  PER: {stock.per:.1f}倍 | PBR: {stock.pbr:.1f}倍\n"
            msg += f"│  配当利回り: {stock.dividend_yield:.1f}%\n"
            msg += f"└─ 成長実績\n"
            msg += f"   継続増配: {stock.dividend_growth_years}年 | "
            msg += f"増収: {stock.revenue_growth_years}年 | "
            msg += f"増益: {stock.profit_growth_years}年\n\n"

        # Add analyzed stocks summary with rotation info
        if all_stocks:
            if group_info:
                msg += f"\n📊 **本日の分析対象銘柄** ({len(all_stocks)}銘柄) - {group_info['weekday_jp']}\n"
            else:
                msg += f"\n📊 **分析対象銘柄** ({len(all_stocks)}銘柄)\n"
            msg += "```\n"
            # Display stocks in columns for better readability
            for i in range(0, len(all_stocks), 3):
                row_stocks = all_stocks[i : i + 3]
                msg += " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"
            msg += "```\n"

        return msg

    def _format_english_stocks_message(
        self,
        stocks: List[ValueStock],
        all_stocks: List[str] = None,
        group_info: dict = None,
        target_date: str = None,
    ) -> str:
        """Format English stocks message with readable formatting.

        Args:
            stocks: List of ValueStock objects to format
            all_stocks: List of all stock names that were analyzed
            group_info: Optional rotation group information for progress display

        Returns:
            str: Formatted English message
        """
        # Add rotation group info if provided (要件 7.5)
        if group_info:
            title = f"🎯 **Today's Value Stocks** - {group_info['progress_text_en']}"
        else:
            title = "🎯 **Today's Value Stocks**"

        # Add date information if target date is specified
        if target_date:
            from datetime import datetime

            try:
                date_obj = datetime.strptime(target_date, "%Y-%m-%d")
                date_str = date_obj.strftime("%B %d, %Y")
                title += f" ({date_str})"
            except ValueError:
                title += f" ({target_date})"

        msg = title + "\n\n"

        for i, stock in enumerate(stocks, 1):
            msg += f"**{i}. {stock.name} ({stock.code})**\n"
            msg += f"┌─ Stock Information\n"
            msg += f"│  Current Price: ¥{stock.current_price:,.0f}\n"
            msg += f"│  PER: {stock.per:.1f}x | PBR: {stock.pbr:.1f}x\n"
            msg += f"│  Dividend Yield: {stock.dividend_yield:.1f}%\n"
            msg += f"└─ Growth Track Record\n"
            msg += f"   Dividend Growth: {stock.dividend_growth_years}yrs | "
            msg += f"Revenue: {stock.revenue_growth_years}yrs | "
            msg += f"Profit: {stock.profit_growth_years}yrs\n\n"

        # Add analyzed stocks summary with rotation info
        if all_stocks:
            if group_info:
                msg += f"\n📊 **Today's Analyzed Stocks** ({len(all_stocks)} stocks) - {group_info['weekday_en']}\n"
            else:
                msg += f"\n📊 **Analyzed Stocks** ({len(all_stocks)} stocks)\n"
            msg += "```\n"
            # Display stocks in columns for better readability
            for i in range(0, len(all_stocks), 3):
                row_stocks = all_stocks[i : i + 3]
                msg += " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"
            msg += "```\n"

        return msg

    def format_no_stocks_message_bilingual(
        self,
        all_stocks: List[str] = None,
        group_info: dict = None,
        target_date: str = None,
    ) -> str:
        """Format no stocks found message in both Japanese and English.

        Args:
            all_stocks: List of all stock names that were analyzed
            group_info: Optional rotation group information for progress display

        Returns:
            str: Formatted bilingual message with Japanese first, then English
        """
        # Japanese message with rotation info
        if group_info:
            japanese_msg = f"📊 **本日の結果** - {group_info['progress_text_jp']}\n\n"
        else:
            japanese_msg = "📊 **本日の結果**\n\n"

        japanese_msg += "本日はバリュー銘柄が見つかりませんでした。\n"
        japanese_msg += "明日も引き続き監視いたします。"

        # English message with rotation info
        if group_info:
            english_msg = (
                f"📊 **Today's Results** - {group_info['progress_text_en']}\n\n"
            )
        else:
            english_msg = "📊 **Today's Results**\n\n"

        english_msg += "No value stocks found today.\n"
        english_msg += "We'll continue monitoring tomorrow."

        # Add analyzed stocks summary with rotation info
        if all_stocks:
            if group_info:
                japanese_msg += f"\n\n📊 **本日の分析対象銘柄** ({len(all_stocks)}銘柄) - {group_info['weekday_jp']}\n"
                english_msg += f"\n\n📊 **Today's Analyzed Stocks** ({len(all_stocks)} stocks) - {group_info['weekday_en']}\n"
            else:
                japanese_msg += f"\n\n📊 **分析対象銘柄** ({len(all_stocks)}銘柄)\n"
                english_msg += (
                    f"\n\n📊 **Analyzed Stocks** ({len(all_stocks)} stocks)\n"
                )

            japanese_msg += "```\n"
            english_msg += "```\n"

            # Display stocks in columns for better readability
            for i in range(0, len(all_stocks), 3):
                row_stocks = all_stocks[i : i + 3]
                japanese_msg += (
                    " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"
                )
                english_msg += " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"

            japanese_msg += "```"
            english_msg += "```"

        return japanese_msg + "\n\n" + "─" * 50 + "\n\n" + english_msg

    def send_progress_notification(
        self,
        current: int,
        total: int,
        current_stock: str = "",
        batch_results: List[str] = None,
    ) -> bool:
        """Send progress notification during long-running analysis.

        Args:
            current: Current progress count
            total: Total items to process
            current_stock: Currently processing stock name
            batch_results: List of stocks processed in current batch

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        try:
            progress_percent = (current / total) * 100

            msg = f"📊 **スクリーニング進捗 / Screening Progress**\n\n"
            msg += f"進捗: {current:,} / {total:,} 銘柄 ({progress_percent:.1f}%)\n"
            msg += f"Progress: {current:,} / {total:,} stocks ({progress_percent:.1f}%)\n\n"

            if current_stock:
                msg += f"現在処理中: {current_stock}\n"
                msg += f"Currently processing: {current_stock}\n\n"

            # Add progress bar
            bar_length = 20
            filled_length = int(bar_length * current // total)
            bar = "█" * filled_length + "░" * (bar_length - filled_length)
            msg += f"[{bar}] {progress_percent:.1f}%\n\n"

            if batch_results:
                msg += f"直近処理銘柄 / Recent stocks:\n"
                msg += "```\n"
                for i in range(0, len(batch_results), 3):
                    row_stocks = batch_results[i : i + 3]
                    msg += " | ".join(f"{stock:<15}" for stock in row_stocks) + "\n"
                msg += "```"

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=msg,
                username=self.config.username,
                icon_emoji=":hourglass_flowing_sand:",
            )

            self.logger.info(f"Sent progress notification: {current}/{total}")
            return True

        except Exception as e:
            self.logger.warning(f"Failed to send progress notification: {str(e)}")
            return False

    def send_analysis_start_notification(
        self, total_stocks: int, mode: str, group_info: dict = None
    ) -> bool:
        """Send notification when analysis starts.

        Args:
            total_stocks: Total number of stocks to analyze
            mode: Analysis mode ("curated", "all", or "rotation")
            group_info: Optional rotation group information for progress display

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        try:
            if mode == "rotation" and group_info:
                # Rotation mode notification (要件 7.5)
                msg = f"🔄 **ローテーションスクリーニング開始** - {group_info['progress_text_jp']}\n\n"
                msg += f"本日の分析対象: {total_stocks:,} 銘柄 ({group_info['weekday_jp']})\n"
                msg += f"予想実行時間: 5-10分\n\n"
                msg += f"🔄 **Rotation Screening Started** - {group_info['progress_text_en']}\n\n"
                msg += f"Today's target: {total_stocks:,} stocks ({group_info['weekday_en']})\n"
                msg += f"Estimated time: 5-10 minutes\n\n"
                msg += f"📅 **週次進捗 / Weekly Progress:** {group_info['group_number']}/{group_info['total_groups']} 完了予定"
            elif mode == "all":
                msg = f"🚀 **週次全銘柄スクリーニング開始**\n\n"
                msg += f"分析対象: {total_stocks:,} 銘柄\n"
                msg += f"予想実行時間: 2-4時間\n\n"
                msg += f"🚀 **Weekly Full Stock Screening Started**\n\n"
                msg += f"Analyzing: {total_stocks:,} stocks\n"
                msg += f"Estimated time: 2-4 hours\n\n"
                msg += f"進捗は100銘柄ごとに通知します。\n"
                msg += f"Progress will be reported every 100 stocks."
            else:
                msg = f"📊 **日次バリュー銘柄スクリーニング開始**\n\n"
                msg += f"分析対象: {total_stocks:,} 銘柄\n"
                msg += f"予想実行時間: 10-15分\n\n"
                msg += f"📊 **Daily Value Stock Screening Started**\n\n"
                msg += f"Analyzing: {total_stocks:,} stocks\n"
                msg += f"Estimated time: 10-15 minutes"

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=msg,
                username=self.config.username,
                icon_emoji=":rocket:",
            )

            self.logger.info(f"Sent analysis start notification for {mode} mode")
            return True

        except Exception as e:
            self.logger.warning(f"Failed to send start notification: {str(e)}")
            return False

    def send_error_notification(self, error: Exception) -> bool:
        """Send error notification to administrators.

        Args:
            error: Exception that occurred

        Returns:
            bool: True if error notification was sent successfully, False otherwise
        """
        try:
            error_msg = f"🚨 **システムエラー / System Error**\n\n"
            error_msg += f"エラーが発生しました / An error occurred:\n"
            error_msg += f"```{str(error)}```\n\n"
            error_msg += f"システム管理者に連絡してください。\n"
            error_msg += f"Please contact the system administrator."

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=error_msg,
                username=self.config.username,
                icon_emoji=":warning:",
            )

            self.logger.info(
                f"Successfully sent error notification to {self.config.channel}"
            )
            return True

        except SlackApiError as e:
            self.logger.error(
                f"Failed to send error notification: {e.response['error']}"
            )
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending error notification: {str(e)}")
            return False

    def _handle_slack_error(self, error: SlackApiError) -> bool:
        """Handle Slack API errors with appropriate responses.

        Args:
            error: SlackApiError that occurred

        Returns:
            bool: False (indicating failure)
        """
        error_code = error.response.get("error", "unknown")
        error_details = {
            "error_code": error_code,
            "channel": self.config.channel,
            "timestamp": logging.Formatter().formatTime(
                logging.LogRecord(
                    name="",
                    level=0,
                    pathname="",
                    lineno=0,
                    msg="",
                    args=(),
                    exc_info=None,
                )
            ),
        }

        if error_code == "channel_not_found":
            self.logger.error(
                f"Channel {self.config.channel} not found. "
                f"Please verify the channel exists and the bot has access."
            )
            return self._try_fallback_notification(error_details)

        elif error_code == "invalid_auth":
            self.logger.error(
                "Invalid Slack token. Please check SLACK_BOT_TOKEN secret."
            )
            self._log_admin_alert("Authentication failed - check Slack token")

        elif error_code == "not_in_channel":
            self.logger.error(
                f"Bot not invited to channel {self.config.channel}. "
                f"Please invite the bot to the channel."
            )
            return self._try_fallback_notification(error_details)

        elif error_code == "channel_is_archived":
            self.logger.error(
                f"Channel {self.config.channel} is archived. "
                f"Please unarchive or use a different channel."
            )
            return self._try_fallback_notification(error_details)

        elif error_code == "msg_too_long":
            self.logger.error(
                "Message too long for Slack. Attempting to split message."
            )
            return self._handle_long_message_error()

        elif error_code == "rate_limited":
            retry_after = error.response.get("headers", {}).get("Retry-After", "60")
            self.logger.warning(
                f"Rate limited. Should retry after {retry_after} seconds."
            )
            self._log_admin_alert(f"Rate limited - retry after {retry_after}s")

        else:
            self.logger.error(
                f"Unexpected Slack API error: {error_code} - {error.response}"
            )
            self._log_admin_alert(f"Unexpected Slack error: {error_code}")

        return False

    def _try_fallback_notification(self, error_details: dict) -> bool:
        """Try to send notification to a fallback channel or method.

        Args:
            error_details: Dictionary containing error information

        Returns:
            bool: True if fallback notification succeeded, False otherwise
        """
        self.logger.info("Attempting fallback notification methods")

        # Try common fallback channels
        fallback_channels = ["#general", "#alerts", "#notifications"]

        for channel in fallback_channels:
            if channel != self.config.channel:
                try:
                    fallback_msg = self._create_fallback_error_message(error_details)

                    response = self.client.chat_postMessage(
                        channel=channel,
                        text=fallback_msg,
                        username=self.config.username,
                        icon_emoji=":warning:",
                    )

                    self.logger.info(
                        f"Successfully sent fallback notification to {channel}"
                    )
                    return True

                except SlackApiError as e:
                    self.logger.debug(
                        f"Fallback channel {channel} also failed: {e.response.get('error')}"
                    )
                    continue

        # If all fallback channels fail, log the error for admin review
        self._log_admin_alert(
            "All notification channels failed - manual intervention required"
        )
        return False

    def _handle_long_message_error(self) -> bool:
        """Handle message too long error by splitting the message.

        Returns:
            bool: True if message was successfully split and sent, False otherwise
        """
        # This would need to be implemented based on the specific message being sent
        # For now, just log the issue
        self.logger.error("Message splitting not yet implemented")
        self._log_admin_alert("Message too long - splitting feature needed")
        return False

    def _create_fallback_error_message(self, error_details: dict) -> str:
        """Create a fallback error message for administrators.

        Args:
            error_details: Dictionary containing error information

        Returns:
            str: Formatted error message
        """
        msg = "🚨 **Slack通知エラー / Slack Notification Error**\n\n"
        msg += f"**エラー詳細 / Error Details:**\n"
        msg += f"• エラーコード / Error Code: `{error_details['error_code']}`\n"
        msg += f"• 対象チャンネル / Target Channel: `{error_details['channel']}`\n"
        msg += f"• 発生時刻 / Timestamp: {error_details['timestamp']}\n\n"
        msg += f"**対応が必要です / Action Required:**\n"
        msg += f"システム管理者は設定を確認してください。\n"
        msg += f"System administrator should check the configuration."

        return msg

    def _log_admin_alert(self, message: str) -> None:
        """Log an alert message for system administrators.

        Args:
            message: Alert message to log
        """
        alert_msg = f"ADMIN ALERT: {message}"
        self.logger.critical(alert_msg)

        # In a production environment, this could also:
        # - Send email alerts
        # - Write to a separate alert log file
        # - Send to monitoring systems
        # - Create GitHub issues automatically

    def send_rotation_summary_notification(
        self, group_info: dict, week_progress: dict = None
    ) -> bool:
        """Send weekly rotation summary notification.

        Args:
            group_info: Current rotation group information
            week_progress: Optional weekly progress summary

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        try:
            msg = f"📅 **週次ローテーション進捗サマリー / Weekly Rotation Progress Summary**\n\n"

            # Current day info
            msg += f"**本日完了 / Today Completed:** {group_info['progress_text_jp']}\n"
            msg += f"**Today Completed:** {group_info['progress_text_en']}\n\n"

            # Weekly progress if provided
            if week_progress:
                msg += f"**週次進捗 / Weekly Progress:**\n"
                for day_idx, day_info in week_progress.items():
                    status = "✅" if day_info.get("completed", False) else "⏳"
                    msg += f"{status} {day_info['weekday_jp']} / {day_info['weekday_en']}: {day_info.get('stocks_analyzed', 0)} 銘柄\n"
                msg += "\n"

            # Next day preview
            msg += f"**明日の予定 / Tomorrow's Schedule:**\n"
            next_group = (group_info["group_index"] + 1) % group_info["total_groups"]
            next_weekday_jp = ["火曜日", "水曜日", "木曜日", "金曜日", "月曜日"][
                next_group
            ]
            next_weekday_en = ["Tuesday", "Wednesday", "Thursday", "Friday", "Monday"][
                next_group
            ]
            msg += f"🔄 {next_weekday_jp}グループ ({next_group + 1}/{group_info['total_groups']})\n"
            msg += f"🔄 {next_weekday_en} Group ({next_group + 1}/{group_info['total_groups']})"

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=msg,
                username=self.config.username,
                icon_emoji=":calendar:",
            )

            self.logger.info("Sent rotation summary notification")
            return True

        except Exception as e:
            self.logger.warning(f"Failed to send rotation summary: {str(e)}")
            return False

    def upload_csv_files(
        self,
        csv_files: Dict[str, str],
        stocks: List[ValueStock] = None,
        target_date: str = None,
    ) -> bool:
        """Upload CSV files to Slack channel (public interface).

        Args:
            csv_files: Dictionary mapping file types to file paths
            stocks: List of ValueStock objects (for context)
            target_date: Optional target date for file naming

        Returns:
            bool: True if all files uploaded successfully, False otherwise
        """
        return self._upload_csv_files(csv_files, stocks or [], target_date)

    def _upload_csv_files(
        self,
        csv_files: Dict[str, str],
        stocks: List[ValueStock],
        target_date: str = None,
    ) -> bool:
        """Upload CSV files to Slack channel with enhanced error handling.

        Args:
            csv_files: Dictionary mapping file types to file paths
            stocks: List of ValueStock objects (for context)
            target_date: Optional target date for file naming

        Returns:
            bool: True if all files uploaded successfully, False otherwise
        """
        if not csv_files:
            self.logger.info("No CSV files to upload")
            return True

        upload_success = True
        uploaded_files = []
        failed_files = []

        # File type to Japanese/English names mapping
        file_descriptions = {
            "main_jp": "メインデータ（日本語）/ Main Data (Japanese)",
            "main_en": "メインデータ（英語）/ Main Data (English)",
            "history_jp": "履歴データ（日本語）/ Historical Data (Japanese)",
            "history_en": "履歴データ（英語）/ Historical Data (English)",
        }

        self.logger.info(f"Starting upload of {len(csv_files)} CSV files")

        for file_type, filepath in csv_files.items():
            try:
                if not Path(filepath).exists():
                    self.logger.warning(f"CSV file not found: {filepath}")
                    failed_files.append(Path(filepath).name)
                    upload_success = False
                    continue

                # Create file description
                description = file_descriptions.get(
                    file_type, f"CSV Data ({file_type})"
                )
                if stocks:
                    description += f" - {len(stocks)} 銘柄 / {len(stocks)} stocks"

                # Upload file with retry logic
                max_retries = 3
                retry_count = 0
                upload_successful = False

                while retry_count < max_retries and not upload_successful:
                    try:
                        response = self.client.files_upload_v2(
                            channel=self.config.channel,
                            file=filepath,
                            title=Path(filepath).name,
                            initial_comment=f"📊 **{description}**",
                            filename=Path(filepath).name,
                        )

                        if response["ok"]:
                            uploaded_files.append(Path(filepath).name)
                            self.logger.info(
                                f"Successfully uploaded CSV file: {Path(filepath).name}"
                            )
                            upload_successful = True
                        else:
                            error_msg = response.get("error", "Unknown error")
                            self.logger.error(
                                f"Failed to upload CSV file {filepath}: {error_msg}"
                            )
                            if retry_count < max_retries - 1:
                                self.logger.info(
                                    f"Retrying upload ({retry_count + 1}/{max_retries})"
                                )
                                retry_count += 1
                            else:
                                failed_files.append(Path(filepath).name)
                                upload_success = False
                                break

                    except Exception as upload_error:
                        self.logger.error(
                            f"Upload attempt {retry_count + 1} failed: {str(upload_error)}"
                        )
                        if retry_count < max_retries - 1:
                            retry_count += 1
                        else:
                            failed_files.append(Path(filepath).name)
                            upload_success = False
                            break

            except Exception as e:
                self.logger.error(f"Error uploading CSV file {filepath}: {str(e)}")
                failed_files.append(Path(filepath).name)
                upload_success = False

        # Send summary message if files were uploaded or failed
        try:
            if uploaded_files or failed_files:
                summary_msg = (
                    f"📁 **CSVファイルアップロード結果 / CSV Upload Results**\n\n"
                )

                if uploaded_files:
                    summary_msg += f"✅ **成功 / Successful uploads:**\n"
                    for filename in uploaded_files:
                        summary_msg += f"• {filename}\n"
                    summary_msg += "\n"

                if failed_files:
                    summary_msg += f"❌ **失敗 / Failed uploads:**\n"
                    for filename in failed_files:
                        summary_msg += f"• {filename}\n"
                    summary_msg += "\n"

                if target_date:
                    summary_msg += f"📅 データ日付 / Data Date: {target_date}"

                self.client.chat_postMessage(
                    channel=self.config.channel,
                    text=summary_msg,
                    username=self.config.username,
                    icon_emoji=":file_folder:",
                )

                self.logger.info(
                    f"CSV upload summary: {len(uploaded_files)} successful, {len(failed_files)} failed"
                )
        except Exception as e:
            self.logger.warning(f"Failed to send CSV upload summary: {str(e)}")

        return upload_success
