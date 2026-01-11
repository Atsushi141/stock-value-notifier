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
        """Send notification about found value stocks with optional CSV files and enhanced status reporting.

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

            # Upload CSV files if provided with enhanced status reporting
            csv_upload_success = True
            if csv_files:
                self.logger.info(f"Attempting to upload {len(csv_files)} CSV files")
                csv_upload_success = self._upload_csv_files(
                    csv_files, stocks, target_date
                )

                if not csv_upload_success:
                    self.logger.warning(
                        "CSV upload failed, sending failure notification"
                    )
                    # Send additional notification about CSV upload failure
                    screening_summary = {
                        "date": target_date,
                        "mode": "value_stocks_found",
                        "analyzed_stocks": len(all_stocks) if all_stocks else 0,
                        "value_stocks_found": len(stocks),
                    }
                    self.send_csv_upload_failure_notification(
                        csv_files,
                        {"upload_errors": "Multiple file upload failures"},
                        screening_summary,
                    )

            # Log comprehensive notification status
            notification_status = {
                "message_sent": True,
                "csv_files_provided": len(csv_files) if csv_files else 0,
                "csv_upload_success": csv_upload_success,
                "stocks_count": len(stocks),
                "target_date": target_date,
            }

            if csv_upload_success:
                self.logger.info(
                    f"Successfully sent value stocks notification with CSV files to {self.config.channel}"
                )
                self.logger.info(f"Notification status: {notification_status}")
            else:
                self.logger.warning(
                    f"Sent value stocks notification but CSV upload failed: {notification_status}"
                )

            return (
                True  # Return True if main message was sent, even if CSV upload failed
            )

        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
            return self._handle_slack_error(e)
        except Exception as e:
            self.logger.error(
                f"Unexpected error sending notification: {str(e)}", exc_info=True
            )
            return False

    def send_no_stocks_notification(
        self,
        all_stocks: List[str] = None,
        group_info: dict = None,
        target_date: str = None,
        csv_files: Dict[str, str] = None,
    ) -> bool:
        """Send notification when no value stocks are found with enhanced CSV status reporting.

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

            # Upload CSV files if provided with enhanced status reporting
            csv_upload_success = True
            if csv_files:
                self.logger.info(
                    f"Attempting to upload {len(csv_files)} summary/empty CSV files"
                )
                csv_upload_success = self._upload_csv_files(csv_files, [], target_date)

                if not csv_upload_success:
                    self.logger.warning(
                        "Summary CSV upload failed, sending failure notification"
                    )
                    # Send additional notification about CSV upload failure
                    screening_summary = {
                        "date": target_date,
                        "mode": "no_stocks_found",
                        "analyzed_stocks": len(all_stocks) if all_stocks else 0,
                        "value_stocks_found": 0,
                    }
                    self.send_csv_upload_failure_notification(
                        csv_files,
                        {"upload_errors": "Summary file upload failures"},
                        screening_summary,
                    )

            # Log comprehensive notification status
            notification_status = {
                "message_sent": True,
                "csv_files_provided": len(csv_files) if csv_files else 0,
                "csv_upload_success": csv_upload_success,
                "analyzed_stocks": len(all_stocks) if all_stocks else 0,
                "target_date": target_date,
            }

            if csv_upload_success:
                self.logger.info(
                    f"Successfully sent no stocks notification with CSV files to {self.config.channel}"
                )
                self.logger.info(f"Notification status: {notification_status}")
            else:
                self.logger.warning(
                    f"Sent no stocks notification but CSV upload failed: {notification_status}"
                )

            return (
                True  # Return True if main message was sent, even if CSV upload failed
            )

        except SlackApiError as e:
            self.logger.error(f"Slack API error: {e.response['error']}")
            return self._handle_slack_error(e)
        except Exception as e:
            self.logger.error(
                f"Unexpected error sending no stocks notification: {str(e)}",
                exc_info=True,
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
        fallback_channels = ["#general", "#alerts", "#notifications", "#random"]

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

        # If all fallback channels fail, try text-based summary
        if self._try_text_based_fallback(error_details):
            return True

        # If all notification methods fail, log the error for admin review
        self._log_admin_alert(
            "All notification channels failed - manual intervention required"
        )
        return False

    def _try_text_based_fallback(self, error_details: dict) -> bool:
        """Try to send a text-based summary when CSV upload fails completely.

        Args:
            error_details: Dictionary containing error information

        Returns:
            bool: True if text summary was sent successfully, False otherwise
        """
        try:
            self.logger.info("Attempting text-based fallback notification")

            # Create a comprehensive text summary
            fallback_msg = self._create_text_based_summary(error_details)

            # Try to send to the original channel as a last resort
            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=fallback_msg,
                username=self.config.username,
                icon_emoji=":warning:",
            )

            self.logger.info("Successfully sent text-based fallback summary")
            return True

        except Exception as e:
            self.logger.error(f"Text-based fallback also failed: {str(e)}")
            return False

    def _create_text_based_summary(self, error_details: dict) -> str:
        """Create a text-based summary when CSV files cannot be uploaded.

        Args:
            error_details: Dictionary containing error information

        Returns:
            str: Formatted text summary
        """
        msg = "📊 **スクリーニング結果サマリー / Screening Results Summary**\n\n"
        msg += "⚠️ CSVファイルのアップロードに失敗しましたが、結果をテキストで報告します。\n"
        msg += "⚠️ CSV file upload failed, but here's a text summary of the results.\n\n"

        # Add basic screening information
        if "screening_summary" in error_details:
            summary = error_details["screening_summary"]
            msg += f"**実行日 / Date:** {summary.get('date', 'Unknown')}\n"
            msg += f"**モード / Mode:** {summary.get('mode', 'Unknown')}\n"
            msg += f"**分析銘柄数 / Analyzed Stocks:** {summary.get('analyzed_stocks', 0)}\n"
            msg += f"**発見銘柄数 / Found Stocks:** {summary.get('value_stocks_found', 0)}\n\n"

        # Add error information
        msg += f"**エラー詳細 / Error Details:**\n"
        msg += f"• エラーコード / Error Code: `{error_details.get('error_code', 'Unknown')}`\n"
        msg += f"• 対象チャンネル / Target Channel: `{error_details.get('channel', 'Unknown')}`\n"
        msg += (
            f"• 発生時刻 / Timestamp: {error_details.get('timestamp', 'Unknown')}\n\n"
        )

        msg += "**対応 / Action Required:**\n"
        msg += "システム管理者にSlack設定の確認を依頼してください。\n"
        msg += "Please ask system administrator to check Slack configuration."

        return msg

    def send_csv_upload_failure_notification(
        self,
        csv_files: Dict[str, str],
        error_summary: Dict[str, Any],
        screening_summary: Dict[str, Any] = None,
    ) -> bool:
        """Send notification when CSV upload fails completely.

        Args:
            csv_files: Dictionary of CSV files that failed to upload
            error_summary: Summary of upload errors
            screening_summary: Optional screening results summary

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        try:
            msg = "🚨 **CSVアップロード失敗通知 / CSV Upload Failure Notification**\n\n"

            # Add screening summary if available
            if screening_summary:
                msg += f"**スクリーニング結果 / Screening Results:**\n"
                msg += f"• 実行日 / Date: {screening_summary.get('date', 'Unknown')}\n"
                msg += f"• モード / Mode: {screening_summary.get('mode', 'Unknown')}\n"
                msg += f"• 分析銘柄数 / Analyzed: {screening_summary.get('analyzed_stocks', 0)}\n"
                msg += f"• 発見銘柄数 / Found: {screening_summary.get('value_stocks_found', 0)}\n\n"

            # Add failed files information
            if csv_files:
                msg += f"**失敗ファイル / Failed Files ({len(csv_files)}):**\n"
                for file_type, filepath in csv_files.items():
                    filename = Path(filepath).name
                    msg += f"• {filename} ({file_type})\n"
                msg += "\n"

            # Add error summary
            if error_summary:
                msg += f"**エラーサマリー / Error Summary:**\n"
                for error_type, count in error_summary.items():
                    msg += f"• {error_type}: {count}\n"
                msg += "\n"

            msg += "**対応 / Action Required:**\n"
            msg += "1. Slackトークンとチャンネル設定を確認 / Check Slack token and channel settings\n"
            msg += "2. ネットワーク接続を確認 / Check network connectivity\n"
            msg += "3. ファイルサイズ制限を確認 / Check file size limits\n"
            msg += "4. 手動でCSVファイルを確認 / Manually check CSV files in system"

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=msg,
                username=self.config.username,
                icon_emoji=":warning:",
            )

            self.logger.info("Successfully sent CSV upload failure notification")
            return True

        except Exception as e:
            self.logger.error(
                f"Failed to send CSV upload failure notification: {str(e)}"
            )
            # Try fallback notification
            error_details = {
                "error_code": "csv_upload_failure",
                "channel": self.config.channel,
                "timestamp": datetime.now().isoformat(),
                "screening_summary": screening_summary,
            }
            return self._try_fallback_notification(error_details)

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
        """Upload CSV files to Slack channel with enhanced error handling and retry mechanisms.

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
        retry_details = {}

        # File type to Japanese/English names mapping
        file_descriptions = {
            "main_jp": "メインデータ（日本語）/ Main Data (Japanese)",
            "main_en": "メインデータ（英語）/ Main Data (English)",
            "history_jp": "履歴データ（日本語）/ Historical Data (Japanese)",
            "history_en": "履歴データ（英語）/ Historical Data (English)",
            "summary_jp": "サマリーデータ（日本語）/ Summary Data (Japanese)",
            "summary_en": "サマリーデータ（英語）/ Summary Data (English)",
        }

        self.logger.info(
            f"Starting upload of {len(csv_files)} CSV files with enhanced retry logic"
        )

        for file_type, filepath in csv_files.items():
            try:
                if not Path(filepath).exists():
                    self.logger.warning(f"CSV file not found: {filepath}")
                    failed_files.append(Path(filepath).name)
                    upload_success = False
                    continue

                # Get file size for logging
                file_size = Path(filepath).stat().st_size
                file_size_kb = round(file_size / 1024, 2)

                # Create file description
                description = file_descriptions.get(
                    file_type, f"CSV Data ({file_type})"
                )
                if stocks:
                    description += f" - {len(stocks)} 銘柄 / {len(stocks)} stocks"

                # Enhanced retry logic with exponential backoff
                max_retries = 5
                retry_count = 0
                upload_successful = False
                base_delay = 1.0  # Base delay in seconds

                while retry_count < max_retries and not upload_successful:
                    try:
                        self.logger.info(
                            f"Uploading {Path(filepath).name} (attempt {retry_count + 1}/{max_retries}, {file_size_kb}KB)"
                        )

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
                                f"Successfully uploaded CSV file: {Path(filepath).name} ({file_size_kb}KB)"
                            )
                            upload_successful = True
                            retry_details[Path(filepath).name] = {
                                "attempts": retry_count + 1,
                                "success": True,
                                "file_size_kb": file_size_kb,
                            }
                        else:
                            error_msg = response.get("error", "Unknown error")
                            self.logger.error(
                                f"Slack API error for {filepath}: {error_msg}"
                            )

                            # Check if this is a permanent error that shouldn't be retried
                            if self._is_permanent_upload_error(error_msg):
                                self.logger.error(
                                    f"Permanent error detected, not retrying: {error_msg}"
                                )
                                failed_files.append(Path(filepath).name)
                                upload_success = False
                                retry_details[Path(filepath).name] = {
                                    "attempts": retry_count + 1,
                                    "success": False,
                                    "error": error_msg,
                                    "permanent": True,
                                }
                                break

                            # Transient error - retry with exponential backoff
                            if retry_count < max_retries - 1:
                                delay = base_delay * (
                                    2**retry_count
                                )  # Exponential backoff
                                self.logger.info(
                                    f"Retrying upload in {delay}s (attempt {retry_count + 1}/{max_retries})"
                                )
                                import time

                                time.sleep(delay)
                                retry_count += 1
                            else:
                                failed_files.append(Path(filepath).name)
                                upload_success = False
                                retry_details[Path(filepath).name] = {
                                    "attempts": retry_count + 1,
                                    "success": False,
                                    "error": error_msg,
                                    "retries_exhausted": True,
                                }
                                break

                    except Exception as upload_error:
                        error_str = str(upload_error)
                        self.logger.error(
                            f"Upload attempt {retry_count + 1} failed: {error_str}"
                        )

                        # Check if this is a network/connection error that should be retried
                        if self._is_retryable_upload_error(upload_error):
                            if retry_count < max_retries - 1:
                                delay = base_delay * (
                                    2**retry_count
                                )  # Exponential backoff
                                self.logger.info(
                                    f"Network error detected, retrying in {delay}s (attempt {retry_count + 1}/{max_retries})"
                                )
                                import time

                                time.sleep(delay)
                                retry_count += 1
                            else:
                                failed_files.append(Path(filepath).name)
                                upload_success = False
                                retry_details[Path(filepath).name] = {
                                    "attempts": retry_count + 1,
                                    "success": False,
                                    "error": error_str,
                                    "retries_exhausted": True,
                                }
                                break
                        else:
                            # Non-retryable error
                            failed_files.append(Path(filepath).name)
                            upload_success = False
                            retry_details[Path(filepath).name] = {
                                "attempts": retry_count + 1,
                                "success": False,
                                "error": error_str,
                                "non_retryable": True,
                            }
                            break

            except Exception as e:
                self.logger.error(
                    f"Error uploading CSV file {filepath}: {str(e)}", exc_info=True
                )
                failed_files.append(Path(filepath).name)
                upload_success = False
                retry_details[Path(filepath).name] = {
                    "attempts": 1,
                    "success": False,
                    "error": str(e),
                    "exception": True,
                }

        # Log comprehensive upload summary
        self.logger.info(
            f"CSV upload completed: {len(uploaded_files)} successful, {len(failed_files)} failed"
        )
        for filename, details in retry_details.items():
            if details["success"]:
                self.logger.info(
                    f"  ✅ {filename}: {details['attempts']} attempts, {details.get('file_size_kb', 0)}KB"
                )
            else:
                self.logger.error(
                    f"  ❌ {filename}: {details['attempts']} attempts, error: {details.get('error', 'Unknown')}"
                )

        # Send enhanced summary message
        try:
            if uploaded_files or failed_files:
                summary_msg = self._create_upload_summary_message(
                    uploaded_files, failed_files, retry_details, target_date
                )

                self.client.chat_postMessage(
                    channel=self.config.channel,
                    text=summary_msg,
                    username=self.config.username,
                    icon_emoji=":file_folder:",
                )

                self.logger.info("Sent enhanced CSV upload summary notification")
        except Exception as e:
            self.logger.warning(f"Failed to send CSV upload summary: {str(e)}")

        return upload_success

    def _is_permanent_upload_error(self, error_code: str) -> bool:
        """Check if an upload error is permanent and should not be retried.

        Args:
            error_code: Slack API error code

        Returns:
            bool: True if error is permanent, False if it should be retried
        """
        permanent_errors = {
            "invalid_auth",
            "account_inactive",
            "token_revoked",
            "no_permission",
            "channel_not_found",
            "not_in_channel",
            "channel_is_archived",
            "file_too_large",
            "invalid_file_type",
        }
        return error_code in permanent_errors

    def _is_retryable_upload_error(self, error: Exception) -> bool:
        """Check if an upload exception is retryable.

        Args:
            error: Exception that occurred during upload

        Returns:
            bool: True if error should be retried, False otherwise
        """
        error_str = str(error).lower()
        retryable_patterns = [
            "connection",
            "timeout",
            "network",
            "temporary",
            "rate_limited",
            "server error",
            "503",
            "502",
            "500",
        ]
        return any(pattern in error_str for pattern in retryable_patterns)

    def _create_upload_summary_message(
        self,
        uploaded_files: List[str],
        failed_files: List[str],
        retry_details: Dict[str, Dict],
        target_date: str = None,
    ) -> str:
        """Create enhanced upload summary message with retry details.

        Args:
            uploaded_files: List of successfully uploaded files
            failed_files: List of failed file uploads
            retry_details: Dictionary with retry attempt details
            target_date: Optional target date

        Returns:
            str: Formatted summary message
        """
        summary_msg = f"📁 **CSVファイルアップロード結果 / CSV Upload Results**\n\n"

        if uploaded_files:
            summary_msg += (
                f"✅ **成功 / Successful uploads ({len(uploaded_files)}):**\n"
            )
            for filename in uploaded_files:
                details = retry_details.get(filename, {})
                attempts = details.get("attempts", 1)
                size_kb = details.get("file_size_kb", 0)
                if attempts > 1:
                    summary_msg += f"• {filename} ({size_kb}KB, {attempts} attempts)\n"
                else:
                    summary_msg += f"• {filename} ({size_kb}KB)\n"
            summary_msg += "\n"

        if failed_files:
            summary_msg += f"❌ **失敗 / Failed uploads ({len(failed_files)}):**\n"
            for filename in failed_files:
                details = retry_details.get(filename, {})
                attempts = details.get("attempts", 1)
                error = details.get("error", "Unknown error")
                # Truncate long error messages
                if len(error) > 50:
                    error = error[:47] + "..."
                summary_msg += f"• {filename} ({attempts} attempts, {error})\n"
            summary_msg += "\n"

        if target_date:
            summary_msg += f"📅 **データ日付 / Data Date:** {target_date}\n"

        # Add retry statistics
        total_attempts = sum(
            details.get("attempts", 1) for details in retry_details.values()
        )
        if total_attempts > len(retry_details):
            summary_msg += f"🔄 **リトライ統計 / Retry Stats:** {total_attempts} total attempts for {len(retry_details)} files"

        return summary_msg
