"""Slack notification module for value stock alerts."""

import logging
from datetime import datetime
from typing import List, Optional
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
        self, stocks: List[ValueStock], all_stocks: List[str] = None
    ) -> bool:
        """Send notification about found value stocks.

        Args:
            stocks: List of ValueStock objects to notify about
            all_stocks: List of all stock names that were analyzed

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        if not stocks:
            return self.send_no_stocks_notification(all_stocks)

        try:
            message = self.format_value_stocks_message_bilingual(stocks, all_stocks)

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=message,
                username=self.config.username,
                icon_emoji=self.config.icon_emoji,
            )

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

    def send_no_stocks_notification(self, all_stocks: List[str] = None) -> bool:
        """Send notification when no value stocks are found.

        Args:
            all_stocks: List of all stock names that were analyzed

        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        try:
            message = self.format_no_stocks_message_bilingual(all_stocks)

            response = self.client.chat_postMessage(
                channel=self.config.channel,
                text=message,
                username=self.config.username,
                icon_emoji=self.config.icon_emoji,
            )

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
        self, stocks: List[ValueStock], all_stocks: List[str] = None
    ) -> str:
        """Format value stocks message in both Japanese and English.

        Args:
            stocks: List of ValueStock objects to format
            all_stocks: List of all stock names that were analyzed

        Returns:
            str: Formatted bilingual message with Japanese first, then English
        """
        # Japanese message first (as per requirement 3.3)
        japanese_msg = self._format_japanese_stocks_message(stocks, all_stocks)

        # English message second
        english_msg = self._format_english_stocks_message(stocks, all_stocks)

        # Combine with clear separator
        return japanese_msg + "\n" + "─" * 50 + "\n\n" + english_msg

    def _format_japanese_stocks_message(
        self, stocks: List[ValueStock], all_stocks: List[str] = None
    ) -> str:
        """Format Japanese stocks message with readable formatting.

        Args:
            stocks: List of ValueStock objects to format
            all_stocks: List of all stock names that were analyzed

        Returns:
            str: Formatted Japanese message
        """
        msg = "🎯 **本日のバリュー銘柄**\n\n"

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

        # Add analyzed stocks summary
        if all_stocks:
            msg += f"\n📊 **分析対象銘柄** ({len(all_stocks)}銘柄)\n"
            msg += "```\n"
            # Display stocks in columns for better readability
            for i in range(0, len(all_stocks), 3):
                row_stocks = all_stocks[i : i + 3]
                msg += " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"
            msg += "```\n"

        return msg

    def _format_english_stocks_message(
        self, stocks: List[ValueStock], all_stocks: List[str] = None
    ) -> str:
        """Format English stocks message with readable formatting.

        Args:
            stocks: List of ValueStock objects to format
            all_stocks: List of all stock names that were analyzed

        Returns:
            str: Formatted English message
        """
        msg = "🎯 **Today's Value Stocks**\n\n"

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

        # Add analyzed stocks summary
        if all_stocks:
            msg += f"\n📊 **Analyzed Stocks** ({len(all_stocks)} stocks)\n"
            msg += "```\n"
            # Display stocks in columns for better readability
            for i in range(0, len(all_stocks), 3):
                row_stocks = all_stocks[i : i + 3]
                msg += " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"
            msg += "```\n"

        return msg

    def format_no_stocks_message_bilingual(self, all_stocks: List[str] = None) -> str:
        """Format no stocks found message in both Japanese and English.

        Args:
            all_stocks: List of all stock names that were analyzed

        Returns:
            str: Formatted bilingual message with Japanese first, then English
        """
        japanese_msg = "📊 **本日の結果**\n\n"
        japanese_msg += "本日はバリュー銘柄が見つかりませんでした。\n"
        japanese_msg += "明日も引き続き監視いたします。"

        english_msg = "📊 **Today's Results**\n\n"
        english_msg += "No value stocks found today.\n"
        english_msg += "We'll continue monitoring tomorrow."

        # Add analyzed stocks summary
        if all_stocks:
            japanese_msg += f"\n\n📊 **分析対象銘柄** ({len(all_stocks)}銘柄)\n"
            japanese_msg += "```\n"
            # Display stocks in columns for better readability
            for i in range(0, len(all_stocks), 3):
                row_stocks = all_stocks[i : i + 3]
                japanese_msg += (
                    " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"
                )
            japanese_msg += "```"

            english_msg += f"\n\n📊 **Analyzed Stocks** ({len(all_stocks)} stocks)\n"
            english_msg += "```\n"
            # Display stocks in columns for better readability
            for i in range(0, len(all_stocks), 3):
                row_stocks = all_stocks[i : i + 3]
                english_msg += " | ".join(f"{stock:<20}" for stock in row_stocks) + "\n"
            english_msg += "```"

        return japanese_msg + "\n\n" + "─" * 50 + "\n\n" + english_msg

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
