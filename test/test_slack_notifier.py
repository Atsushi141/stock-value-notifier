"""Tests for the SlackNotifier module."""

import pytest
from unittest.mock import Mock, patch
from slack_sdk.errors import SlackApiError

from src.slack_notifier import SlackNotifier
from src.models import SlackConfig, ValueStock


@pytest.fixture
def slack_config():
    """Create a test SlackConfig."""
    return SlackConfig(token="test-token", channel="#test-channel")


@pytest.fixture
def slack_notifier(slack_config):
    """Create a test SlackNotifier."""
    return SlackNotifier(slack_config)


@pytest.fixture
def sample_value_stock():
    """Create a sample ValueStock for testing."""
    return ValueStock(
        code="7203.T",
        name="トヨタ自動車",
        current_price=2500.0,
        per=12.5,
        pbr=1.2,
        dividend_yield=2.8,
        dividend_growth_years=5,
        revenue_growth_years=3,
        profit_growth_years=4,
        per_stability=25.0,
        score=85.0,
    )


class TestSlackNotifier:
    """Test cases for SlackNotifier."""

    def test_format_value_stocks_message_bilingual(
        self, slack_notifier, sample_value_stock
    ):
        """Test bilingual message formatting for value stocks."""
        stocks = [sample_value_stock]
        message = slack_notifier.format_value_stocks_message_bilingual(stocks)

        # Check that both Japanese and English content is present
        assert "本日のバリュー銘柄" in message
        assert "Today's Value Stocks" in message
        assert "トヨタ自動車" in message
        assert "7203.T" in message
        assert "¥2,500" in message
        assert "12.5倍" in message  # Japanese PER
        assert "12.5x" in message  # English PER
        assert "─" in message  # Separator

        # Check Japanese comes first
        japanese_pos = message.find("本日のバリュー銘柄")
        english_pos = message.find("Today's Value Stocks")
        assert japanese_pos < english_pos

    def test_format_no_stocks_message_bilingual(self, slack_notifier):
        """Test bilingual message formatting for no stocks found."""
        message = slack_notifier.format_no_stocks_message_bilingual()

        # Check that both Japanese and English content is present
        assert "本日の結果" in message
        assert "Today's Results" in message
        assert "バリュー銘柄が見つかりませんでした" in message
        assert "No value stocks found today" in message
        assert "─" in message  # Separator

        # Check Japanese comes first
        japanese_pos = message.find("本日の結果")
        english_pos = message.find("Today's Results")
        assert japanese_pos < english_pos

    @patch("src.slack_notifier.WebClient")
    def test_send_value_stocks_notification_success(
        self, mock_webclient, slack_notifier, sample_value_stock
    ):
        """Test successful value stocks notification."""
        # Setup mock
        mock_client = Mock()
        mock_webclient.return_value = mock_client
        slack_notifier.client = mock_client
        mock_client.chat_postMessage.return_value = {"ok": True}

        stocks = [sample_value_stock]
        result = slack_notifier.send_value_stocks_notification(stocks)

        assert result is True
        mock_client.chat_postMessage.assert_called_once()
        call_args = mock_client.chat_postMessage.call_args
        assert call_args[1]["channel"] == "#test-channel"
        assert "本日のバリュー銘柄" in call_args[1]["text"]

    @patch("src.slack_notifier.WebClient")
    def test_send_no_stocks_notification_success(self, mock_webclient, slack_notifier):
        """Test successful no stocks notification."""
        # Setup mock
        mock_client = Mock()
        mock_webclient.return_value = mock_client
        slack_notifier.client = mock_client
        mock_client.chat_postMessage.return_value = {"ok": True}

        result = slack_notifier.send_no_stocks_notification()

        assert result is True
        mock_client.chat_postMessage.assert_called_once()
        call_args = mock_client.chat_postMessage.call_args
        assert call_args[1]["channel"] == "#test-channel"
        assert "バリュー銘柄が見つかりませんでした" in call_args[1]["text"]

    @patch("src.slack_notifier.WebClient")
    def test_send_notification_slack_api_error(
        self, mock_webclient, slack_notifier, sample_value_stock
    ):
        """Test handling of Slack API errors."""
        # Setup mock to raise SlackApiError
        mock_client = Mock()
        mock_webclient.return_value = mock_client
        slack_notifier.client = mock_client

        error_response = {"error": "channel_not_found"}
        mock_client.chat_postMessage.side_effect = SlackApiError(
            "Error", error_response
        )

        stocks = [sample_value_stock]
        result = slack_notifier.send_value_stocks_notification(stocks)

        assert result is False

    @patch("src.slack_notifier.WebClient")
    def test_send_error_notification(self, mock_webclient, slack_notifier):
        """Test error notification functionality."""
        # Setup mock
        mock_client = Mock()
        mock_webclient.return_value = mock_client
        slack_notifier.client = mock_client
        mock_client.chat_postMessage.return_value = {"ok": True}

        test_error = Exception("Test error")
        result = slack_notifier.send_error_notification(test_error)

        assert result is True
        mock_client.chat_postMessage.assert_called_once()
        call_args = mock_client.chat_postMessage.call_args
        assert call_args[1]["channel"] == "#test-channel"
        assert "システムエラー" in call_args[1]["text"]
        assert "System Error" in call_args[1]["text"]
        assert "Test error" in call_args[1]["text"]

    def test_create_fallback_error_message(self, slack_notifier):
        """Test fallback error message creation."""
        error_details = {
            "error_code": "channel_not_found",
            "channel": "#test-channel",
            "timestamp": "2024-01-01 12:00:00",
        }

        message = slack_notifier._create_fallback_error_message(error_details)

        assert "Slack通知エラー" in message
        assert "Slack Notification Error" in message
        assert "channel_not_found" in message
        assert "#test-channel" in message
        assert "2024-01-01 12:00:00" in message
        assert "対応が必要です" in message
        assert "Action Required" in message


if __name__ == "__main__":
    pytest.main([__file__])
