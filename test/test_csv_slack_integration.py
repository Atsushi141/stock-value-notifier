"""
Tests for CSV generation and Slack integration to prevent upload failures.

This test suite ensures that CSV files are properly generated and uploaded to Slack,
preventing issues where CSV files are not sent to Slack channels.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pandas as pd
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from csv_exporter import CSVExporter
from slack_notifier import SlackNotifier
from models import ValueStock, SlackConfig
from workflow_runner import WorkflowRunner


class TestCSVGeneration:
    """Test CSV file generation functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.csv_exporter = CSVExporter(self.temp_dir)

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @pytest.fixture
    def sample_value_stocks(self):
        """Create sample ValueStock objects for testing."""
        return [
            ValueStock(
                code="7203.T",
                name="トヨタ自動車",
                current_price=2500.0,
                per=12.5,
                pbr=1.2,
                dividend_yield=2.8,
                dividend_growth_years=3,
                revenue_growth_years=2,
                profit_growth_years=3,
                per_stability=0.25,
                sector_17="輸送用機器",
                sector_33="輸送用機器",
                market_category="プライム（内国株式）",
                size_category="TOPIX Large70",
                retrieved_at=datetime.now(),
                dividend_history={"2022": 220, "2023": 230, "2024": 240},
                revenue_history={"2022": 31379500, "2023": 37154300, "2024": 42000000},
                profit_history={"2022": 2245400, "2023": 2726100, "2024": 3000000},
                per_history={"2022": 11.5, "2023": 12.0, "2024": 12.5},
            ),
            ValueStock(
                code="6758.T",
                name="ソニーグループ",
                current_price=13500.0,
                per=14.2,
                pbr=1.4,
                dividend_yield=0.8,
                dividend_growth_years=2,
                revenue_growth_years=3,
                profit_growth_years=2,
                per_stability=0.28,
                sector_17="電気機器",
                sector_33="電気機器",
                market_category="プライム（内国株式）",
                size_category="TOPIX Large70",
                retrieved_at=datetime.now(),
                dividend_history={"2022": 60, "2023": 70, "2024": 80},
                revenue_history={"2022": 11539000, "2023": 13574000, "2024": 15000000},
                profit_history={"2022": 882900, "2023": 1208300, "2024": 1400000},
                per_history={"2022": 13.8, "2023": 14.0, "2024": 14.2},
            ),
        ]

    def test_csv_generation_all_files_created(self, sample_value_stocks):
        """Test that all 4 CSV files are generated correctly."""
        csv_files = self.csv_exporter.export_all_csv_files(
            sample_value_stocks, "2026-01-10"
        )

        # Check that all 4 files are returned
        expected_files = ["main_jp", "main_en", "history_jp", "history_en"]
        assert set(csv_files.keys()) == set(expected_files)

        # Check that all files exist
        for file_type, filepath in csv_files.items():
            assert os.path.exists(filepath), f"File {file_type} not created: {filepath}"
            assert (
                os.path.getsize(filepath) > 0
            ), f"File {file_type} is empty: {filepath}"

    def test_csv_content_validation(self, sample_value_stocks):
        """Test that CSV files contain correct content and structure."""
        csv_files = self.csv_exporter.export_all_csv_files(
            sample_value_stocks, "2026-01-10"
        )

        # Test main Japanese CSV
        main_jp_df = pd.read_csv(csv_files["main_jp"])
        assert len(main_jp_df) == len(sample_value_stocks)
        assert "銘柄コード" in main_jp_df.columns
        assert "銘柄名" in main_jp_df.columns
        assert "現在株価" in main_jp_df.columns
        # Ensure score column is NOT present (requirement)
        assert "スコア" not in main_jp_df.columns
        assert "Score" not in main_jp_df.columns

        # Test main English CSV
        main_en_df = pd.read_csv(csv_files["main_en"])
        assert len(main_en_df) == len(sample_value_stocks)
        assert "Stock Code" in main_en_df.columns
        assert "Company Name" in main_en_df.columns
        assert "Current Price" in main_en_df.columns

        # Test history Japanese CSV
        history_jp_df = pd.read_csv(csv_files["history_jp"])
        assert len(history_jp_df) > 0  # Should have historical data rows
        assert "銘柄コード" in history_jp_df.columns
        assert "データ種別" in history_jp_df.columns
        assert "2024" in history_jp_df.columns

        # Test history English CSV
        history_en_df = pd.read_csv(csv_files["history_en"])
        assert len(history_en_df) > 0
        assert "Stock Code" in history_en_df.columns
        assert "Data Type" in history_en_df.columns
        assert "2024" in history_en_df.columns

    def test_csv_generation_with_empty_stocks(self):
        """Test CSV generation with empty stock list."""
        csv_files = self.csv_exporter.export_all_csv_files([], "2026-01-10")

        # Should still create files with headers
        assert len(csv_files) == 4
        for file_type, filepath in csv_files.items():
            assert os.path.exists(filepath)
            # Files should have headers but no data rows
            df = pd.read_csv(filepath)
            assert len(df) == 0  # No data rows
            assert len(df.columns) > 0  # But has headers

    def test_csv_generation_error_handling(self, sample_value_stocks):
        """Test CSV generation error handling."""
        # Test with invalid directory
        invalid_exporter = CSVExporter("/invalid/directory/path")

        with pytest.raises(Exception):
            invalid_exporter.export_all_csv_files(sample_value_stocks, "2026-01-10")

    def test_csv_filename_format(self, sample_value_stocks):
        """Test that CSV filenames follow the correct format."""
        csv_files = self.csv_exporter.export_all_csv_files(
            sample_value_stocks, "2026-01-10"
        )

        expected_filenames = {
            "main_jp": "value_stocks_20260110.csv",
            "main_en": "value_stocks_20260110_en.csv",
            "history_jp": "value_stocks_20260110_history.csv",
            "history_en": "value_stocks_20260110_history_en.csv",
        }

        for file_type, filepath in csv_files.items():
            filename = os.path.basename(filepath)
            assert filename == expected_filenames[file_type]


class TestSlackIntegration:
    """Test Slack integration functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.slack_config = SlackConfig(
            token="xoxb-test-token", channel="#test-channel"
        )
        self.slack_notifier = SlackNotifier(self.slack_config)

    @pytest.fixture
    def mock_csv_files(self):
        """Create mock CSV file paths."""
        return {
            "main_jp": "/tmp/value_stocks_20260110.csv",
            "main_en": "/tmp/value_stocks_20260110_en.csv",
            "history_jp": "/tmp/value_stocks_20260110_history.csv",
            "history_en": "/tmp/value_stocks_20260110_history_en.csv",
        }

    @pytest.fixture
    def sample_value_stocks(self):
        """Create sample ValueStock objects for testing."""
        return [
            ValueStock(
                code="7203.T",
                name="トヨタ自動車",
                current_price=2500.0,
                per=12.5,
                pbr=1.2,
                dividend_yield=2.8,
                dividend_growth_years=3,
                revenue_growth_years=2,
                profit_growth_years=3,
                per_stability=0.25,
                sector_17="輸送用機器",
                sector_33="輸送用機器",
                market_category="プライム（内国株式）",
                size_category="TOPIX Large70",
                retrieved_at=datetime.now(),
            )
        ]

    @patch("slack_sdk.WebClient.files_upload_v2")
    @patch("slack_sdk.WebClient.chat_postMessage")
    @patch("pathlib.Path.exists")
    def test_csv_upload_success(
        self, mock_exists, mock_chat, mock_upload, mock_csv_files, sample_value_stocks
    ):
        """Test successful CSV file upload to Slack."""
        # Mock file existence
        mock_exists.return_value = True

        # Mock successful upload responses
        mock_upload.return_value = {"ok": True, "file": {"id": "F123456"}}
        mock_chat.return_value = {"ok": True}

        # Test upload
        result = self.slack_notifier.upload_csv_files(
            mock_csv_files, sample_value_stocks, "2026-01-10"
        )

        assert result is True
        assert mock_upload.call_count == 4  # 4 files uploaded
        assert mock_chat.call_count == 1  # 1 summary message

    @patch("slack_sdk.WebClient.files_upload_v2")
    @patch("slack_sdk.WebClient.chat_postMessage")
    @patch("pathlib.Path.exists")
    def test_csv_upload_partial_failure(
        self, mock_exists, mock_chat, mock_upload, mock_csv_files, sample_value_stocks
    ):
        """Test CSV upload with some files failing."""
        # Mock file existence
        mock_exists.return_value = True

        # Mock mixed upload responses (some succeed, some fail)
        mock_upload.side_effect = [
            {"ok": True, "file": {"id": "F123456"}},  # main_jp success
            {"ok": False, "error": "file_too_large"},  # main_en failure
            {"ok": True, "file": {"id": "F123457"}},  # history_jp success
            {"ok": False, "error": "rate_limited"},  # history_en failure
        ]
        mock_chat.return_value = {"ok": True}

        # Test upload
        result = self.slack_notifier.upload_csv_files(
            mock_csv_files, sample_value_stocks, "2026-01-10"
        )

        assert result is False  # Should return False due to failures
        assert mock_upload.call_count == 4  # All files attempted
        assert mock_chat.call_count == 1  # Summary message still sent

    @patch("slack_sdk.WebClient.files_upload_v2")
    @patch("pathlib.Path.exists")
    def test_csv_upload_missing_files(
        self, mock_exists, mock_upload, mock_csv_files, sample_value_stocks
    ):
        """Test CSV upload when files don't exist."""
        # Mock file non-existence
        mock_exists.return_value = False

        # Test upload
        result = self.slack_notifier.upload_csv_files(
            mock_csv_files, sample_value_stocks, "2026-01-10"
        )

        assert result is False  # Should fail due to missing files
        assert mock_upload.call_count == 0  # No uploads attempted

    @patch("slack_sdk.WebClient.files_upload_v2")
    @patch("slack_sdk.WebClient.chat_postMessage")
    @patch("pathlib.Path.exists")
    def test_csv_upload_retry_mechanism(
        self, mock_exists, mock_chat, mock_upload, mock_csv_files, sample_value_stocks
    ):
        """Test CSV upload retry mechanism."""
        # Mock file existence
        mock_exists.return_value = True

        # Mock upload with initial failures then success
        mock_upload.side_effect = [
            {"ok": False, "error": "rate_limited"},  # First attempt fails
            {"ok": False, "error": "rate_limited"},  # Second attempt fails
            {"ok": True, "file": {"id": "F123456"}},  # Third attempt succeeds
            {"ok": True, "file": {"id": "F123457"}},  # Other files succeed
            {"ok": True, "file": {"id": "F123458"}},
            {"ok": True, "file": {"id": "F123459"}},
        ]
        mock_chat.return_value = {"ok": True}

        # Test upload
        result = self.slack_notifier.upload_csv_files(
            mock_csv_files, sample_value_stocks, "2026-01-10"
        )

        assert result is True
        # Should have retried the first file 3 times, plus 3 other files = 6 total calls
        assert mock_upload.call_count == 6

    def test_csv_upload_empty_files_dict(self, sample_value_stocks):
        """Test CSV upload with empty files dictionary."""
        result = self.slack_notifier.upload_csv_files(
            {}, sample_value_stocks, "2026-01-10"
        )
        assert result is True  # Should succeed (nothing to upload)

    def test_csv_upload_none_files_dict(self, sample_value_stocks):
        """Test CSV upload with None files dictionary."""
        result = self.slack_notifier.upload_csv_files(
            None, sample_value_stocks, "2026-01-10"
        )
        assert result is True  # Should succeed (nothing to upload)


class TestWorkflowIntegration:
    """Test end-to-end workflow integration."""

    @patch("src.workflow_runner.WorkflowRunner.setup_environment")
    @patch("src.csv_exporter.CSVExporter.export_all_csv_files")
    @patch("src.slack_notifier.SlackNotifier.send_value_stocks_notification")
    def test_workflow_csv_generation_and_upload(
        self, mock_slack_send, mock_csv_export, mock_setup
    ):
        """Test that workflow properly generates and uploads CSV files."""
        # Mock setup
        mock_setup.return_value = None

        # Mock CSV generation
        mock_csv_files = {
            "main_jp": "/tmp/value_stocks_20260110.csv",
            "main_en": "/tmp/value_stocks_20260110_en.csv",
            "history_jp": "/tmp/value_stocks_20260110_history.csv",
            "history_en": "/tmp/value_stocks_20260110_history_en.csv",
        }
        mock_csv_export.return_value = mock_csv_files

        # Mock Slack notification
        mock_slack_send.return_value = True

        # Create workflow runner
        runner = WorkflowRunner()

        # Mock the components
        runner.csv_exporter = Mock()
        runner.csv_exporter.export_all_csv_files = mock_csv_export
        runner.slack_notifier = Mock()
        runner.slack_notifier.send_value_stocks_notification = mock_slack_send

        # Test CSV generation helper method
        sample_stocks = [
            ValueStock(
                code="7203.T",
                name="トヨタ自動車",
                current_price=2500.0,
                per=12.5,
                pbr=1.2,
                dividend_yield=2.8,
                dividend_growth_years=3,
                revenue_growth_years=2,
                profit_growth_years=3,
                per_stability=0.25,
                sector_17="輸送用機器",
                sector_33="輸送用機器",
                market_category="プライム（内国株式）",
                size_category="TOPIX Large70",
                retrieved_at=datetime.now(),
            )
        ]

        result = runner._generate_and_upload_csv_files(sample_stocks, "2026-01-10")

        # Verify CSV generation was called
        mock_csv_export.assert_called_once_with(sample_stocks, "2026-01-10")

        # Verify result
        assert result == mock_csv_files

    @patch("src.workflow_runner.WorkflowRunner.setup_environment")
    def test_workflow_csv_generation_error_handling(self, mock_setup):
        """Test workflow CSV generation error handling."""
        # Mock setup
        mock_setup.return_value = None

        # Create workflow runner
        runner = WorkflowRunner()

        # Mock CSV exporter to raise exception
        runner.csv_exporter = Mock()
        runner.csv_exporter.export_all_csv_files.side_effect = Exception(
            "CSV generation failed"
        )

        # Test should handle error gracefully
        result = runner._generate_and_upload_csv_files([], "2026-01-10")

        # Should return None on error
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__])
