"""
Property-based tests for ConfigManager class.
Tests Property 8: 設定値の妥当性 (Configuration value validity)
"""

import os
import pytest
from hypothesis import given, strategies as st, assume
from unittest.mock import patch
import logging

from src.config_manager import ConfigManager, ScreeningConfig, SlackConfig, Config


class TestConfigManagerProperties:
    """Property-based tests for ConfigManager."""

    def setup_method(self):
        """Set up test environment."""
        self.config_manager = ConfigManager()
        # Suppress logging during tests
        logging.getLogger().setLevel(logging.CRITICAL)

    @given(
        max_per=st.floats(
            min_value=-100, max_value=100, allow_nan=False, allow_infinity=False
        ),
        max_pbr=st.floats(
            min_value=-100, max_value=100, allow_nan=False, allow_infinity=False
        ),
        min_dividend_yield=st.floats(
            min_value=-100, max_value=100, allow_nan=False, allow_infinity=False
        ),
        min_growth_years=st.integers(min_value=-10, max_value=10),
        max_per_volatility=st.floats(
            min_value=-100, max_value=100, allow_nan=False, allow_infinity=False
        ),
    )
    def test_property_8_configuration_value_validity(
        self, max_per, max_pbr, min_dividend_yield, min_growth_years, max_per_volatility
    ):
        """
        Property 8: 設定値の妥当性
        For any configuration values, if invalid values are detected,
        the system should use default values and output warning logs.

        **Validates: Requirements 5.4, 5.5**
        **Feature: stock-value-notifier, Property 8: 設定値の妥当性**
        """
        # Set up environment variables with test values
        env_vars = {
            "MAX_PER": str(max_per),
            "MAX_PBR": str(max_pbr),
            "MIN_DIVIDEND_YIELD": str(min_dividend_yield),
            "MIN_GROWTH_YEARS": str(min_growth_years),
            "MAX_PER_VOLATILITY": str(max_per_volatility),
        }

        with patch.dict(os.environ, env_vars, clear=False):
            # Get screening configuration
            screening_config = self.config_manager.get_screening_config()

            # Property: All configuration values should be valid (positive where required)
            assert (
                screening_config.max_per > 0
            ), f"max_per should be positive, got {screening_config.max_per}"
            assert (
                screening_config.max_pbr > 0
            ), f"max_pbr should be positive, got {screening_config.max_pbr}"
            assert (
                screening_config.min_dividend_yield >= 0
            ), f"min_dividend_yield should be non-negative, got {screening_config.min_dividend_yield}"
            assert (
                screening_config.min_growth_years > 0
            ), f"min_growth_years should be positive, got {screening_config.min_growth_years}"
            assert (
                screening_config.max_per_volatility >= 0
            ), f"max_per_volatility should be non-negative, got {screening_config.max_per_volatility}"

            # Property: If input was invalid, default values should be used
            default_config = ScreeningConfig()

            if max_per <= 0:
                assert screening_config.max_per == default_config.max_per
            if max_pbr <= 0:
                assert screening_config.max_pbr == default_config.max_pbr
            if min_dividend_yield < 0:
                assert (
                    screening_config.min_dividend_yield
                    == default_config.min_dividend_yield
                )
            if min_growth_years <= 0:
                assert (
                    screening_config.min_growth_years == default_config.min_growth_years
                )
            if max_per_volatility < 0:
                assert (
                    screening_config.max_per_volatility
                    == default_config.max_per_volatility
                )

    @given(
        token=st.text(min_size=0, max_size=100),
        channel=st.text(min_size=0, max_size=100),
    )
    def test_property_8_slack_config_validation(self, token, channel):
        """
        Property 8: Slack設定値の妥当性
        For any Slack configuration, validation should properly identify valid/invalid configurations.

        **Validates: Requirements 5.4, 5.5**
        **Feature: stock-value-notifier, Property 8: 設定値の妥当性**
        """
        slack_config = SlackConfig(token=token, channel=channel)
        is_valid = self.config_manager._validate_slack_config(slack_config)

        # Property: Empty or whitespace-only tokens/channels should be invalid
        if not token or not token.strip():
            assert (
                not is_valid
            ), f"Empty token should be invalid, but validation returned {is_valid}"

        if not channel or not channel.strip():
            assert (
                not is_valid
            ), f"Empty channel should be invalid, but validation returned {is_valid}"

        # Property: Non-empty tokens and channels should pass basic validation
        if token and token.strip() and channel and channel.strip():
            assert (
                is_valid
            ), f"Non-empty token and channel should be valid, but validation returned {is_valid}"

    @given(
        max_per=st.floats(
            min_value=0.1, max_value=100, allow_nan=False, allow_infinity=False
        ),
        max_pbr=st.floats(
            min_value=0.1, max_value=100, allow_nan=False, allow_infinity=False
        ),
        min_dividend_yield=st.floats(
            min_value=0, max_value=100, allow_nan=False, allow_infinity=False
        ),
        min_growth_years=st.integers(min_value=1, max_value=10),
        max_per_volatility=st.floats(
            min_value=0, max_value=100, allow_nan=False, allow_infinity=False
        ),
    )
    def test_property_8_valid_screening_config_validation(
        self, max_per, max_pbr, min_dividend_yield, min_growth_years, max_per_volatility
    ):
        """
        Property 8: 有効なスクリーニング設定の検証
        For any valid screening configuration values, validation should return True.

        **Validates: Requirements 5.4, 5.5**
        **Feature: stock-value-notifier, Property 8: 設定値の妥当性**
        """
        screening_config = ScreeningConfig(
            max_per=max_per,
            max_pbr=max_pbr,
            min_dividend_yield=min_dividend_yield,
            min_growth_years=min_growth_years,
            max_per_volatility=max_per_volatility,
        )

        is_valid = self.config_manager._validate_screening_config(screening_config)

        # Property: All valid configurations should pass validation
        assert (
            is_valid
        ), f"Valid configuration should pass validation: {screening_config}"

    @given(
        j_quants_token=st.text(min_size=1, max_size=100),
        slack_token=st.text(min_size=1, max_size=100),
        slack_channel=st.text(min_size=1, max_size=100),
    )
    def test_property_8_complete_config_validation(
        self, j_quants_token, slack_token, slack_channel
    ):
        """
        Property 8: 完全な設定の妥当性検証
        For any complete configuration, validation should work consistently.

        **Validates: Requirements 5.4, 5.5**
        **Feature: stock-value-notifier, Property 8: 設定値の妥当性**
        """
        # Assume non-empty strings for this test
        assume(j_quants_token.strip())
        assume(slack_token.strip())
        assume(slack_channel.strip())

        slack_config = SlackConfig(token=slack_token, channel=slack_channel)
        screening_config = ScreeningConfig()  # Use default valid values
        config = Config(
            j_quants_token=j_quants_token,
            slack_config=slack_config,
            screening_config=screening_config,
        )

        is_valid = self.config_manager.validate_config(config)

        # Property: Configuration with non-empty tokens should be valid
        assert (
            is_valid
        ), f"Configuration with non-empty values should be valid: {config}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
