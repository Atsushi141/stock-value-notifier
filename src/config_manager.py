"""
Configuration management module for stock value notifier system.
Handles loading and validation of configuration from GitHub Secrets and environment variables.
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional
from .models import RotationConfig


@dataclass
class ScreeningConfig:
    """Configuration for stock screening parameters."""

    max_per: float = 15.0
    max_pbr: float = 1.5
    min_dividend_yield: float = 2.0
    min_growth_years: int = 3
    max_per_volatility: float = 30.0


@dataclass
class SlackConfig:
    """Configuration for Slack notification settings."""

    token: str
    channel: str
    username: str = "バリュー株通知Bot"
    icon_emoji: str = ":chart_with_upwards_trend:"


@dataclass
class Config:
    """Main configuration container."""

    slack_config: SlackConfig
    screening_config: ScreeningConfig
    rotation_config: RotationConfig


class ConfigManager:
    """
    Manages configuration loading and validation from GitHub Secrets and environment variables.

    Handles:
    - Loading configuration from environment variables
    - Validating configuration values
    - Providing default values for invalid configurations
    - Logging warnings for configuration issues
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def load_config_from_env(self) -> Config:
        """
        Load configuration from environment variables (GitHub Secrets).

        Returns:
            Config: Complete configuration object with validated values

        Raises:
            ValueError: If required configuration is missing or invalid
        """
        # Load required tokens
        slack_token = os.getenv("SLACK_BOT_TOKEN")
        slack_channel = os.getenv("SLACK_CHANNEL")

        # Validate required configuration
        if not slack_token:
            raise ValueError("SLACK_BOT_TOKEN environment variable is required")
        if not slack_channel:
            raise ValueError("SLACK_CHANNEL environment variable is required")

        # Create Slack configuration
        slack_config = SlackConfig(
            token=slack_token,
            channel=slack_channel,
            username=os.getenv("SLACK_USERNAME", SlackConfig.username),
            icon_emoji=os.getenv("SLACK_ICON_EMOJI", SlackConfig.icon_emoji),
        )

        # Create screening configuration with validation
        screening_config = self.get_screening_config()

        # Create rotation configuration with validation
        rotation_config = self.get_rotation_config()

        return Config(
            slack_config=slack_config,
            screening_config=screening_config,
            rotation_config=rotation_config,
        )

    def validate_config(self, config: Config) -> bool:
        """
        Validate the complete configuration object.

        Args:
            config: Configuration object to validate

        Returns:
            bool: True if configuration is valid, False otherwise
        """
        try:
            # Validate Slack configuration
            if not self._validate_slack_config(config.slack_config):
                return False

            # Validate screening configuration
            if not self._validate_screening_config(config.screening_config):
                return False

            # Validate rotation configuration
            if not self._validate_rotation_config(config.rotation_config):
                return False

            return True

        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False

    def get_screening_config(self) -> ScreeningConfig:
        """
        Load and validate screening configuration from environment variables.
        Uses default values for invalid or missing configuration.

        Returns:
            ScreeningConfig: Validated screening configuration
        """
        config = ScreeningConfig()

        # Load and validate max_per
        try:
            max_per = float(os.getenv("MAX_PER", config.max_per))
            if max_per <= 0:
                self.logger.warning(
                    f"Invalid MAX_PER value: {max_per}. Using default: {config.max_per}"
                )
            else:
                config.max_per = max_per
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid MAX_PER format. Using default: {config.max_per}"
            )

        # Load and validate max_pbr
        try:
            max_pbr = float(os.getenv("MAX_PBR", config.max_pbr))
            if max_pbr <= 0:
                self.logger.warning(
                    f"Invalid MAX_PBR value: {max_pbr}. Using default: {config.max_pbr}"
                )
            else:
                config.max_pbr = max_pbr
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid MAX_PBR format. Using default: {config.max_pbr}"
            )

        # Load and validate min_dividend_yield
        try:
            min_dividend_yield = float(
                os.getenv("MIN_DIVIDEND_YIELD", config.min_dividend_yield)
            )
            if min_dividend_yield < 0:
                self.logger.warning(
                    f"Invalid MIN_DIVIDEND_YIELD value: {min_dividend_yield}. Using default: {config.min_dividend_yield}"
                )
            else:
                config.min_dividend_yield = min_dividend_yield
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid MIN_DIVIDEND_YIELD format. Using default: {config.min_dividend_yield}"
            )

        # Load and validate min_growth_years
        try:
            min_growth_years = int(
                os.getenv("MIN_GROWTH_YEARS", config.min_growth_years)
            )
            if min_growth_years <= 0:
                self.logger.warning(
                    f"Invalid MIN_GROWTH_YEARS value: {min_growth_years}. Using default: {config.min_growth_years}"
                )
            else:
                config.min_growth_years = min_growth_years
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid MIN_GROWTH_YEARS format. Using default: {config.min_growth_years}"
            )

        # Load and validate max_per_volatility
        try:
            max_per_volatility = float(
                os.getenv("MAX_PER_VOLATILITY", config.max_per_volatility)
            )
            if max_per_volatility < 0:
                self.logger.warning(
                    f"Invalid MAX_PER_VOLATILITY value: {max_per_volatility}. Using default: {config.max_per_volatility}"
                )
            else:
                config.max_per_volatility = max_per_volatility
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid MAX_PER_VOLATILITY format. Using default: {config.max_per_volatility}"
            )

        return config

    def get_slack_config(self) -> SlackConfig:
        """
        Load Slack configuration from environment variables.

        Returns:
            SlackConfig: Slack configuration object

        Raises:
            ValueError: If required Slack configuration is missing
        """
        token = os.getenv("SLACK_BOT_TOKEN")
        channel = os.getenv("SLACK_CHANNEL")

        if not token:
            raise ValueError("SLACK_BOT_TOKEN environment variable is required")
        if not channel:
            raise ValueError("SLACK_CHANNEL environment variable is required")

        return SlackConfig(
            token=token,
            channel=channel,
            username=os.getenv("SLACK_USERNAME", SlackConfig.username),
            icon_emoji=os.getenv("SLACK_ICON_EMOJI", SlackConfig.icon_emoji),
        )

    def get_rotation_config(self) -> RotationConfig:
        """
        Load and validate rotation configuration from environment variables.
        Uses default values for invalid or missing configuration.

        Returns:
            RotationConfig: Validated rotation configuration

        Note: Implements requirements 7.6, 7.7 - rotation mode configuration
        """
        config = RotationConfig()

        # Load SCREENING_MODE to determine if rotation is enabled
        screening_mode = os.getenv("SCREENING_MODE", "curated").lower()
        config.enabled = screening_mode == "rotation"

        if config.enabled:
            self.logger.info("Rotation mode enabled via SCREENING_MODE=rotation")

        # Load and validate total_groups (ROTATION_GROUPS)
        try:
            total_groups = int(os.getenv("ROTATION_GROUPS", config.total_groups))
            if total_groups <= 0 or total_groups > 10:  # Reasonable limits
                self.logger.warning(
                    f"Invalid ROTATION_GROUPS value: {total_groups}. "
                    f"Using default: {config.total_groups}"
                )
            else:
                config.total_groups = total_groups
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid ROTATION_GROUPS format. Using default: {config.total_groups}"
            )

        # Load and validate group_distribution_method
        distribution_method = os.getenv(
            "GROUP_DISTRIBUTION_METHOD", config.group_distribution_method
        ).lower()
        if distribution_method in ["sector", "market_cap"]:
            config.group_distribution_method = distribution_method
        else:
            self.logger.warning(
                f"Invalid GROUP_DISTRIBUTION_METHOD: {distribution_method}. "
                f"Using default: {config.group_distribution_method}"
            )

        return config

    def _validate_slack_config(self, slack_config: SlackConfig) -> bool:
        """
        Validate Slack configuration.

        Args:
            slack_config: Slack configuration to validate

        Returns:
            bool: True if valid, False otherwise
        """
        if not slack_config.token or len(slack_config.token.strip()) == 0:
            self.logger.error("Slack token is empty")
            return False

        if not slack_config.channel or len(slack_config.channel.strip()) == 0:
            self.logger.error("Slack channel is empty")
            return False

        # Basic Slack token format validation (should start with xoxb- for bot tokens)
        if not slack_config.token.startswith("xoxb-"):
            self.logger.warning(
                "Slack token does not appear to be a valid bot token (should start with 'xoxb-')"
            )

        # Basic channel format validation (should start with # or be a channel ID)
        if not (
            slack_config.channel.startswith("#") or slack_config.channel.startswith("C")
        ):
            self.logger.warning(
                "Slack channel should start with '#' or be a channel ID starting with 'C'"
            )

        return True

    def _validate_screening_config(self, screening_config: ScreeningConfig) -> bool:
        """
        Validate screening configuration values.

        Args:
            screening_config: Screening configuration to validate

        Returns:
            bool: True if valid, False otherwise
        """
        if screening_config.max_per <= 0:
            self.logger.error(f"Invalid max_per value: {screening_config.max_per}")
            return False

        if screening_config.max_pbr <= 0:
            self.logger.error(f"Invalid max_pbr value: {screening_config.max_pbr}")
            return False

        if screening_config.min_dividend_yield < 0:
            self.logger.error(
                f"Invalid min_dividend_yield value: {screening_config.min_dividend_yield}"
            )
            return False

        if screening_config.min_growth_years <= 0:
            self.logger.error(
                f"Invalid min_growth_years value: {screening_config.min_growth_years}"
            )
            return False

        if screening_config.max_per_volatility < 0:
            self.logger.error(
                f"Invalid max_per_volatility value: {screening_config.max_per_volatility}"
            )
            return False

        return True

    def _validate_rotation_config(self, rotation_config: RotationConfig) -> bool:
        """
        Validate rotation configuration values.

        Args:
            rotation_config: Rotation configuration to validate

        Returns:
            bool: True if valid, False otherwise
        """
        if rotation_config.total_groups <= 0:
            self.logger.error(
                f"Invalid total_groups value: {rotation_config.total_groups}"
            )
            return False

        if rotation_config.total_groups > 10:
            self.logger.error(
                f"Total groups too high: {rotation_config.total_groups} (max: 10)"
            )
            return False

        if rotation_config.group_distribution_method not in ["sector", "market_cap"]:
            self.logger.error(
                f"Invalid group_distribution_method: {rotation_config.group_distribution_method}"
            )
            return False

        return True

    def get_screening_mode(self) -> str:
        """
        Get the current screening mode from environment variables.

        Returns:
            str: Screening mode ("curated", "all", or "rotation")

        Note: Implements requirement 7.6 - compatibility with existing modes
        """
        mode = os.getenv("SCREENING_MODE", "curated").lower()

        valid_modes = ["curated", "all", "rotation"]
        if mode not in valid_modes:
            self.logger.warning(
                f"Invalid SCREENING_MODE: {mode}. Using default: curated"
            )
            return "curated"

        return mode
