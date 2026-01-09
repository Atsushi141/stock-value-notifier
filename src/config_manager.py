"""
Configuration management module for stock value notifier system.
Handles loading and validation of configuration from GitHub Secrets and environment variables.
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any
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
        Enhanced with TSE-specific options.

        Returns:
            RotationConfig: Validated rotation configuration

        Note: Implements requirements 7.6, 7.7 - rotation mode configuration with TSE support
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
        valid_methods = ["sector", "market_size", "mixed", "round_robin"]
        if distribution_method in valid_methods:
            config.group_distribution_method = distribution_method
        else:
            self.logger.warning(
                f"Invalid GROUP_DISTRIBUTION_METHOD: {distribution_method}. "
                f"Valid options: {valid_methods}. Using default: {config.group_distribution_method}"
            )

        # Load TSE-specific configuration options

        # Use 17-sector vs 33-sector classification
        use_17_sector = os.getenv("USE_17_SECTOR_CLASSIFICATION", "true").lower()
        config.use_17_sector_classification = use_17_sector in ["true", "1", "yes"]

        # Balance market categories
        balance_markets = os.getenv("BALANCE_MARKET_CATEGORIES", "true").lower()
        config.balance_market_categories = balance_markets in ["true", "1", "yes"]

        # Use TSE metadata
        use_tse_metadata = os.getenv("USE_TSE_METADATA", "true").lower()
        config.use_tse_metadata = use_tse_metadata in ["true", "1", "yes"]

        # Auto-optimize distribution method
        auto_optimize = os.getenv("AUTO_OPTIMIZE_DISTRIBUTION", "false").lower()
        config.auto_optimize_distribution = auto_optimize in ["true", "1", "yes"]

        # Load optimization weights
        try:
            sector_weight = float(
                os.getenv("SECTOR_BALANCE_WEIGHT", config.sector_balance_weight)
            )
            if 0 <= sector_weight <= 1:
                config.sector_balance_weight = sector_weight
            else:
                self.logger.warning(
                    f"Invalid SECTOR_BALANCE_WEIGHT: {sector_weight}. Using default: {config.sector_balance_weight}"
                )
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid SECTOR_BALANCE_WEIGHT format. Using default: {config.sector_balance_weight}"
            )

        try:
            size_weight = float(
                os.getenv("SIZE_BALANCE_WEIGHT", config.size_balance_weight)
            )
            if 0 <= size_weight <= 1:
                config.size_balance_weight = size_weight
            else:
                self.logger.warning(
                    f"Invalid SIZE_BALANCE_WEIGHT: {size_weight}. Using default: {config.size_balance_weight}"
                )
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid SIZE_BALANCE_WEIGHT format. Using default: {config.size_balance_weight}"
            )

        try:
            group_weight = float(
                os.getenv("GROUP_SIZE_WEIGHT", config.group_size_weight)
            )
            if 0 <= group_weight <= 1:
                config.group_size_weight = group_weight
            else:
                self.logger.warning(
                    f"Invalid GROUP_SIZE_WEIGHT: {group_weight}. Using default: {config.group_size_weight}"
                )
        except (ValueError, TypeError):
            self.logger.warning(
                f"Invalid GROUP_SIZE_WEIGHT format. Using default: {config.group_size_weight}"
            )

        # Validate that weights sum to approximately 1.0
        total_weight = (
            config.sector_balance_weight
            + config.size_balance_weight
            + config.group_size_weight
        )
        if abs(total_weight - 1.0) > 0.01:  # Allow small floating point differences
            self.logger.warning(
                f"Optimization weights sum to {total_weight:.3f}, not 1.0. "
                f"Results may be skewed."
            )

        # Log TSE configuration summary
        if config.enabled:
            self.logger.info(
                f"TSE Rotation Configuration: "
                f"method={config.group_distribution_method}, "
                f"17sector={config.use_17_sector_classification}, "
                f"tse_metadata={config.use_tse_metadata}, "
                f"auto_optimize={config.auto_optimize_distribution}"
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
        Validate rotation configuration values including TSE-specific options.

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

        valid_methods = ["sector", "market_size", "mixed", "round_robin"]
        if rotation_config.group_distribution_method not in valid_methods:
            self.logger.error(
                f"Invalid group_distribution_method: {rotation_config.group_distribution_method}. "
                f"Valid options: {valid_methods}"
            )
            return False

        # Validate optimization weights
        if not (0 <= rotation_config.sector_balance_weight <= 1):
            self.logger.error(
                f"Invalid sector_balance_weight: {rotation_config.sector_balance_weight} (must be 0-1)"
            )
            return False

        if not (0 <= rotation_config.size_balance_weight <= 1):
            self.logger.error(
                f"Invalid size_balance_weight: {rotation_config.size_balance_weight} (must be 0-1)"
            )
            return False

        if not (0 <= rotation_config.group_size_weight <= 1):
            self.logger.error(
                f"Invalid group_size_weight: {rotation_config.group_size_weight} (must be 0-1)"
            )
            return False

        # Check that weights sum to approximately 1.0
        total_weight = (
            rotation_config.sector_balance_weight
            + rotation_config.size_balance_weight
            + rotation_config.group_size_weight
        )
        if abs(total_weight - 1.0) > 0.1:  # Allow some tolerance
            self.logger.warning(
                f"Optimization weights sum to {total_weight:.3f}, not 1.0. "
                f"This may affect optimization results."
            )

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

    def get_tse_rotation_config(self) -> Dict[str, Any]:
        """
        Get TSE-specific rotation configuration details.

        Returns:
            Dict containing TSE rotation configuration information

        Note: Implements requirement 7.6 - TSE configuration options
        """
        rotation_config = self.get_rotation_config()

        return {
            "enabled": rotation_config.enabled,
            "total_groups": rotation_config.total_groups,
            "distribution_method": rotation_config.group_distribution_method,
            "use_17_sector": rotation_config.use_17_sector_classification,
            "balance_markets": rotation_config.balance_market_categories,
            "use_tse_metadata": rotation_config.use_tse_metadata,
            "auto_optimize": rotation_config.auto_optimize_distribution,
            "optimization_weights": {
                "sector_balance": rotation_config.sector_balance_weight,
                "size_balance": rotation_config.size_balance_weight,
                "group_size": rotation_config.group_size_weight,
            },
            "sector_classification": (
                "17業種" if rotation_config.use_17_sector_classification else "33業種"
            ),
        }

    def get_available_distribution_methods(self) -> Dict[str, str]:
        """
        Get available distribution methods with descriptions.

        Returns:
            Dict mapping method names to descriptions

        Note: Implements requirement 7.6 - distribution method selection
        """
        return {
            "round_robin": "Simple round-robin distribution (backward compatible)",
            "sector": "Distribute by sector classification (17業種 or 33業種)",
            "market_size": "Distribute by market size category (TOPIX Small, etc.)",
            "mixed": "Mixed distribution using sector + size + market criteria",
        }

    def get_configuration_help(self) -> Dict[str, Any]:
        """
        Get help information for TSE rotation configuration.

        Returns:
            Dict containing configuration help and examples

        Note: Implements requirement 7.6 - configuration guidance
        """
        return {
            "environment_variables": {
                "SCREENING_MODE": {
                    "description": "Screening mode selection",
                    "options": ["curated", "all", "rotation"],
                    "default": "curated",
                    "example": "rotation",
                },
                "ROTATION_GROUPS": {
                    "description": "Number of rotation groups (1-10)",
                    "type": "integer",
                    "default": 5,
                    "example": "5",
                },
                "GROUP_DISTRIBUTION_METHOD": {
                    "description": "Method for distributing stocks across groups",
                    "options": ["round_robin", "sector", "market_size", "mixed"],
                    "default": "sector",
                    "example": "mixed",
                },
                "USE_17_SECTOR_CLASSIFICATION": {
                    "description": "Use 17-sector vs 33-sector classification",
                    "type": "boolean",
                    "default": "true",
                    "example": "true",
                },
                "BALANCE_MARKET_CATEGORIES": {
                    "description": "Balance market categories across groups",
                    "type": "boolean",
                    "default": "true",
                    "example": "true",
                },
                "USE_TSE_METADATA": {
                    "description": "Use TSE metadata for intelligent distribution",
                    "type": "boolean",
                    "default": "true",
                    "example": "true",
                },
                "AUTO_OPTIMIZE_DISTRIBUTION": {
                    "description": "Automatically select optimal distribution method",
                    "type": "boolean",
                    "default": "false",
                    "example": "false",
                },
                "SECTOR_BALANCE_WEIGHT": {
                    "description": "Weight for sector balance in optimization (0.0-1.0)",
                    "type": "float",
                    "default": 0.3,
                    "example": "0.3",
                },
                "SIZE_BALANCE_WEIGHT": {
                    "description": "Weight for size balance in optimization (0.0-1.0)",
                    "type": "float",
                    "default": 0.3,
                    "example": "0.3",
                },
                "GROUP_SIZE_WEIGHT": {
                    "description": "Weight for group size balance in optimization (0.0-1.0)",
                    "type": "float",
                    "default": 0.4,
                    "example": "0.4",
                },
            },
            "examples": {
                "basic_rotation": {
                    "SCREENING_MODE": "rotation",
                    "ROTATION_GROUPS": "5",
                    "GROUP_DISTRIBUTION_METHOD": "sector",
                },
                "advanced_tse_rotation": {
                    "SCREENING_MODE": "rotation",
                    "ROTATION_GROUPS": "5",
                    "GROUP_DISTRIBUTION_METHOD": "mixed",
                    "USE_17_SECTOR_CLASSIFICATION": "true",
                    "BALANCE_MARKET_CATEGORIES": "true",
                    "USE_TSE_METADATA": "true",
                },
                "auto_optimized_rotation": {
                    "SCREENING_MODE": "rotation",
                    "ROTATION_GROUPS": "5",
                    "AUTO_OPTIMIZE_DISTRIBUTION": "true",
                    "SECTOR_BALANCE_WEIGHT": "0.4",
                    "SIZE_BALANCE_WEIGHT": "0.3",
                    "GROUP_SIZE_WEIGHT": "0.3",
                },
            },
            "notes": [
                "All weights must sum to 1.0 for proper optimization",
                "TSE metadata requires data_j.xls file to be available",
                "Auto-optimization will test all methods and select the best one",
                "17-sector classification provides broader categories than 33-sector",
                "Mixed distribution provides the most balanced sector representation",
            ],
        }
