#!/usr/bin/env python3
"""
Main execution script for the stock value notifier system.

This script serves as the entry point for GitHub Actions workflow execution.
It integrates all system components and orchestrates the daily screening workflow.

Usage:
    python main.py

Environment Variables (GitHub Secrets):
    SLACK_BOT_TOKEN: Slack Bot Token for notifications
    SLACK_CHANNEL: Target Slack channel for notifications

Optional Environment Variables:
    MAX_PER: Maximum PER threshold (default: 15.0)
    MAX_PBR: Maximum PBR threshold (default: 1.5)
    MIN_DIVIDEND_YIELD: Minimum dividend yield threshold (default: 2.0)
    MIN_GROWTH_YEARS: Minimum consecutive growth years (default: 3)
    MAX_PER_VOLATILITY: Maximum PER volatility coefficient (default: 30.0)
    SLACK_USERNAME: Bot username (default: "バリュー株通知Bot")
    SLACK_ICON_EMOJI: Bot icon emoji (default: ":chart_with_upwards_trend:")

Requirements Addressed:
- 全体統合: Complete system integration
- GitHub Actions環境での実行対応: GitHub Actions execution support
- 要件4.1, 4.2: Scheduled execution with market day validation
- 要件6.1, 6.2: Comprehensive logging and error handling
"""

import sys
import os
import logging
from pathlib import Path

# Add src directory to Python path for module imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src.workflow_runner import WorkflowRunner


def setup_github_actions_logging():
    """
    Setup logging configuration optimized for GitHub Actions environment.

    GitHub Actions provides special logging commands that can be used to:
    - Set output variables
    - Create annotations
    - Group log output
    - Set environment variables
    """
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Configure root logger for GitHub Actions
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),  # GitHub Actions captures stdout
            logging.FileHandler(logs_dir / "github_actions.log"),
        ],
    )

    # Set up GitHub Actions specific logging
    logger = logging.getLogger(__name__)

    # GitHub Actions workflow commands
    logger.info("::group::Stock Value Notifier Initialization")
    logger.info("Setting up stock value notifier system...")
    logger.info("::endgroup::")

    return logger


def validate_github_actions_environment():
    """
    Validate that all required environment variables are available.

    This function checks for required GitHub Secrets and provides
    helpful error messages for missing configuration.

    Returns:
        bool: True if environment is valid, False otherwise
    """
    logger = logging.getLogger(__name__)

    logger.info("::group::Environment Validation")

    required_vars = {
        "SLACK_BOT_TOKEN": "Slack Bot Token for sending notifications",
        "SLACK_CHANNEL": "Target Slack channel for notifications",
    }

    missing_vars = []

    for var_name, description in required_vars.items():
        value = os.getenv(var_name)
        if not value:
            missing_vars.append(f"{var_name}: {description}")
            logger.error(f"::error::Missing required environment variable: {var_name}")
        else:
            # Don't log the actual token value for security
            if "TOKEN" in var_name:
                logger.info(f"✓ {var_name}: [CONFIGURED]")
            else:
                logger.info(f"✓ {var_name}: {value}")

    # Log optional environment variables
    optional_vars = {
        "MAX_PER": os.getenv("MAX_PER", "15.0"),
        "MAX_PBR": os.getenv("MAX_PBR", "1.5"),
        "MIN_DIVIDEND_YIELD": os.getenv("MIN_DIVIDEND_YIELD", "2.0"),
        "MIN_GROWTH_YEARS": os.getenv("MIN_GROWTH_YEARS", "3"),
        "MAX_PER_VOLATILITY": os.getenv("MAX_PER_VOLATILITY", "30.0"),
        "SLACK_USERNAME": os.getenv("SLACK_USERNAME", "バリュー株通知Bot"),
        "SLACK_ICON_EMOJI": os.getenv("SLACK_ICON_EMOJI", ":chart_with_upwards_trend:"),
        "TARGET_DATE": os.getenv("TARGET_DATE", ""),
        "FORCE_EXECUTION": os.getenv("FORCE_EXECUTION", "false"),
    }

    logger.info("Optional configuration:")
    for var_name, value in optional_vars.items():
        logger.info(f"  {var_name}: {value}")

    logger.info("::endgroup::")

    if missing_vars:
        logger.error("::group::Configuration Errors")
        logger.error("The following required environment variables are missing:")
        for var_info in missing_vars:
            logger.error(f"  - {var_info}")
        logger.error("")
        logger.error("Please configure these variables in your GitHub repository:")
        logger.error("1. Go to your repository settings")
        logger.error("2. Navigate to 'Secrets and variables' > 'Actions'")
        logger.error("3. Add the missing secrets")
        logger.error("::endgroup::")
        return False

    return True


def log_system_information():
    """Log system information for debugging and monitoring purposes."""
    logger = logging.getLogger(__name__)

    logger.info("::group::System Information")

    # Python version
    logger.info(f"Python version: {sys.version}")

    # GitHub Actions environment information
    github_vars = {
        "GITHUB_WORKFLOW": "Workflow name",
        "GITHUB_RUN_ID": "Unique run identifier",
        "GITHUB_RUN_NUMBER": "Run number",
        "GITHUB_ACTOR": "User who triggered the workflow",
        "GITHUB_REPOSITORY": "Repository name",
        "GITHUB_REF": "Branch or tag ref",
        "GITHUB_SHA": "Commit SHA",
        "GITHUB_EVENT_NAME": "Event that triggered the workflow",
    }

    logger.info("GitHub Actions environment:")
    for var_name, description in github_vars.items():
        value = os.getenv(var_name, "Not set")
        logger.info(f"  {var_name}: {value}")

    # Working directory
    logger.info(f"Working directory: {os.getcwd()}")

    # Available disk space
    try:
        import shutil

        total, used, free = shutil.disk_usage(os.getcwd())
        logger.info(
            f"Disk space - Total: {total // (1024**3)}GB, "
            f"Used: {used // (1024**3)}GB, "
            f"Free: {free // (1024**3)}GB"
        )
    except Exception as e:
        logger.warning(f"Could not get disk usage: {e}")

    logger.info("::endgroup::")


def main():
    """
    Main entry point for the stock value notifier system.

    This function:
    1. Sets up GitHub Actions optimized logging
    2. Validates the environment configuration
    3. Logs system information for debugging
    4. Initializes and runs the WorkflowRunner
    5. Handles any critical errors with proper GitHub Actions annotations

    Exit codes:
    - 0: Success
    - 1: Configuration error
    - 2: Workflow execution error
    - 3: Unexpected error
    """
    # Setup logging first
    logger = setup_github_actions_logging()

    try:
        logger.info("🚀 Starting Stock Value Notifier System")
        logger.info("=" * 60)

        # Log system information
        log_system_information()

        # Validate environment
        if not validate_github_actions_environment():
            logger.error("::error::Environment validation failed")
            logger.error("Please check your GitHub Secrets configuration")
            sys.exit(1)

        logger.info("✅ Environment validation passed")

        # Initialize and run the workflow
        logger.info("::group::Workflow Execution")
        logger.info("Initializing WorkflowRunner...")

        workflow_runner = WorkflowRunner()

        logger.info("Starting main workflow execution...")
        workflow_runner.main()

        logger.info("::endgroup::")

        logger.info("=" * 60)
        logger.info("✅ Stock Value Notifier completed successfully")

        # Set GitHub Actions output for success
        print("::set-output name=status::success")
        print("::set-output name=message::Stock screening completed successfully")

        sys.exit(0)

    except ValueError as e:
        # Configuration or validation errors
        logger.error("::group::Configuration Error")
        logger.error(f"::error::Configuration error: {str(e)}")
        logger.error("Please check your environment variables and GitHub Secrets")
        logger.error("::endgroup::")

        # Set GitHub Actions output for configuration error
        print(f"::set-output name=status::config_error")
        print(f"::set-output name=message::Configuration error: {str(e)}")

        sys.exit(1)

    except Exception as e:
        # Workflow execution or unexpected errors
        logger.error("::group::Workflow Error")
        logger.error(f"::error::Workflow execution failed: {str(e)}")
        logger.exception("Full error details:")
        logger.error("::endgroup::")

        # Set GitHub Actions output for workflow error
        print(f"::set-output name=status::workflow_error")
        print(f"::set-output name=message::Workflow failed: {str(e)}")

        # Try to send error notification if possible
        try:
            logger.info("Attempting to send error notification...")
            # The WorkflowRunner should handle error notifications
            # but if it fails during initialization, we can't rely on it
        except Exception as notification_error:
            logger.error(f"Failed to send error notification: {notification_error}")

        sys.exit(2)

    except KeyboardInterrupt:
        # Handle manual interruption gracefully
        logger.warning("::warning::Workflow interrupted by user")
        logger.info("Cleaning up and exiting...")

        print("::set-output name=status::interrupted")
        print("::set-output name=message::Workflow was interrupted")

        sys.exit(130)  # Standard exit code for SIGINT

    except SystemExit:
        # Re-raise SystemExit to preserve exit codes
        raise

    except BaseException as e:
        # Catch any other unexpected errors (like SystemError, etc.)
        logger.critical("::group::Critical System Error")
        logger.critical(f"::error::Critical system error: {str(e)}")
        logger.exception("Full error details:")
        logger.critical("::endgroup::")

        print(f"::set-output name=status::critical_error")
        print(f"::set-output name=message::Critical error: {str(e)}")

        sys.exit(3)


if __name__ == "__main__":
    main()
