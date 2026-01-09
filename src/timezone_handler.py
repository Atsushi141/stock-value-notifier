"""
Timezone handling utility for safe datetime operations

This module provides utilities for handling timezone-aware and naive datetime objects
safely, particularly for financial data processing where timezone inconsistencies
can cause comparison errors.
"""

import logging
from datetime import datetime
from typing import Optional, Union
import pandas as pd
import pytz


class TimezoneHandler:
    """
    Utility class for safe timezone handling and datetime operations

    Provides methods for:
    - Safe timezone conversion
    - Unified datetime comparison
    - DataFrame datetime index normalization
    - Fallback processing for timezone errors
    """

    def __init__(self, default_timezone: str = "Asia/Tokyo"):
        """
        Initialize TimezoneHandler

        Args:
            default_timezone: Default timezone to use for conversions
        """
        self.logger = logging.getLogger(__name__)
        self.default_tz = pytz.timezone(default_timezone)
        self.utc = pytz.UTC

    def normalize_datetime_index(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame datetime index to handle timezone inconsistencies

        Args:
            df: DataFrame with datetime index

        Returns:
            DataFrame with normalized datetime index
        """
        if df.empty:
            return df

        try:
            # Check if index is datetime-like
            if not isinstance(df.index, pd.DatetimeIndex):
                return df

            # If index is timezone-aware, convert to default timezone
            if df.index.tz is not None:
                self.logger.debug(
                    f"Converting timezone-aware index from {df.index.tz} to {self.default_tz}"
                )
                df.index = df.index.tz_convert(self.default_tz)
            else:
                # If index is naive, localize to default timezone
                self.logger.debug(
                    f"Localizing naive datetime index to {self.default_tz}"
                )
                df.index = df.index.tz_localize(self.default_tz)

            return df

        except Exception as e:
            self.logger.warning(f"Failed to normalize datetime index: {e}")
            # Fallback: strip timezone information if normalization fails
            try:
                if hasattr(df.index, "tz_localize"):
                    df.index = df.index.tz_localize(None)
                    self.logger.info(
                        "Fallback: Stripped timezone information from index"
                    )
                return df
            except Exception as fallback_error:
                self.logger.error(
                    f"Fallback timezone handling also failed: {fallback_error}"
                )
                return df

    def safe_timezone_comparison(self, dt1: datetime, dt2: datetime) -> bool:
        """
        Safely compare two datetime objects that may have different timezone awareness

        Args:
            dt1: First datetime object
            dt2: Second datetime object

        Returns:
            True if dt1 >= dt2, False otherwise
        """
        try:
            # If both are timezone-aware or both are naive, compare directly
            if (dt1.tzinfo is None) == (dt2.tzinfo is None):
                return dt1 >= dt2

            # If one is timezone-aware and one is naive, normalize both
            normalized_dt1 = self.convert_to_timezone(dt1, self.default_tz.zone)
            normalized_dt2 = self.convert_to_timezone(dt2, self.default_tz.zone)

            return normalized_dt1 >= normalized_dt2

        except Exception as e:
            self.logger.warning(f"Timezone comparison failed: {e}")
            # Fallback: strip timezone info and compare as naive datetimes
            try:
                naive_dt1 = self.strip_timezone_if_needed(dt1)
                naive_dt2 = self.strip_timezone_if_needed(dt2)
                return naive_dt1 >= naive_dt2
            except Exception as fallback_error:
                self.logger.error(f"Fallback comparison also failed: {fallback_error}")
                # Last resort: assume comparison is True to continue processing
                return True

    def convert_to_timezone(self, dt: datetime, target_tz: str) -> datetime:
        """
        Safely convert datetime to target timezone

        Args:
            dt: Datetime object to convert
            target_tz: Target timezone string (e.g., "Asia/Tokyo")

        Returns:
            Datetime object in target timezone
        """
        try:
            target_timezone = pytz.timezone(target_tz)

            if dt.tzinfo is None:
                # Naive datetime - localize to default timezone first, then convert
                localized_dt = self.default_tz.localize(dt)
                return localized_dt.astimezone(target_timezone)
            else:
                # Timezone-aware datetime - convert directly
                return dt.astimezone(target_timezone)

        except Exception as e:
            self.logger.warning(f"Timezone conversion failed: {e}")
            # Fallback: return original datetime
            return dt

    def strip_timezone_if_needed(self, dt: datetime) -> datetime:
        """
        Remove timezone information from datetime if present

        Args:
            dt: Datetime object

        Returns:
            Naive datetime object
        """
        try:
            if dt.tzinfo is not None:
                return dt.replace(tzinfo=None)
            return dt
        except Exception as e:
            self.logger.warning(f"Failed to strip timezone: {e}")
            return dt

    def safe_datetime_filter(
        self, df: pd.DataFrame, start_date: datetime
    ) -> pd.DataFrame:
        """
        Safely filter DataFrame by start date, handling timezone inconsistencies

        Args:
            df: DataFrame with datetime index
            start_date: Start date for filtering

        Returns:
            Filtered DataFrame
        """
        if df.empty:
            return df

        try:
            # Normalize the DataFrame index first
            normalized_df = self.normalize_datetime_index(df)

            # Convert start_date to match the DataFrame index timezone
            if (
                isinstance(normalized_df.index, pd.DatetimeIndex)
                and normalized_df.index.tz is not None
            ):
                # DataFrame has timezone-aware index
                if start_date.tzinfo is None:
                    # start_date is naive, localize it
                    start_date = self.default_tz.localize(start_date)
                # Convert to same timezone as DataFrame
                start_date = start_date.astimezone(normalized_df.index.tz)
            else:
                # DataFrame has naive index, ensure start_date is also naive
                start_date = self.strip_timezone_if_needed(start_date)

            # Perform the filtering
            filtered_df = normalized_df[normalized_df.index >= start_date]
            self.logger.debug(
                f"Filtered DataFrame from {len(normalized_df)} to {len(filtered_df)} records"
            )

            return filtered_df

        except Exception as e:
            self.logger.error(f"Safe datetime filtering failed: {e}")
            # Fallback: try to filter with naive datetimes
            try:
                # Strip timezone from both DataFrame index and start_date
                if hasattr(df.index, "tz_localize"):
                    df_naive = df.copy()
                    df_naive.index = df_naive.index.tz_localize(None)
                else:
                    df_naive = df

                start_date_naive = self.strip_timezone_if_needed(start_date)

                filtered_df = df_naive[df_naive.index >= start_date_naive]
                self.logger.info(
                    f"Fallback filtering successful: {len(filtered_df)} records"
                )
                return filtered_df

            except Exception as fallback_error:
                self.logger.error(f"Fallback filtering also failed: {fallback_error}")
                # Last resort: return original DataFrame
                return df

    def localize_datetime(
        self, dt: datetime, timezone: Optional[str] = None
    ) -> datetime:
        """
        Localize naive datetime to specified timezone

        Args:
            dt: Naive datetime object
            timezone: Timezone string, defaults to default_timezone

        Returns:
            Timezone-aware datetime object
        """
        if dt.tzinfo is not None:
            # Already timezone-aware
            return dt

        try:
            tz = pytz.timezone(timezone) if timezone else self.default_tz
            return tz.localize(dt)
        except Exception as e:
            self.logger.warning(f"Failed to localize datetime: {e}")
            return dt
