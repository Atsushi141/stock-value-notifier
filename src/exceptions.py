"""
Custom exceptions for the stock value notifier system.

This module defines all custom exceptions used throughout the system
to avoid circular import issues.
"""

from typing import Optional, Dict, Any


class APIError(Exception):
    """Custom exception for API-related errors"""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class AuthenticationError(APIError):
    """Exception for authentication-related errors"""

    pass


class RateLimitError(APIError):
    """Exception for rate limit errors"""

    pass


class DataNotFoundError(APIError):
    """Exception for data not found errors"""

    pass
