"""
Secure logging utilities for medical data applications.

This module provides secure logging functions that prevent sensitive data leakage
while maintaining useful debugging and audit capabilities for production environments.
"""

import logging
import re
from typing import Any, Optional


class SecureLogger:
    """
    Secure logging wrapper that sanitizes sensitive data before logging.

    Designed for medical applications where patient data must be protected
    while maintaining audit trails and debugging capabilities.
    """

    # Patterns that should never appear in logs
    SENSITIVE_PATTERNS = [
        r'(?i)password[\'"]?\s*[:=]\s*[\'"]?([^\s\'"]+)',  # Password fields
        r'(?i)pwd[\'"]?\s*[:=]\s*[\'"]?([^\s\'"]+)',  # PWD fields
        r'(?i)secret[\'"]?\s*[:=]\s*[\'"]?([^\s\'"]+)',  # Secret fields
        r'(?i)token[\'"]?\s*[:=]\s*[\'"]?([^\s\'"]+)',  # Token fields
        r'(?i)key[\'"]?\s*[:=]\s*[\'"]?([^\s\'"]+)',  # Key fields
    ]

    # Patient data patterns that should be minimized in logs
    PATIENT_DATA_PATTERNS = [
        r"\b\d{4}-\d{2}-\d{2}\b",  # Date patterns (DOB)
        r"\b\d{1,2}/\d{1,2}/\d{4}\b",  # Date patterns (MM/DD/YYYY)
        r"\b\d{1,2}\.\d{1,2}\.\d{4}\b",  # Date patterns (DD.MM.YYYY)
    ]

    def __init__(self, logger: logging.Logger, production_mode: bool = True):
        """
        Initialize secure logger wrapper.

        Args:
            logger: The underlying logger instance
            production_mode: If True, applies strict security filtering
        """
        self.logger = logger
        self.production_mode = production_mode

    def _sanitize_message(self, message: str) -> str:
        """
        Sanitize log message by removing/masking sensitive data.

        Args:
            message: Original log message

        Returns:
            Sanitized message safe for logging
        """
        sanitized = message

        # Remove sensitive authentication data
        for pattern in self.SENSITIVE_PATTERNS:
            sanitized = re.sub(pattern, r"\1=***REDACTED***", sanitized)

        # In production mode, also sanitize patient data patterns
        if self.production_mode:
            for pattern in self.PATIENT_DATA_PATTERNS:
                sanitized = re.sub(pattern, "***DATE***", sanitized)

        return sanitized

    def _sanitize_params(self, params: Any) -> str:
        """
        Safely represent query parameters for logging.

        Args:
            params: SQL query parameters (tuple, list, dict, etc.)

        Returns:
            Safe string representation of parameters
        """
        if params is None:
            return "None"

        try:
            if isinstance(params, (tuple, list)):
                if not params:
                    return "[]"

                # In production, only show parameter count and types
                if self.production_mode:
                    param_info = []
                    for i, param in enumerate(params):
                        if isinstance(param, str) and len(param) > 20:
                            param_info.append(f"param_{i}=<string[{len(param)}]>")
                        elif isinstance(param, (int, float)):
                            param_info.append(f"param_{i}=<{type(param).__name__}>")
                        else:
                            param_info.append(f"param_{i}=<{type(param).__name__}>")
                    return f"[{', '.join(param_info)}]"
                else:
                    # Development mode: show first few characters of strings
                    safe_params = []
                    for param in params:
                        if isinstance(param, str) and len(param) > 10:
                            safe_params.append(f"{param[:3]}...{param[-3:]}")
                        else:
                            safe_params.append(str(param))
                    return f"[{', '.join(safe_params)}]"

            elif isinstance(params, dict):
                if self.production_mode:
                    return f"<dict with {len(params)} keys>"
                else:
                    safe_dict = {}
                    for key, value in params.items():
                        if isinstance(value, str) and len(value) > 10:
                            safe_dict[key] = f"{value[:3]}...{value[-3:]}"
                        else:
                            safe_dict[key] = value
                    return str(safe_dict)
            else:
                return f"<{type(params).__name__}>"

        except Exception:
            return "<error serializing parameters>"

    def _get_sql_summary(self, sql: str) -> str:
        """
        Create a safe summary of SQL query for logging.

        Args:
            sql: SQL query string

        Returns:
            Safe summary of the SQL query
        """
        if not sql:
            return "<empty query>"

        try:
            # Extract the main SQL command
            sql_clean = " ".join(sql.split())  # Normalize whitespace
            first_word = sql_clean.split()[0].upper() if sql_clean.split() else "UNKNOWN"

            # Count approximate length and complexity
            word_count = len(sql_clean.split())

            if self.production_mode:
                return f"<{first_word} query, {word_count} tokens>"
            else:
                # Development mode: show more detail but still sanitized
                if len(sql_clean) > 100:
                    return f"{first_word}: {sql_clean[:50]}...{sql_clean[-20:]}"
                else:
                    return f"{first_word}: {sql_clean}"

        except Exception:
            return "<error parsing SQL>"

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message with security filtering."""
        if self.logger.isEnabledFor(logging.DEBUG):
            sanitized = self._sanitize_message(message)
            self.logger.debug(sanitized, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message with security filtering."""
        sanitized = self._sanitize_message(message)
        self.logger.info(sanitized, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message with security filtering."""
        sanitized = self._sanitize_message(message)
        self.logger.warning(sanitized, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message with security filtering."""
        sanitized = self._sanitize_message(message)
        self.logger.error(sanitized, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message with security filtering."""
        sanitized = self._sanitize_message(message)
        self.logger.critical(sanitized, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        """Log exception message with security filtering."""
        sanitized = self._sanitize_message(message)
        self.logger.exception(sanitized, **kwargs)

    def log_database_operation(
        self,
        operation: str,
        success: bool = True,
        duration_ms: Optional[float] = None,
        row_count: Optional[int] = None,
    ) -> None:
        """
        Log database operation in a secure, audit-friendly way.

        Args:
            operation: Type of operation (SELECT, INSERT, etc.)
            success: Whether operation succeeded
            duration_ms: Operation duration in milliseconds
            row_count: Number of rows affected/returned
        """
        status = "SUCCESS" if success else "FAILED"
        duration_str = f", {duration_ms:.2f}ms" if duration_ms is not None else ""
        row_str = f", {row_count} rows" if row_count is not None else ""

        self.info(f"DB_AUDIT: {operation} {status}{duration_str}{row_str}")

    def log_sql_execution(
        self,
        sql: str,
        params: Any = None,
        success: bool = True,
        duration_ms: Optional[float] = None,
    ) -> None:
        """
        Securely log SQL query execution.

        Args:
            sql: SQL query string
            params: Query parameters
            success: Whether execution succeeded
            duration_ms: Execution duration in milliseconds
        """
        sql_summary = self._get_sql_summary(sql)
        param_summary = self._sanitize_params(params)
        status = "SUCCESS" if success else "FAILED"
        duration_str = f" ({duration_ms:.2f}ms)" if duration_ms is not None else ""

        self.debug(f"SQL_EXEC: {sql_summary} | PARAMS: {param_summary} | {status}{duration_str}")

    def log_patient_search(
        self,
        search_type: str,
        criteria_count: int,
        results_count: int,
        duration_ms: Optional[float] = None,
    ) -> None:
        """
        Log patient search operations without exposing sensitive data.

        Args:
            search_type: Type of search (fuzzy, exact, etc.)
            criteria_count: Number of search criteria used
            results_count: Number of results returned
            duration_ms: Search duration in milliseconds
        """
        duration_str = f" ({duration_ms:.2f}ms)" if duration_ms is not None else ""
        self.info(
            f"PATIENT_SEARCH: {search_type} search with {criteria_count} criteria "
            f"returned {results_count} results{duration_str}",
        )

    def log_authentication_event(
        self,
        event_type: str,
        username: Optional[str] = None,
        success: bool = True,
        details: Optional[str] = None,
    ) -> None:
        """
        Log authentication events securely.

        Args:
            event_type: Type of event (login, logout, etc.)
            username: Username (will be partially masked)
            success: Whether event succeeded
            details: Additional details (will be sanitized)
        """
        status = "SUCCESS" if success else "FAILED"
        user_str = ""

        if username:
            # Mask username for security
            masked_user = f"{username[:2]}***{username[-1:]}" if len(username) > 4 else "***"
            user_str = f" user={masked_user}"

        details_str = f" | {self._sanitize_message(details)}" if details else ""

        self.info(f"AUTH: {event_type} {status}{user_str}{details_str}")


def get_secure_logger(name: str, production_mode: bool = True) -> SecureLogger:
    """
    Get a secure logger instance.

    Args:
        name: Logger name (typically __name__)
        production_mode: Enable production security filtering

    Returns:
        SecureLogger instance
    """
    base_logger = logging.getLogger(name)
    return SecureLogger(base_logger, production_mode=production_mode)


def configure_secure_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    production_mode: bool = True,
) -> None:
    """
    Configure secure logging for the entire application.

    Args:
        level: Logging level
        log_file: Optional log file path
        production_mode: Enable production security filtering
    """
    # Create formatters that don't expose sensitive information
    if production_mode:
        # Production format: minimal, structured
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        # Development format: more detailed
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Set production mode as a module-level variable for easy access
    global _PRODUCTION_MODE
    _PRODUCTION_MODE = production_mode


# Module-level variable to track production mode
_PRODUCTION_MODE = True


def is_production_mode() -> bool:
    """Check if logging is in production mode."""
    return _PRODUCTION_MODE
