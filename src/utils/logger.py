import logging
from typing import Any, Dict


class CloudLogger:
    def __init__(self, name: str = __name__):
        """
        Initializes a logging instance for use with Google Cloud Functions.
        Cloud Functions automatically captures logging output and sends it to Cloud Logging.

        Args:
            name (str, optional): The name of the logger instance. Defaults to current module name.
        """
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO)

        self._std_logger = logging.getLogger(name)

    def _format_message(self, message: str, kwargs: Dict[str, Any]) -> str:
        """Format message with additional context if provided."""
        return f"{message} {kwargs}" if kwargs else message

    def info(self, message: str, **kwargs: Any) -> None:
        """
        Logs an informational message that will automatically be sent to Cloud Logging.

        Args:
            message: The message to be logged.
            **kwargs: Additional key-value pairs that will appear in the Cloud Logging entry.
        """
        self._std_logger.info(self._format_message(message, kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        """
        Logs an error message that will automatically be sent to Cloud Logging.

        Args:
            message: The error message to be logged.
            **kwargs: Additional key-value pairs that will appear in the Cloud Logging entry.
        """
        self._std_logger.error(self._format_message(message, kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        """
        Logs a warning message that will automatically be sent to Cloud Logging.

        Args:
            message: The warning message to be logged.
            **kwargs: Additional key-value pairs that will appear in the Cloud Logging entry.
        """
        self._std_logger.warning(self._format_message(message, kwargs))
