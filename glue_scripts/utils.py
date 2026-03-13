"""
Utility functions for the ETL pipeline: structured logging, retries, and input validation.
"""

import json
import logging
import re
import time
import uuid
from functools import wraps
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def generate_correlation_id(job_name: str) -> str:
    """Generate a unique correlation ID for the job run."""
    return f"{job_name}-{uuid.uuid4()}"


class StructuredFormatter(logging.Formatter):
    """Format log records as JSON for CloudWatch Insights queryability."""

    def __init__(self, correlation_id: str):
        super().__init__()
        self.correlation_id = correlation_id

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "correlation_id": self.correlation_id,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_obj)


def configure_structured_logging(correlation_id: str) -> None:
    """Configure the root logger to use structured JSON output."""
    root = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter(correlation_id=correlation_id))
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 2.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[F], F]:
    """Retry a function on transient failures with exponential backoff."""

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc = None
            current_delay = delay_seconds
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts:
                        time.sleep(current_delay)
                        current_delay *= backoff
            raise last_exc

        return wrapper

    return decorator


def validate_job_args(bucket: str, source_prefix: str, output_prefix: str) -> None:
    """
    Validate job arguments before execution. Raises ValueError on invalid input.
    """
    if not bucket or not bucket.strip():
        raise ValueError("bucket must be non-empty")
    if not re.match(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", bucket):
        raise ValueError(
            "bucket must be a valid S3 bucket name (3-63 chars, lowercase, alphanumeric)"
        )
    if not source_prefix or not source_prefix.strip():
        raise ValueError("source_prefix must be non-empty")
    if source_prefix != source_prefix.strip():
        raise ValueError("source_prefix must not have leading/trailing whitespace")
    if not output_prefix or not output_prefix.strip():
        raise ValueError("output_prefix must be non-empty")
    if output_prefix != output_prefix.strip():
        raise ValueError("output_prefix must not have leading/trailing whitespace")
