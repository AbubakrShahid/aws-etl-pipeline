"""
Structured metrics for the ETL job, emitted as JSON for CloudWatch Insights.
"""

import json
import logging
import time

logger = logging.getLogger(__name__)


class JobMetrics:
    """
    Tracks job metrics and emits structured JSON for observability.
    Use mark_success() or mark_failure() to finalize.
    """

    def __init__(self, job_name: str, correlation_id: str):
        self.job_name = job_name
        self.correlation_id = correlation_id
        self.start_time = time.time()
        self.data: dict = {
            "job_name": job_name,
            "correlation_id": correlation_id,
            "input_row_count": 0,
            "output_row_count": 0,
            "dropped_rows": 0,
            "drop_percentage": 0.0,
            "duration_seconds": 0.0,
            "status": "running",
            "error": None,
        }

    def set_input_count(self, count: int) -> None:
        """Set the total input row count before transformations."""
        self.data["input_row_count"] = count

    def set_output_count(self, count: int) -> None:
        """Set the output row count and compute dropped/drop_percentage."""
        self.data["output_row_count"] = count
        dropped = self.data["input_row_count"] - count
        self.data["dropped_rows"] = dropped
        if self.data["input_row_count"] > 0:
            self.data["drop_percentage"] = round(
                dropped / self.data["input_row_count"] * 100, 2
            )

    def mark_success(self) -> None:
        """Mark the job as successful and emit final metrics."""
        self.data["status"] = "success"
        self.data["duration_seconds"] = round(time.time() - self.start_time, 2)
        self.data["correlation_id"] = self.correlation_id
        logger.info("JOB_METRICS %s", json.dumps(self.data))

    def mark_failure(self, error: str) -> None:
        """Mark the job as failed and emit final metrics with error details."""
        self.data["status"] = "failed"
        self.data["error"] = error
        self.data["duration_seconds"] = round(time.time() - self.start_time, 2)
        self.data["correlation_id"] = self.correlation_id
        logger.error("JOB_METRICS %s", json.dumps(self.data))
