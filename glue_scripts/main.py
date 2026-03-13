"""
Glue ETL job entry point for the Brazilian E-Commerce pipeline.
Orchestrates read, validate, transform, and write with structured logging and retries.
"""

import logging
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from config import (
    CRITICAL_NULL_CHECKS,
    DATASET_FILES,
    FINAL_COLUMNS,
    MIN_INPUT_ROWS,
    REQUIRED_COLUMNS,
)
from metrics import JobMetrics
from s3_utils import get_latest_dataset_prefix
from transformer import apply_all
from utils import (
    configure_structured_logging,
    generate_correlation_id,
    validate_job_args,
)
from validator import validate_all

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def run_pipeline(
    spark,
    base_path: str,
    output_path: str,
    metrics: JobMetrics,
    glue_logger,
) -> None:
    """
    Execute the ETL pipeline: read, validate, transform, write.
    Raises on validation or unexpected errors.
    """
    raw = {
        name: spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{base_path}/{filename}")
        for name, filename in DATASET_FILES.items()
    }

    total_input = sum(df.count() for df in raw.values())
    metrics.set_input_count(total_input)
    glue_logger.info(f"Total input rows: {total_input}")

    if total_input < MIN_INPUT_ROWS:
        raise ValueError(
            f"Input has {total_input} rows; minimum required is {MIN_INPUT_ROWS}. "
            "Ensure dataset files exist and contain data."
        )

    cleaned = validate_all(raw, REQUIRED_COLUMNS, CRITICAL_NULL_CHECKS)

    joined_df = apply_all(
        cleaned["customers"],
        cleaned["orders"],
        cleaned["order_items"],
    )

    output_count = joined_df.count()
    if output_count == 0:
        raise ValueError(
            "Join produced 0 rows. Check that orders, order_items, and customers "
            "have matching keys and delivered orders exist."
        )

    final_df = joined_df.select(*FINAL_COLUMNS)
    metrics.set_output_count(output_count)
    glue_logger.info(f"Final output rows: {output_count}")

    final_df.write.mode("overwrite").parquet(output_path)
    glue_logger.info(f"Written to {output_path}")


args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "bucket", "source_prefix", "output_prefix"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

glue_logger = glue_context.get_logger()
correlation_id = generate_correlation_id(args["JOB_NAME"])
configure_structured_logging(correlation_id=correlation_id)

bucket = args["bucket"]
source_prefix = args["source_prefix"]
output_prefix = args["output_prefix"]

validate_job_args(bucket, source_prefix, output_prefix)

if source_prefix == "dataset":
    source_prefix = get_latest_dataset_prefix(bucket, "dataset")
    glue_logger.info(f"Using latest dataset prefix: {source_prefix}")

base_path = f"s3://{bucket}/{source_prefix}"
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
# Output path includes timestamp per run. Note: Athena table at results/ will
# accumulate all runs (duplicates across runs).
output_path = f"s3://{bucket}/{output_prefix}/{timestamp}"
metrics = JobMetrics(args["JOB_NAME"], correlation_id=correlation_id)

glue_logger.info(f"Source: {base_path} | Output: {output_path}")

try:
    run_pipeline(spark, base_path, output_path, metrics, glue_logger)
    metrics.mark_success()
    job.commit()
    glue_logger.info("Glue job completed successfully")

except ValueError as ve:
    metrics.mark_failure(str(ve))
    glue_logger.error(f"Validation error: {ve}")
    raise
except Exception as e:
    metrics.mark_failure(str(e))
    glue_logger.error(f"Unexpected failure: {e}")
    raise
