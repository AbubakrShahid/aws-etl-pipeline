"""
S3-specific utilities for the ETL pipeline.
"""

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from utils import retry


@retry(
    max_attempts=3,
    delay_seconds=2.0,
    backoff=2.0,
    exceptions=(BotoCoreError, ClientError),
)
def get_latest_dataset_prefix(bucket: str, base: str) -> str:
    """
    List S3 prefixes under the given base and return the latest (lexicographically).
    Retries on transient S3/network failures.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{base}/", Delimiter="/"):
        for p in page.get("CommonPrefixes", []):
            prefixes.append(p["Prefix"].rstrip("/"))
    if not prefixes:
        raise ValueError(f"No dataset folders found under s3://{bucket}/{base}/")
    return max(prefixes)
