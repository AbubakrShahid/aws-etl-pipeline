import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import boto3
from botocore.exceptions import BotoCoreError, ClientError


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "brazilian_ecommerce"
DATASET_FILES: List[str] = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload Brazilian E-Commerce CSV files to S3 under a timestamped dataset/ prefix."
    )
    parser.add_argument(
        "--bucket-name",
        required=True,
        help="Name of the S3 bucket to upload into.",
    )
    parser.add_argument(
        "--region",
        help="AWS region to use for the S3 client. Defaults to the environment/SDK configuration.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help="Local directory containing the Brazilian E-Commerce CSV files.",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def build_s3_key(timestamp: str, filename: str) -> str:
    return f"dataset/{timestamp}/{filename}"


def main() -> None:
    configure_logging()
    args = parse_args()

    data_dir: Path = args.data_dir
    bucket_name: str = args.bucket_name

    if not data_dir.is_dir():
        logging.error("Data directory does not exist: %s", data_dir)
        raise SystemExit(1)

    missing_files = [name for name in DATASET_FILES if not (data_dir / name).is_file()]
    if missing_files:
        logging.error("Missing required dataset files in %s: %s", data_dir, ", ".join(missing_files))
        raise SystemExit(1)

    session_kwargs = {}
    if args.region:
        session_kwargs["region_name"] = args.region

    s3_client = boto3.client("s3", **session_kwargs)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uploaded_keys: List[str] = []

    for filename in DATASET_FILES:
        local_path = data_dir / filename
        key = build_s3_key(timestamp, filename)

        logging.info("Uploading %s to s3://%s/%s", local_path, bucket_name, key)

        try:
            s3_client.upload_file(str(local_path), bucket_name, key)
        except (BotoCoreError, ClientError) as exc:
            logging.error("Failed to upload %s: %s", local_path, exc)
            raise SystemExit(1)

        uploaded_keys.append(key)

    logging.info("Successfully uploaded %d files to bucket %s", len(uploaded_keys), bucket_name)
    for key in uploaded_keys:
        logging.info("Uploaded object key: %s", key)


if __name__ == "__main__":
    main()

