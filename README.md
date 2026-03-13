AWS ETL Pipeline
================

Overview
--------

This repository implements a small AWS-based ETL pipeline using the Brazilian E-Commerce dataset (Olist). Infrastructure is managed with Terraform; data upload and future ETL logic are implemented in Python.

Current Status (Phase 2)
------------------------

- Project structure under `infra/`, `etl/`, `data/`, and `scripts/`
- Terraform-managed S3 bucket for raw and processed data
- Python script to upload the Brazilian E-Commerce CSV files to S3 with timestamped keys
- Glue ETL job and transformations will be added in later phases

Prerequisites
-------------

- AWS CLI configured with valid credentials
- Terraform >= 1.0
- Python 3.8+
- Brazilian E-Commerce dataset CSVs in `data/brazilian_ecommerce/` (see Kaggle: Brazilian E-Commerce Public Dataset by Olist)

Terraform Setup
---------------

1. Navigate to the Terraform directory:

   ```bash
   cd infra/terraform
   ```

2. Create `terraform.tfvars` from the example:

   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

3. Edit `terraform.tfvars` and set your values:

   - `aws_region` – AWS region (e.g. `us-east-1`)
   - `data_bucket_name` – Globally unique S3 bucket name (e.g. `yourname-etl-bucket-2025`)
   - `project_name` – Optional; defaults to `aws-etl-pipeline`

4. Initialize and apply:

   ```bash
   terraform init
   terraform plan -var-file=terraform.tfvars
   terraform apply -var-file=terraform.tfvars
   ```

5. Note the bucket name from the output; you will need it for the upload script.

Upload Script
-------------

The `upload_brazilian_ecommerce_dataset.py` script uploads the three core Brazilian E-Commerce CSV files to S3 under a timestamped prefix.

**What it does:**

- Reads `olist_customers_dataset.csv`, `olist_orders_dataset.csv`, and `olist_order_items_dataset.csv` from `data/brazilian_ecommerce/`
- Uploads each file to `s3://<bucket>/dataset/<YYYYMMDD_HHMMSS>/<filename>`
- Uses UTC timestamps so multiple runs produce distinct folders

**Arguments:**

| Argument       | Required | Description                                      |
|----------------|----------|--------------------------------------------------|
| `--bucket-name`| Yes      | S3 bucket name (from Terraform output)           |
| `--region`     | No       | AWS region (defaults to SDK/env configuration)   |
| `--data-dir`   | No       | Local path to dataset folder (default: `data/brazilian_ecommerce`) |

**Run the script:**

1. Create and activate a virtual environment (recommended on macOS):

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. From the project root:

   ```bash
   python scripts/upload_brazilian_ecommerce_dataset.py \
     --bucket-name YOUR_BUCKET_NAME \
     --region us-east-1
   ```

Replace `YOUR_BUCKET_NAME` with the value from `terraform.tfvars` or `terraform output data_bucket_name`.

**Verify:**

```bash
aws s3 ls s3://YOUR_BUCKET_NAME/dataset/ --recursive
```
