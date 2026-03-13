# AWS ETL Pipeline — Brazilian E-Commerce

## Overview

This project implements a production-grade ETL pipeline on AWS using the
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
All infrastructure is managed with Terraform. The pipeline extracts raw CSV
data from S3, transforms it with AWS Glue (PySpark), and writes clean Parquet
output back to S3 — queryable immediately via Amazon Athena.

---

## Quick Start

If you have AWS CLI configured and the dataset ready:

```bash
make install                    # Install dependencies
make test                       # Run tests locally (optional but recommended)
make deploy                     # Deploy infrastructure (requires terraform.tfvars)
make upload-data                # Upload CSVs to S3
make run-job                    # Run the Glue ETL job
```

Verify output:

```bash
aws s3 ls s3://$(cd infra/terraform && terraform output -raw data_bucket_name)/results/ --recursive
```

Then query in Athena: `SELECT * FROM "aws-etl-pipeline-db"."orders_transformed" LIMIT 10;`

**Before deploy:** Download the [Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place the three CSVs in `data/brazilian_ecommerce/`. Create `infra/terraform/terraform.tfvars` from the example and set `data_bucket_name` (globally unique).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS Account                          │
│                                                             │
│   S3 Bucket                                                 │
│   ├── dataset/<timestamp>/   ← raw CSVs (3 files)          │
│   ├── results/<timestamp>/   ← transformed Parquet          │
│   ├── scripts/etl/           ← Glue job code               │
│   └── athena-results/        ← Athena query output          │
│                                                             │
│   AWS Glue Job                                              │
│   └── Reads CSVs → Validates → Transforms → Writes Parquet │
│                                                             │
│   Glue Data Catalog                                         │
│   └── orders_transformed table → points to results/        │
│                                                             │
│   Amazon Athena                                             │
│   └── Query Parquet output with SQL                        │
│                                                             │
│   CloudWatch Logs                                           │
│   └── Structured JSON logs + job metrics per run           │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```
1. Upload CSVs to s3://bucket/dataset/<timestamp>/
        ↓
2. Glue job reads 3 CSV files with inferSchema
        ↓
3. Validate — required columns, drop critical nulls
        ↓
4. Transform — dedupe, filter delivered orders,
               cast types, add total_value, join
        ↓
5. Write Parquet to s3://bucket/results/<timestamp>/
        ↓
6. Query with Athena SQL
```

---

## Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Three CSV files are used:

| File | Description |
|------|-------------|
| `olist_customers_dataset.csv` | Customer city and state |
| `olist_orders_dataset.csv` | Order status and timestamps |
| `olist_order_items_dataset.csv` | Item prices and freight |

**Why this dataset:** It has multiple related tables requiring a join,
real-world messy data (nulls, mixed statuses, malformed values), and is
large enough to demonstrate meaningful transformations while remaining
cheap to process on Glue.

---

## Project Structure

```
aws-etl-pipeline/
├── glue_scripts/
│   ├── config.py          # Column schemas, validation rules, constants
│   ├── validator.py       # Required column checks, null handling
│   ├── transformer.py     # All transformations and joins
│   ├── metrics.py         # Structured JSON metrics for CloudWatch
│   ├── utils.py           # Logging, retry decorator, job arg validation
│   ├── s3_utils.py        # S3-specific logic (latest prefix resolution)
│   └── main.py            # Glue entry point — thin orchestration only
├── infra/terraform/
│   ├── main.tf            # Root module — calls s3, glue modules
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example
│   └── modules/
│       ├── s3/            # S3 bucket, encryption, public access block
│       └── glue/          # IAM, CloudWatch, Glue job, Catalog, Athena
├── scripts/
│   └── upload_brazilian_ecommerce_dataset.py
├── data/
│   └── brazilian_ecommerce/
│       ├── olist_customers_dataset.csv
│       ├── olist_orders_dataset.csv
│       └── olist_order_items_dataset.csv
├── tests/
│   ├── conftest.py
│   └── unit/
│       ├── test_config.py
│       ├── test_validator.py
│       ├── test_transformer.py
│       ├── test_metrics.py
│       └── test_utils.py
├── .github/workflows/ci.yml
├── Makefile
├── requirements.txt
├── requirements-dev.txt
├── .flake8
└── .gitignore
```

---

## ETL Code Strategy

The Glue job is split into focused, testable modules rather than a single
script. Each module has one responsibility:

**`config.py`** — Single source of truth for all constants. Column names,
null check rules, valid statuses, and output schema are defined here.
Nothing else hardcodes these values.

**`validator.py`** — Validates DataFrames before transformation. Raises
`ValueError` with a clear message if required columns are missing. Logs
and drops rows with nulls in critical columns.

**`transformer.py`** — Pure transformation functions. Each function does
one thing: deduplicate, filter, cast, derive a column, or join. Functions
are independent and individually testable.

**`metrics.py`** — Emits a structured JSON metrics object to CloudWatch
at the end of every run. Includes input/output row counts, drop percentage,
duration, status, and correlation ID. This makes job runs queryable in
CloudWatch Insights.

**`utils.py`** — Cross-cutting utilities: structured JSON logging,
`@retry` decorator with exponential backoff, job argument validation,
and correlation ID generation.

**`s3_utils.py`** — S3-specific logic isolated from general utilities.
Resolves the latest timestamped dataset folder in S3, with retry on
transient boto3 errors.

**`main.py`** — Thin orchestration only. Initialises Spark early,
generates a correlation ID, wires modules together, and handles
success/failure signalling to Glue.

---

## Design Decisions

| Area | Choice | Rationale |
|------|--------|-----------|
| Dataset | Brazilian E-Commerce (3 CSVs) | Requires a join — more realistic than single-table datasets |
| Output format | Parquet | Columnar, compressed, directly queryable by Athena |
| Logging | Structured JSON with correlation ID | Each run is traceable end-to-end in CloudWatch Insights |
| Retries | 3 attempts, 2s delay, 2× backoff | Handles transient S3 and network errors without failing the job |
| Correlation ID | Generated once, passed explicitly | No global mutable state — testable and thread-safe |
| Spark init | Early in `main.py` | Reserves the cluster before any other work begins |
| Null handling | Log warning then drop | Silent data loss is worse than logged data loss |
| Malformed numerics | `cast()` then drop nulls | `cast()` returns null for invalid values; downstream drop catches them |
| Terraform | Modular (s3, glue modules) | Reusable and easier to reason about than a single flat file |
| Athena | Glue Catalog table + workgroup | Completes the full ELT cycle — output is immediately queryable |
| `terraform.tfvars` | Never committed | Contains environment-specific values; documented in `.gitignore` |

---

## Extra: What Goes Beyond the Assessment

The assessment asked for a basic ETL pipeline with Glue, S3, Terraform, and
a README. This implementation adds:

- **Modular code structure** — 6 focused files instead of one script
- **`@retry` decorator** — exponential backoff on transient failures
- **Correlation ID** — every log line and metric tied to one job run
- **Structured JSON metrics** — queryable in CloudWatch Insights
- **27 unit tests** — covering transformations, validation, metrics, and utilities
- **Athena integration** — transformed data queryable with SQL immediately
- **GitHub Actions CI** — tests and lint run on every push
- **Black + Flake8** — automated formatting and linting
- **Makefile** — single commands for every workflow step

---

## Prerequisites

- AWS CLI installed and configured
- Terraform >= 1.0
- Python 3.8+
- Java 8 or 11 (required by PySpark for local tests)
- Brazilian E-Commerce CSVs in `data/brazilian_ecommerce/`
  (download from Kaggle — link above)

---

## Required IAM Permissions

Before running `terraform apply`, your IAM user needs the following.
Add all statements as a single inline policy on your user:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3",
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": "*"
    },
    {
      "Sid": "IAM",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole", "iam:DeleteRole",
        "iam:AttachRolePolicy", "iam:DetachRolePolicy",
        "iam:PutRolePolicy", "iam:GetRole",
        "iam:PassRole", "iam:ListRolePolicies",
        "iam:ListAttachedRolePolicies",
        "iam:GetRolePolicy", "iam:DeleteRolePolicy",
        "iam:CreatePolicy", "iam:DeletePolicy",
        "iam:GetPolicy", "iam:GetPolicyVersion",
        "iam:ListPolicyVersions", "iam:CreatePolicyVersion"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Glue",
      "Effect": "Allow",
      "Action": [
        "glue:CreateJob", "glue:DeleteJob",
        "glue:UpdateJob", "glue:GetJob",
        "glue:StartJobRun", "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:CreateDatabase", "glue:GetDatabase",
        "glue:DeleteDatabase", "glue:CreateTable",
        "glue:GetTable", "glue:UpdateTable",
        "glue:DeleteTable"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:PutRetentionPolicy",
        "logs:DescribeLogGroups",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:TagResource"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws-glue/*"
    },
    {
      "Sid": "Athena",
      "Effect": "Allow",
      "Action": [
        "athena:CreateWorkGroup", "athena:GetWorkGroup",
        "athena:UpdateWorkGroup", "athena:DeleteWorkGroup",
        "athena:ListWorkGroups", "athena:StartQueryExecution",
        "athena:GetQueryExecution", "athena:GetQueryResults",
        "athena:ListTagsForResource", "athena:TagResource"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## Setup and Deployment

### 1. Clone and install dependencies

```bash
git clone <your-repo-url>
cd aws-etl-pipeline
make install
```

`make install` uses `requirements-dev.txt` (pytest, flake8, black, pyspark, boto3). For upload-only: `pip install -r requirements.txt`.

### 2. Download the dataset

Download from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place the three CSVs in `data/brazilian_ecommerce/`:

```
data/brazilian_ecommerce/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
└── olist_order_items_dataset.csv
```

With Kaggle CLI: `kaggle datasets download -d olistbr/brazilian-ecommerce` (then unzip and move the three files).

### 3. Configure Terraform

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
aws_region       = "us-east-1"
data_bucket_name = "yourname-etl-bucket"   # must be globally unique
project_name     = "aws-etl-pipeline"      # optional
```

> **Never commit `terraform.tfvars`** — it is listed in `.gitignore`.

### 4. Deploy infrastructure

```bash
make deploy
```

This runs `terraform init` + `terraform apply`. It creates:
- S3 bucket (with encryption and public access block)
- IAM role and policy for Glue
- CloudWatch log group
- Glue ETL job
- Glue Data Catalog database and table
- Athena workgroup

### 5. Upload the dataset

```bash
make upload-data
```

This uploads the three CSVs to `s3://bucket/dataset/<timestamp>/`.

### 6. Run the Glue job

```bash
make run-job
```

Monitor progress in the AWS Glue console or CloudWatch Logs
(`/aws-glue/jobs/aws-etl-pipeline-brazilian-ecommerce-etl`).

### 7. Verify output

```bash
aws s3 ls s3://$(cd infra/terraform && terraform output -raw data_bucket_name)/results/ --recursive
```

---

## Querying with Athena

1. Open [Amazon Athena](https://console.aws.amazon.com/athena)
2. Select workgroup: `aws-etl-pipeline-workgroup`
3. Select database: `aws-etl-pipeline-db`
4. Run a query:

```sql
-- Preview data
SELECT * FROM orders_transformed LIMIT 10;

-- All orders should be delivered
SELECT DISTINCT order_status FROM orders_transformed;

-- Top cities by revenue
SELECT customer_city,
       COUNT(*) AS order_count,
       ROUND(SUM(total_value), 2) AS total_revenue
FROM orders_transformed
GROUP BY customer_city
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## Running Tests

```bash
make test
```

27 unit tests across 5 files:

| File | What It Tests |
|------|--------------|
| `test_config.py` | Config structure, required keys |
| `test_validator.py` | Column validation, null dropping |
| `test_transformer.py` | Filters, casts, derived columns, edge cases |
| `test_metrics.py` | JobMetrics lifecycle, correlation ID |
| `test_utils.py` | Arg validation, retry decorator behaviour |

Notable edge cases covered:
- All orders cancelled → empty DataFrame after filter
- Malformed numeric strings → become null after cast
- Empty join result → pipeline raises rather than writing empty output

---

## Makefile Reference

| Target | Description |
|--------|-------------|
| `make install` | Install dev dependencies |
| `make test` | Run 27 unit tests |
| `make lint` | Run Flake8 on glue_scripts and tests |
| `make format` | Auto-format with Black |
| `make format-check` | Check formatting without changing files |
| `make deploy` | Terraform init + apply |
| `make destroy` | Terraform destroy — removes all AWS resources |
| `make upload-data` | Upload CSVs to S3 |
| `make run-job` | Start the Glue ETL job |

Override defaults: `make upload-data BUCKET=my-bucket REGION=eu-west-1`

---

## CI

GitHub Actions runs on every push and pull request to `main`/`master`:

- Installs dependencies
- Runs `make test` (27 unit tests)
- Runs `make lint` (Flake8)
- Runs `make format-check` (Black)

See `.github/workflows/ci.yml`.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `make upload-data` fails: "BUCKET not set" | Run `make deploy` first, or pass `BUCKET=your-bucket-name` |
| `make run-job` fails: "JOB_NAME not set" | Run `make deploy` first, or pass `JOB_NAME=your-job-name` |
| Terraform: `logs:CreateLogGroup` AccessDenied | Add CloudWatch Logs permissions to your IAM user (see Required IAM Permissions) |
| Terraform: `BucketNotEmpty` on destroy | Bucket has `force_destroy = true`; run `make destroy` again. If it still fails, empty the bucket manually first |
| CloudWatch log group shows 0 log streams | Job may not have run yet, or logs are in S3 (`spark-logs/`). Check Glue run status |
| Athena: "Table not found" | Ensure the Glue job has run at least once so Parquet files exist under `results/` |

---

## Teardown

To remove all AWS resources created by this project:

```bash
make destroy
```

This runs `terraform destroy` and removes the S3 bucket, Glue job, IAM
role, CloudWatch log group, Glue Catalog, and Athena workgroup.