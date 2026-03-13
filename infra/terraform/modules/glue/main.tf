data "archive_file" "glue_libs" {
  type        = "zip"
  output_path = "${path.module}/glue_libs.zip"

  source {
    content  = file("${var.glue_scripts_path}/config.py")
    filename = "config.py"
  }
  source {
    content  = file("${var.glue_scripts_path}/validator.py")
    filename = "validator.py"
  }
  source {
    content  = file("${var.glue_scripts_path}/transformer.py")
    filename = "transformer.py"
  }
  source {
    content  = file("${var.glue_scripts_path}/metrics.py")
    filename = "metrics.py"
  }
  source {
    content  = file("${var.glue_scripts_path}/utils.py")
    filename = "utils.py"
  }
  source {
    content  = file("${var.glue_scripts_path}/s3_utils.py")
    filename = "s3_utils.py"
  }
}

resource "aws_s3_object" "glue_script" {
  bucket = var.bucket_id
  key    = "scripts/etl/main.py"
  source = "${var.glue_scripts_path}/main.py"
  etag   = filemd5("${var.glue_scripts_path}/main.py")
}

resource "aws_s3_object" "glue_libs" {
  bucket = var.bucket_id
  key    = "scripts/etl/glue_libs.zip"
  source = data.archive_file.glue_libs.output_path
  etag   = filemd5(data.archive_file.glue_libs.output_path)
}

resource "aws_cloudwatch_log_group" "glue_job" {
  name             = "/aws-glue/jobs/${var.project_name}-brazilian-ecommerce-etl"
  retention_in_days = var.log_retention_days
  tags             = var.tags
}

resource "aws_iam_role" "glue_job" {
  name = "${var.project_name}-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "glue_job" {
  name = "${var.project_name}-glue-job-policy"
  role = aws_iam_role.glue_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          var.bucket_arn,
          "${var.bucket_arn}/dataset/*",
          "${var.bucket_arn}/scripts/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${var.bucket_arn}/results/*",
          "${var.bucket_arn}/spark-logs/*",
          "${var.bucket_arn}/temp/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.glue_job.arn}:*"
      }
    ]
  })
}

resource "aws_glue_job" "brazilian_ecommerce_etl" {
  name     = "${var.project_name}-brazilian-ecommerce-etl"
  role_arn = aws_iam_role.glue_job.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_id}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"               = "job-bookmark-disable"
    "--enable-metrics"                    = "true"
    "--enable-continuous-cloudwatch-log"  = "true"
    "--continuous-log-logGroup"           = aws_cloudwatch_log_group.glue_job.name
    "--enable-spark-ui"                   = "true"
    "--spark-event-logs-path"             = "s3://${var.bucket_id}/spark-logs/"
    "--TempDir"                           = "s3://${var.bucket_id}/temp/"
    "--extra-py-files"                    = "s3://${var.bucket_id}/${aws_s3_object.glue_libs.key}"
    "--bucket"                            = var.bucket_id
    "--source_prefix"                     = "dataset"
    "--output_prefix"                     = "results"
  }

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers

  tags = var.tags
}

# --- Athena / Glue Data Catalog ---

resource "aws_glue_catalog_database" "etl_db" {
  name        = "${var.project_name}-db"
  description = "Glue database for ETL output tables"
}

resource "aws_glue_catalog_table" "orders_transformed" {
  name          = "orders_transformed"
  database_name = aws_glue_catalog_database.etl_db.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_id}/results/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "customer_city"
      type = "string"
    }
    columns {
      name = "customer_state"
      type = "string"
    }
    columns {
      name = "order_status"
      type = "string"
    }
    columns {
      name = "order_purchase_timestamp"
      type = "timestamp"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "freight_value"
      type = "double"
    }
    columns {
      name = "total_value"
      type = "double"
    }
  }
}

resource "aws_athena_workgroup" "etl" {
  name = "${var.project_name}-workgroup"

  configuration {
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.bucket_id}/athena-results/"
    }
  }
}
