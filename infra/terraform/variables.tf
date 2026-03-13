variable "aws_region" {
  description = "AWS region to deploy resources into."
  type        = string
}

variable "project_name" {
  description = "Project name used in resource tags."
  type        = string
  default     = "aws-etl-pipeline"
}

variable "data_bucket_name" {
  description = "Name of the S3 bucket used for raw and processed data."
  type        = string
}

variable "glue_version" {
  description = "Glue version for the ETL job."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X, G.2X, Z.2X)."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}

variable "glue_log_retention_days" {
  description = "CloudWatch log retention in days for the Glue job."
  type        = number
  default     = 14
}
