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
