variable "project_name" {
  description = "Project name for resource naming."
  type        = string
}

variable "bucket_id" {
  description = "ID of the S3 data bucket."
  type        = string
}

variable "bucket_arn" {
  description = "ARN of the S3 data bucket."
  type        = string
}

variable "glue_scripts_path" {
  description = "Path to the glue_scripts directory."
  type        = string
}

variable "glue_version" {
  description = "Glue version."
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags to apply to resources."
  type        = map(string)
  default     = {}
}
