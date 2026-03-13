terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  common_tags = {
    Project   = var.project_name
    ManagedBy = "Terraform"
  }
  glue_scripts_path = "${path.module}/../../glue_scripts"
}

module "s3" {
  source = "./modules/s3"

  bucket_name   = var.data_bucket_name
  tags          = local.common_tags
}

module "glue" {
  source = "./modules/glue"

  project_name      = var.project_name
  bucket_id         = module.s3.bucket_id
  bucket_arn        = module.s3.bucket_arn
  glue_scripts_path = local.glue_scripts_path
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  log_retention_days = var.glue_log_retention_days
  tags              = local.common_tags
}
