output "data_bucket_name" {
  description = "Name of the S3 data bucket."
  value       = module.s3.bucket_id
}

output "glue_job_name" {
  description = "Name of the Glue ETL job."
  value       = module.glue.job_name
}

output "athena_workgroup" {
  description = "Name of the Athena workgroup for querying ETL output."
  value       = module.glue.athena_workgroup
}

output "glue_database" {
  description = "Name of the Glue catalog database."
  value       = module.glue.glue_database
}
