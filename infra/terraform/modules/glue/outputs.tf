output "job_name" {
  description = "Name of the Glue ETL job."
  value       = aws_glue_job.brazilian_ecommerce_etl.name
}

output "athena_workgroup" {
  description = "Name of the Athena workgroup for querying ETL output."
  value       = aws_athena_workgroup.etl.name
}

output "glue_database" {
  description = "Name of the Glue catalog database."
  value       = aws_glue_catalog_database.etl_db.name
}
