output "data_bucket_name" {
  description = "Name of the S3 data bucket."
  value       = aws_s3_bucket.data.bucket
}

