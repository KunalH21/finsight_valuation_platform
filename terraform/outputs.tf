output "bronze_bucket_arn" {
  value = aws_s3_bucket.bronze.arn
}

output "silver_bucket_arn" {
  value = aws_s3_bucket.silver.arn
}

output "gold_bucket_arn" {
  value = aws_s3_bucket.gold.arn
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.bucket
}

output "bronze_ingestion_prefix" {
  value = trim(var.bronze_ingestion_prefix, "/")
}

output "finsight_ingestion_role_arn" {
  value = aws_iam_role.finsight_ingestion_role.arn
}

output "finsight_ingestion_user_arn" {
  value = aws_iam_user.finsight_ingestion_user.arn
}

output "finsight_ingestion_user_access_key_id" {
  value     = aws_iam_access_key.finsight_ingestion_user_key.id
  sensitive = true
}

output "finsight_ingestion_user_secret_access_key" {
  value     = aws_iam_access_key.finsight_ingestion_user_key.secret
  sensitive = true
}
