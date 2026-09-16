output "bronze_bucket_name" {
  value       = google_storage_bucket.bronze.name
  description = "Name of the Bronze GCS bucket."
}

output "silver_bucket_name" {
  value       = google_storage_bucket.silver.name
  description = "Name of the Silver GCS bucket."
}

output "gold_bucket_name" {
  value       = google_storage_bucket.gold.name
  description = "Name of the Gold GCS bucket."
}