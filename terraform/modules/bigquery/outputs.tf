output "silver_dataset_id" {
  value       = google_bigquery_dataset.silver.dataset_id
  description = "ID of the Silver BigQuery Dataset."
}

output "gold_dataset_id" {
  value       = google_bigquery_dataset.gold.dataset_id
  description = "ID of the Gold BigQuery Dataset."
}