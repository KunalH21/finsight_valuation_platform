output "ingestion_sa_email" {
  value       = google_service_account.ingestion_sa.email
  description = "The email address of the ingestion service account."
}

output "ingestion_sa_name" {
  value       = google_service_account.ingestion_sa.name
  description = "The fully qualified resource name of the ingestion service account."
}

output "processing_sa_email" {
  value       = google_service_account.processing_sa.email
  description = "The email address of the processing service account."
}

output "processing_sa_name" {
  value       = google_service_account.processing_sa.name
  description = "The fully qualified resource name of the processing service account."
}