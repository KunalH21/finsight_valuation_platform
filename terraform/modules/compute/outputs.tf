output "ingestion_job_name" {
  value = google_cloud_run_v2_job.ingestion_job.name
}

output "processing_job_name" {
  value = google_cloud_run_v2_job.processing_job.name
}