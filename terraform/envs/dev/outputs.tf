output "vpc_network_name" {
  value       = module.vpc.network_name
  description = "The deployed VPC network name."
}

output "vpc_subnet_name" {
  value       = module.vpc.subnet_name
  description = "The deployed primary subnetwork name."
}

output "ingestion_service_account_email" {
  value       = module.iam.ingestion_sa_email
  description = "Deployed Ingestion Service Account Email."
}

output "processing_service_account_email" {
  value       = module.iam.processing_sa_email
  description = "Deployed Processing Service Account Email."
}

output "bronze_bucket_name" {
  value       = module.storage.bronze_bucket_name
  description = "Deployed Bronze GCS bucket."
}

output "silver_bucket_name" {
  value       = module.storage.silver_bucket_name
  description = "Deployed Silver GCS bucket."
}

output "gold_bucket_name" {
  value       = module.storage.gold_bucket_name
  description = "Deployed Gold GCS bucket."
}

output "silver_dataset_id" {
  value       = module.bigquery.silver_dataset_id
  description = "Deployed Silver BigQuery dataset ID."
}

output "gold_dataset_id" {
  value       = module.bigquery.gold_dataset_id
  description = "Deployed Gold BigQuery dataset ID."
}

output "wif_provider_name" {
  value       = module.wif.provider_name
  description = "WIF Provider Name for GitHub Actions workflow."
}