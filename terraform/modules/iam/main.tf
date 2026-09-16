# 1. Service Account for Raw Data Ingestion (yfinance jobs)
resource "google_service_account" "ingestion_sa" {
  account_id   = "sa-finsight-ingestion-${var.environment}"
  display_name = "FinSight Raw Data Ingestion Service Account"
  description  = "Used by ingestion workers to write raw data payloads to GCS Bronze layer."
  project      = var.project_id
}

# 2. Service Account for Data Transformation (PySpark jobs)
resource "google_service_account" "processing_sa" {
  account_id   = "sa-finsight-processing-${var.environment}"
  display_name = "FinSight Data Processing Service Account"
  description  = "Used by PySpark compute workloads to transform Bronze data into Silver Parquet storage."
  project      = var.project_id
}
