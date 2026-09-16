# 1. Silver Staging Dataset
resource "google_bigquery_dataset" "silver" {
  dataset_id                 = lower("finsight_silver_${var.environment}")
  friendly_name              = "Finsight Silver Staging (${var.environment})"
  description                = "Cleansed and structured staging tables derived from GCS Silver layer."
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = var.environment == "dev" ? true : false
}

# 2. Gold Analytics Dataset
resource "google_bigquery_dataset" "gold" {
  dataset_id                 = lower("finsight_gold_${var.environment}")
  friendly_name              = "Finsight Gold Datamart (${var.environment})"
  description                = "Curated data models and analytical metrics for business reporting."
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = var.environment == "dev" ? true : false
}

# --- IAM Bindings for BigQuery ---

# Processing SA: Data Editor on Silver Dataset
resource "google_bigquery_dataset_access" "processing_silver_editor" {
  dataset_id    = google_bigquery_dataset.silver.dataset_id
  project       = var.project_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = var.processing_sa_email
}

# Processing SA: Data Editor on Gold Dataset
resource "google_bigquery_dataset_access" "processing_gold_editor" {
  dataset_id    = google_bigquery_dataset.gold.dataset_id
  project       = var.project_id
  role          = "roles/bigquery.dataEditor"
  user_by_email = var.processing_sa_email
}

# Processing SA: BigQuery Job User (Project-Level to run queries/slots)
resource "google_project_iam_member" "processing_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.processing_sa_email}"
}