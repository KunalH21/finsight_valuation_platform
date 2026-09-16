# 1. Bronze Layer (Raw Landing Zone)
resource "google_storage_bucket" "bronze" {
  name                        = "bronze-${var.environment}-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      age = 30 # Days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90 # Days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

# 2. Silver Layer (Cleaned Parquet Storage)
resource "google_storage_bucket" "silver" {
  name                        = "silver-${var.environment}-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

# 3. Gold Layer (Curated Analytics Storage)
resource "google_storage_bucket" "gold" {
  name                        = "gold-${var.environment}-${var.project_id}"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }
}

# --- IAM Bindings (Least Privilege Security Model) ---

# Ingestion SA: Write-Only on Bronze
resource "google_storage_bucket_iam_member" "ingestion_bronze_writer" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${var.ingestion_sa_email}"
}

# Processing SA: Read-Only on Bronze
resource "google_storage_bucket_iam_member" "processing_bronze_reader" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.processing_sa_email}"
}

# Processing SA: Full Read/Write on Silver
resource "google_storage_bucket_iam_member" "processing_silver_user" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${var.processing_sa_email}"
}

# Processing SA: Full Read/Write on Gold
resource "google_storage_bucket_iam_member" "processing_gold_user" {
  bucket = google_storage_bucket.gold.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${var.processing_sa_email}"
}