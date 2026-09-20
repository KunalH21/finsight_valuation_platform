resource "google_cloud_run_v2_job" "ingestion_job" {
  name     = "finsight-ingestion-${var.environment}"
  location = var.region

  template {
    template {
      service_account = var.ingestion_sa_email

      containers {
        image   = "${var.region}-docker.pkg.dev/${var.project_id}/finsight-docker-repo/finsight-app:latest"
        command = ["python", "-m", "ingestion.fetch_data"]

        env {
          name  = "BRONZE_BUCKET"
          value = var.bronze_bucket_name
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job" "processing_job" {
  name     = "finsight-processing-${var.environment}"
  location = var.region

  template {
    template {
      service_account = var.processing_sa_email

      containers {
        image   = "${var.region}-docker.pkg.dev/${var.project_id}/finsight-docker-repo/finsight-app:latest"
        command = ["python", "-m", "spark.transform_data"]

        env {
          name  = "BRONZE_BUCKET"
          value = var.bronze_bucket_name
        }
        env {
          name  = "SILVER_BUCKET"
          value = var.silver_bucket_name
        }
        env {
          name  = "SILVER_DATASET"
          value = var.silver_dataset_id
        }
      }
    }
  }
}