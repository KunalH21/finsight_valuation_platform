resource "google_artifact_registry_repository" "finsight_repo" {
  location      = "us-central1" # Or your preferred GCP region
  repository_id = "finsight-docker-repo"
  description   = "Docker repository for FinSight application images"
  format        = "DOCKER"
}