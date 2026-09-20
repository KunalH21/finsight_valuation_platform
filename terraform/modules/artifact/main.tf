resource "google_artifact_registry_repository" "finsight_repo" {
  location      = var.region
  repository_id = "finsight-docker-repo-${var.environment}"
  description   = "Docker repository for FinSight application images (${var.environment})"
  format        = "DOCKER"
}