terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.50.0"
    }
  }
}


provider "google" {
  project = var.project_id
  region  = var.region

  default_labels = {
    environment = var.environment
    managed_by  = "terraform"
    project     = "finsight"
  }
}