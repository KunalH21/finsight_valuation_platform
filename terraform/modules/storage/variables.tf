variable "project_id" {
  type        = string
  description = "Target GCP Project ID."
}

variable "region" {
  type        = string
  description = "GCP region for storage bucket deployment."
  default     = "us-central1"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, stage, prod)."
}

variable "ingestion_sa_email" {
  type        = string
  description = "Email of the Ingestion Service Account."
}

variable "processing_sa_email" {
  type        = string
  description = "Email of the Processing Service Account."
}