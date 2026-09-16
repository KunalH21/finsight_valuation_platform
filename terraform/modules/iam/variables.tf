variable "project_id" {
  type        = string
  description = "Target GCP Project ID."
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, stage, prod)."
}