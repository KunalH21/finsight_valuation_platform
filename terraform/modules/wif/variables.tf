variable "project_id" {
  type        = string
  description = "Target GCP Project ID."
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, stage, prod)."
}

variable "github_repository" {
  type        = string
  description = "GitHub repository in the format 'owner/repo' (e.g., 'kunalhirwani/FinSight')."
}

variable "terraform_sa_id" {
  type        = string
  description = "Fully qualified resource ID of the Terraform Service Account to impersonate."
}