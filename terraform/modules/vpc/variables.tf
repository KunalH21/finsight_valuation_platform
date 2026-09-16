variable "project_id" {
  type        = string
  description = "The target GCP Project ID."
}

variable "network_name" {
  type        = string
  description = "The name of the VPC network."
  default     = "finsight-vpc-dev"
}

variable "region" {
  type        = string
  description = "Primary GCP region for subnets."
  default     = "us-central1"
}

variable "subnet_cidr" {
  type        = string
  description = "Primary IP range for the main workload subnet."
  default     = "10.10.0.0/24"
}