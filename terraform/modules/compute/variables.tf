variable "project_id" { type = string }
variable "region" { type = string }
variable "environment" { type = string }
variable "ingestion_sa_email" { type = string }
variable "processing_sa_email" { type = string }
variable "bronze_bucket_name" { type = string }
variable "silver_bucket_name" { type = string }
variable "silver_dataset_id" { type = string }