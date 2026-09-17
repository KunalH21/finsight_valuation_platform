module "vpc" {
  source       = "../../modules/vpc"
  project_id   = var.project_id
  region       = var.region
  network_name = "finsight-vpc-${var.environment}"
  subnet_cidr  = "10.10.0.0/24"
}

module "iam" {
  source      = "../../modules/iam"
  project_id  = var.project_id
  environment = var.environment
}

module "storage" {
  source              = "../../modules/storage"
  project_id          = var.project_id
  region              = var.region
  environment         = var.environment
  ingestion_sa_email  = module.iam.ingestion_sa_email
  processing_sa_email = module.iam.processing_sa_email
}

module "bigquery" {
  source              = "../../modules/bigquery"
  project_id          = var.project_id
  region              = var.region
  environment         = var.environment
  processing_sa_email = module.iam.processing_sa_email
}

module "wif" {
  source            = "../../modules/wif"
  project_id        = var.project_id
  environment       = var.environment
  github_repository = "KunalH21/finsight_valuation_platform"
  terraform_sa_id   = "projects/finsight-007/serviceAccounts/sa-terraform-dev@finsight-007.iam.gserviceaccount.com"
}

# Testing PR commenting pipeline