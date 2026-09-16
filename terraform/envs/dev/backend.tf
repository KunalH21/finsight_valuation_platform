terraform {
  backend "gcs" {
    bucket = "finsight-tfstate-dev-finsight-007" # Replace with your exact state bucket name
    prefix = "dev/state"
  }
}