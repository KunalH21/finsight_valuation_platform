variable "aws_region" {
  description = "AWS region for deployment"
  default     = "us-east-1"
}

variable "bronze_ingestion_prefix" {
  description = "S3 key prefix within the bronze bucket that the ingestion role can write to"
  type        = string
  default     = "ingestion/raw/"
}

variable "ingestion_user_name" {
  description = "IAM user name for the least-privilege ingestion demonstration"
  type        = string
  default     = "finsight-ingestion-user"
}

variable "bucket_prefix" {
  description = "Unique prefix for your S3 buckets (e.g., your name or account ID)"
  type        = string
  default     = "1234"
}

variable "project_name" {
  description = "Project name tag"
  default     = "finsight-valuation-platform"
}

variable "company_universe" {
  description = "List of 50 S&P 500 companies across 5 sectors for valuation analysis"
  type        = list(string)
  default = [
    # Technology
    "AAPL", "MSFT", "GOOGL", "META", "NVDA", "ORCL", "CRM", "ADBE", "INTC", "AMD",
    # Financials
    "JPM", "BAC", "GS", "MS", "WFC", "C", "BLK", "AXP", "USB", "PNC",
    # Healthcare
    "JNJ", "PFE", "UNH", "ABT", "MRK", "TMO", "DHR", "AMGN", "BMY", "GILD",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG", "PXD", "PSX", "MPC", "VLO", "HAL",
    # Consumer
    "AMZN", "WMT", "PG", "KO", "PEP", "MCD", "NKE", "COST", "TGT", "HD"
  ]
}
