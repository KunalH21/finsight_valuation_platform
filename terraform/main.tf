provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}

# --- S3 BRONZE BUCKET (Raw JSON Data) ---
resource "aws_s3_bucket" "bronze" {
  bucket = "finsight-bronze-layer"
  tags   = { Name = "Bronze Layer", Project = "finsight" }
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

# Lifecycle Policy: Archive to Glacier after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "archive_raw_json_to_glacier"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# --- S3 SILVER BUCKET (Cleaned Parquet Data) ---
resource "aws_s3_bucket" "silver" {
  bucket = "finsight-silver-layer"
  tags   = { Name = "Silver Layer", Project = "finsight" }
}

resource "aws_s3_bucket_versioning" "silver_versioning" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

# --- S3 GOLD BUCKET (Analytical Tables) ---
resource "aws_s3_bucket" "gold" {
  bucket = "finsight-gold-layer"
  tags   = { Name = "Gold Layer", Project = "finsight" }
}

resource "aws_s3_bucket_versioning" "gold_versioning" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

locals {
  bronze_prefix_clean = trim(var.bronze_ingestion_prefix, "/")
}

resource "aws_iam_user" "finsight_ingestion_user" {
  name = var.ingestion_user_name
  path = "/service-role/"
  tags = {
    Project = var.project_name
    Role    = "ingestion"
  }
}

resource "aws_iam_role" "finsight_ingestion_role" {
  name = "finsight-ingestion-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowIngestionUser"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_user.finsight_ingestion_user.arn }
        Action    = "sts:AssumeRole"
      },
      {
        Sid       = "SnowflakeS3AccessHandshake"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::768309077451:user/a20s1000-s" }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = { "sts:ExternalId" = "NYC44173_SFCRole=2_lav8HUgpJnMttbOD3foHhtJufGM=" }
        }
      }
    ]
  })

  tags = {
    Project = var.project_name
    Role    = "ingestion"
  }
}

resource "aws_iam_role_policy" "finsight_ingestion_role_policy" {
  name = "finsight-ingestion-role-putobject"
  role = aws_iam_role.finsight_ingestion_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AllowPutAndGetObjectBronzePrefix"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::finsight-bronze-layer/*"
      },
      {
        Sid      = "AllowListBucketBronzePrefix"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::finsight-bronze-layer"
      },
      {
        Sid      = "AllowSilverReadForSnowflake"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "arn:aws:s3:::finsight-silver-layer/*"
      },
      {
        Sid      = "AllowSilverListForSnowflake"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::finsight-silver-layer"
      }
    ]
  })
}
/*
resource "aws_iam_user_policy" "finsight_ingestion_user_assume_role" {
  name = "finsight-ingestion-user-assume-role"
  user = aws_iam_user.finsight_ingestion_user.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowAssumeFinsightIngestionRole"
        Effect = "Allow"
        Action = [
          "sts:AssumeRole"
        ]
        Resource = aws_iam_role.finsight_ingestion_role.arn
      }
    ]
  })
}
*/

resource "aws_iam_user_policy" "finsight_ingestion_user_direct_access" {
  name = "finsight-ingestion-user-direct-access"
  user = aws_iam_user.finsight_ingestion_user.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AllowS3Access"
        Effect   = "Allow"
        Action   = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::finsight-bronze-layer",
          "arn:aws:s3:::finsight-bronze-layer/*",
          "arn:aws:s3:::finsight-silver-layer",
          "arn:aws:s3:::finsight-silver-layer/*"
        ]
      }
    ]
  })
}
resource "aws_iam_access_key" "finsight_ingestion_user_key" {
  user = aws_iam_user.finsight_ingestion_user.name
}

