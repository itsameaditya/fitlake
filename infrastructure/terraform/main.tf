terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Buckets (replace MinIO for production) ─────────────────────────────────

resource "aws_s3_bucket" "fitlake_warehouse" {
  bucket = "${var.project_name}-warehouse-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "fitlake_raw" {
  bucket = "${var.project_name}-raw-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.fitlake_warehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "warehouse" {
  bucket = aws_s3_bucket.fitlake_warehouse.id
  rule {
    id     = "archive-old-snapshots"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

# ── EMR Serverless (run Spark jobs without managing clusters) ─────────────────

resource "aws_emrserverless_application" "fitlake_spark" {
  name          = "${var.project_name}-spark-${var.environment}"
  release_label = "emr-7.0.0"
  type          = "SPARK"

  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
      }
    }
  }

  maximum_capacity {
    cpu    = "40 vCPU"
    memory = "160 GB"
  }

  tags = local.common_tags
}

# ── IAM Role for EMR ──────────────────────────────────────────────────────────

resource "aws_iam_role" "emr_execution" {
  name = "${var.project_name}-emr-execution-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy" "emr_s3_access" {
  name = "fitlake-s3-access"
  role = aws_iam_role.emr_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.fitlake_warehouse.arn,
          "${aws_s3_bucket.fitlake_warehouse.arn}/*",
          aws_s3_bucket.fitlake_raw.arn,
          "${aws_s3_bucket.fitlake_raw.arn}/*",
        ]
      }
    ]
  })
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "warehouse_bucket" {
  value = aws_s3_bucket.fitlake_warehouse.bucket
}

output "raw_bucket" {
  value = aws_s3_bucket.fitlake_raw.bucket
}

output "emr_application_id" {
  value = aws_emrserverless_application.fitlake_spark.id
}

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
