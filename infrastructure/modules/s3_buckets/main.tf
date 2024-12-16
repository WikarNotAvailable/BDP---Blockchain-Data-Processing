resource "aws_s3_bucket" "aws_glue_assets" {
  bucket = "aws-glue-assets-982534349340-eu-north-1"
}

resource "aws_s3_bucket" "bdp_anomaly_classification" {
  bucket = "bdp-anomaly-classification"
}

resource "aws_s3_bucket" "bdp_cleaned_transactions" {
  bucket = "bdp-cleaned-transactions"
}

resource "aws_s3_bucket" "bdp_features" {
  bucket = "bdp-features"
}

resource "aws_s3_bucket" "bdp_metadata" {
  bucket = "bdp-metadata"
}

resource "aws_s3_bucket" "bdp_wallets_aggregations" {
  bucket = "bdp-wallets-aggregations"
}
