resource "aws_glue_job" "wallets_aggregations_job" {
  name         = "Walets aggregations job"
  role_arn     = var.glue_role_arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${var.script_bucket}/scripts/wallets_aggregations.py"
    python_version  = "3"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "5.0"
  default_arguments = var.default_arguments
}

resource "aws_glue_job" "ethereum_transaction_cleaning" {
  name         = "Ethereum transaction cleaning"
  role_arn     = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.script_bucket}/scripts/ethereum_transaction_cleaning.py"
    python_version  = "3"
  }
  
  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "5.0"
  default_arguments = var.default_arguments
}

resource "aws_glue_job" "bitcoin_transaction_cleaning" {
  name         = "Bitcoin transaction cleaning"
  role_arn     = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.script_bucket}/scripts/bitcoin_transaction_cleaning.py"
    python_version  = "3"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "5.0"
  default_arguments = var.default_arguments
}
