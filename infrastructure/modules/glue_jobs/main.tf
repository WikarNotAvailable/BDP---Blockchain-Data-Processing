locals {
  transactions_cleaning_arguments = {
    "--END_DATE"       = "2024-12-12"
    "--START_DATE"     = "2024-12-10"
    "--NETWORK_PREFIX" = "all"
  }

  transactions_cleaning_final_arguments = merge(
    var.default_arguments,
    local.transactions_cleaning_arguments
  )
}

resource "aws_glue_job" "wallets_aggregations" {
  name     = "Wallets aggregations"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/aggregations.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
}

resource "aws_glue_job" "transactions_cleaning" {
  name     = "Transactions cleaning"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/etl_cloud.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = local.transactions_cleaning_final_arguments
}
