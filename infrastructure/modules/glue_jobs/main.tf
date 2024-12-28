locals {
  transactions_cleaning_arguments = {
    "--END_DATE"       = "2024-11-30"
    "--START_DATE"     = "2024-11-1"
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
    script_location = "s3://${var.glue_script_bucket}/wallets_aggregations.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
  timeout           = 120
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
  timeout           = 120
}

resource "aws_glue_job" "feature_scaling" {
  name     = "Feature scaling"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/preprocessing_cloud.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
  timeout           = 120
}

resource "aws_glue_job" "removing_orphan_files" {
  name     = "Orphan files removal"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/orphan_files_removal.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
  timeout           = 120
}

resource "aws_glue_trigger" "weekly_trigger" {
  name     = "Every Monday at 6:00 AM"
  type     = "SCHEDULED"
  schedule = "cron(0 6 ? * 2 *)" # Every Monday at 6:00 AM

  actions {
    job_name = aws_glue_job.removing_orphan_files.name
  }
}