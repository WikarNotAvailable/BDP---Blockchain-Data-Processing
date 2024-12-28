resource "aws_glue_workflow" "etl" {
  name        = var.etl_workflow_name
  description = "Workflow to perform etl jobs on demand."
}

resource "aws_glue_trigger" "etl_on_demand_trigger" {
  name          = "ETL on demand trigger"
  type          = "ON_DEMAND"
  workflow_name = var.etl_workflow_name

  actions {
    job_name = var.transactions_cleaning_job_name
  }
}

resource "aws_glue_trigger" "aggregations_trigger" {
  name          = "Aggregations trigger"
  type          = "CONDITIONAL"
  workflow_name = var.etl_workflow_name

  predicate {
    conditions {
      job_name = var.transactions_cleaning_job_name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = var.wallets_aggregations_job_name
  }
}

resource "aws_glue_trigger" "feature_scaling_trigger" {
  name          = "Feature scaling trigger"
  type          = "CONDITIONAL"
  workflow_name = var.etl_workflow_name

  predicate {
    conditions {
      job_name = var.wallets_aggregations_job_name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = var.feature_scaling_job_name
  }
}