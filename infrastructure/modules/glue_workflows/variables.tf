variable "etl_workflow_name" {
  type        = string
  description = "Name of the etl Glue workflow."
}

variable "transactions_cleaning_job_name" {
  type        = string
  description = "Name of the transactions cleaning removal Glue job."
}

variable "wallets_aggregations_job_name" {
  type        = string
  description = "Name of the wallets aggregations Glue job."
}

variable "feature_scaling_job_name" {
  type        = string
  description = "Name of the feature scaling Glue job."
}
