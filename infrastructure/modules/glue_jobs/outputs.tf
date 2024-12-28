output "orphan_files_removal_job_name" {
  value       = aws_glue_job.removing_orphan_files.name
  description = "Name of the removing_orphan_files Glue job"
}

output "transactions_cleaning_job_name" {
  value       = aws_glue_job.transactions_cleaning.name
  description = "Name of the transactions_cleaning Glue job"
}

output "wallets_aggregations_job_name" {
  value       = aws_glue_job.wallets_aggregations.name
  description = "Name of the wallets_aggregations Glue job"
}

output "feature_scaling_job_name" {
  value       = aws_glue_job.feature_scaling.name
  description = "Name of the feature_scaling Glue job"
}