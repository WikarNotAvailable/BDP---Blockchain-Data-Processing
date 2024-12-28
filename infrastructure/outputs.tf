output "glue_role_arn" {
  description = "ARN of Glue role"
  value       = module.iam.glue_role_arn
}

output "orphan_files_removal_job_name" {
  description = "Orphan files removal job name"
  value       = module.glue_jobs.orphan_files_removal_job_name
}