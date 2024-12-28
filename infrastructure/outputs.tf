output "glue_role_arn" {
  description = "ARN of Glue role"
  value       = module.iam.glue_role_arn
}

output "orphan_files_removal_job_name" {
  description = "Orphan files removal job name"
  value       = module.glue_jobs.orphan_files_removal_job_name
}

#Glue data catalog is dynamically created during ETL process
/*output "bdp_database_name" {
  description = "Glue Catalog database name"
  value       = module.glue_catalog.database_name
}*/
