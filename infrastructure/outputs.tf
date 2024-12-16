output "glue_role_arn" {
  description = "ARN of Glue role"
  value       = module.iam.glue_role_arn
}

output "bdp_database_name" {
  description = "Glue Catalog database name"
  value       = module.glue_catalog.database_name
}
