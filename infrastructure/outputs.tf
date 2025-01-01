output "glue_role_arn" {
  description = "ARN of Glue role"
  value       = module.iam.glue_role_arn
}