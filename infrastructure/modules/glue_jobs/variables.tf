variable "script_bucket" {
  type        = string
  description = "Bucket with Glue scripts and assets"
}

variable "glue_role_arn" {
  type        = string
  description = "ARN of IAM role for Glue"
}

variable "default_arguments" {
  type        = map(string)
  description = "Map of default glue jobs arguments"
}