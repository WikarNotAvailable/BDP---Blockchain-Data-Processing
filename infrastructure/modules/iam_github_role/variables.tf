variable "github_role_name" {
  type        = string
  description = "IAM role for Github integration name"
}

variable "glue_script_bucket" {
  type        = string
  description = "Bucket with Glue scripts"
}