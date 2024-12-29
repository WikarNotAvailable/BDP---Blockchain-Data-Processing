variable "bdp_cleaned_transactions_bucket" {
  type        = string
  description = "Cleaned transactions bucket name"
}

variable "bdp_wallets_aggregations_bucket" {
  type        = string
  description = "Wallets aggreagations bucket name"
}

variable "bdp_features_bucket" {
  type        = string
  description = "Features bucket name"
}

variable "glue_role_arn" {
  type        = string
  description = "ARN of IAM role for Glue"
}