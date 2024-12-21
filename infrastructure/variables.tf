variable "aws_region" {
  type        = string
  default     = "eu-north-1"
  description = "AWS region"
}

variable "glue_role_name" {
  type        = string
  default     = "AWSGlueServiceRole"
  description = " IAM role for Glue"
}

variable "glue_script_bucket" {
  type        = string
  default     = "bdp-glue-scripts"
  description = "Bucket with Glue scripts"
}

variable "glue_jobs_default_arguments" {
  type = map(string)
  default = {
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-982534349340-eu-north-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://aws-glue-assets-982534349340-eu-north-1/temporary/"
    "--enable-auto-scaling"              = "true"
  }
}