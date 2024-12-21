module "s3_buckets" {
  source = "./modules/s3_buckets"
  script_bucket = var.script_bucket
}

module "iam" {
  source         = "./modules/iam"
  glue_role_name = var.glue_role_name
}

#Glue data catalog is dynamically created during ETL process
/*module "glue_catalog" {
  source = "./modules/glue_catalog"
}*/

module "glue_jobs" {
  source            = "./modules/glue_jobs"
  script_bucket     = var.script_bucket
  glue_role_arn     = module.iam.glue_role_arn
  default_arguments = var.glue_jobs_default_arguments
}
