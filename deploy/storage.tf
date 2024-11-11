module "s3_es" {
  source = "terraform-aws-modules/s3-bucket/aws"

  bucket = "es-store-uw1"
  #acl    = "private"

  # Allow deletion of non-empty bucket
  force_destroy = true

  attach_elb_log_delivery_policy = true
}

module "s3_bucket_for_lambdas" {
  source = "terraform-aws-modules/s3-bucket/aws"

  bucket = "lambda-store-uw1"
  #acl    = "private"

  # Allow deletion of non-empty bucket
  force_destroy = true

  attach_elb_log_delivery_policy = true
}

module "s3_bucket_for_layers" {
  source = "terraform-aws-modules/s3-bucket/aws"

  bucket = "analytic-lib-layer-uw1"
  #acl    = "private"

  # Allow deletion of non-empty bucket
  force_destroy = true

  attach_elb_log_delivery_policy = true
}