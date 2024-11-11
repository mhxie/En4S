module "cv_layer_s3" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "cv-lambda-layer-s3"
  description         = "OpenCV lambda layer (deployed from S3)"
  compatible_runtimes = ["python3.10"]

  source_path = "../layers/cv2-python310"

  store_on_s3 = true
  s3_bucket   = module.s3_bucket_for_layers.s3_bucket_id
}

module "iio_layer_s3" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "iio-lambda-layer-s3"
  description         = "ImageIOxffmpeg lambda layer (deployed from S3)"
  compatible_runtimes = ["python3.10"]

  source_path = "../layers/imageio-python310"

  store_on_s3 = true
  s3_bucket   = module.s3_bucket_for_layers.s3_bucket_id
}

module "thrift_layer_s3" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "thrift-lambda-layer-s3"
  description         = "Thrift lambda layer (deployed from S3)"
  compatible_runtimes = ["python3.10"]

  source_path = "../layers/thrift-python310"

  store_on_s3 = true
  s3_bucket   = module.s3_bucket_for_layers.s3_bucket_id
}

module "numpy_layer_s3" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "numpy-lambda-layer-s3"
  description         = "NumPy lambda layer (deployed from S3)"
  compatible_runtimes = ["python3.10"]

  source_path = "../layers/numpy-python310"

  store_on_s3 = true
  s3_bucket   = module.s3_bucket_for_layers.s3_bucket_id
}