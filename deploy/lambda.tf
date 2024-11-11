module "terasort" {
  source             = "terraform-aws-modules/lambda/aws"
  function_name      = "esbench_terasort"
  description        = "Terasort Application"
  handler            = "index.lambda_handler"
  runtime            = "python3.10"
  memory_size        = 3009 # MB
  timeout            = 120 # seconds
  policies           = [module.invoke_lambda_policy.arn, module.access_s3_policy.arn]
  attach_policies    = true
  number_of_policies = 2

  source_path = [{
      path = "../app/terasort/index.py",
    }, {
      path = "../app/terasort/mapper.py",
    }, {
      path = "../app/terasort/reducer.py",
    }, {
      path = "../app/terasort/access.py",
    }, {
      path = "../lib/",
    }, {
    path = "../app/asynces.py",
    }
  ]

  layers = [
    module.numpy_layer_s3.lambda_layer_arn,
    module.thrift_layer_s3.lambda_layer_arn,
  ]

  vpc_subnet_ids         = [module.vpc.private_subnets[0]]
  vpc_security_group_ids = [module.sg.this_security_group_id]
  attach_network_policy  = true

  environment_variables = {
    app_name = "terasort",
    next_url = "esbench_terasort",
    self_url = "esbench_terasort",
    s3     = module.s3_es.s3_bucket_id
    en4s   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
    mock   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
  }

  tags = {
    Name = "Terasort Application"
  }
}

module "etl" {
  source             = "terraform-aws-modules/lambda/aws"
  function_name      = "esbench_etl"
  description        = "ETL Application"
  handler            = "index.lambda_handler"
  runtime            = "python3.10"
  memory_size        = 3009 # MB
  timeout            = 60 # seconds
  policies           = [module.invoke_lambda_policy.arn, module.access_s3_policy.arn]
  attach_policies    = true
  number_of_policies = 2

  source_path = [{
      path = "../app/ETL/index.py",
    }, {
      path = "../app/ETL/helper.py",
    }, {
      path = "../lib/",
    }, {
    path = "../app/asynces.py",
    }
  ]

  layers = [
    module.numpy_layer_s3.lambda_layer_arn,
    module.thrift_layer_s3.lambda_layer_arn,
  ]

  vpc_subnet_ids         = [module.vpc.private_subnets[0]]
  vpc_security_group_ids = [module.sg.this_security_group_id]
  attach_network_policy  = true

  environment_variables = {
    app_name = "etl",
    next_url = "esbench_etl",
    self_url = "esbench_etl",
    s3     = module.s3_es.s3_bucket_id
    en4s   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
    mock   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
  }

  tags = {
    Name = "ETL Pipeline"
  }
}

module "analytics_process" {
  source        = "terraform-aws-modules/lambda/aws"
  function_name = "esbench_analytics_ps"
  description   = "Video Analytics Application"
  handler       = "index.lambda_handler"
  runtime       = "python3.10"
  memory_size   = 3009 # MB
  timeout       = 60 # seconds
  policies      = [module.invoke_lambda_policy.arn, module.access_s3_policy.arn]
  attach_policies    = true
  number_of_policies = 2

  create_package = false
  local_existing_package = "../function_packages/analytics_function.zip"

  layers = [
    module.cv_layer_s3.lambda_layer_arn,
    module.thrift_layer_s3.lambda_layer_arn,
  ]

  vpc_subnet_ids         = [module.vpc.private_subnets[0]]
  vpc_security_group_ids = [module.sg.this_security_group_id]
  attach_network_policy  = true


  environment_variables = {
    app_name = "analytics",
    next_url = "esbench_analytics_tc",
    self_url = "esbench_analytics_ps",
    s3     = "sls-video-src"
    en4s   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
    mock   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
  }

  tags = {
    Name = "Analytics - process stage"
  }
}

module "analytics_transcode" {
  source        = "terraform-aws-modules/lambda/aws"
  function_name = "esbench_analytics_tc"
  description   = "Video Analytics Application"
  handler       = "index.lambda_handler"
  runtime       = "python3.10"
  memory_size   = 3009 # MB
  timeout       = 180 # seconds
  policies      = [module.invoke_lambda_policy.arn, module.access_s3_policy.arn]
  attach_policies    = true
  number_of_policies = 2

  create_package = false
  local_existing_package = "../function_packages/analytics_function.zip"

  layers = [
    module.iio_layer_s3.lambda_layer_arn,
    module.thrift_layer_s3.lambda_layer_arn,
  ]

  vpc_subnet_ids         = [module.vpc.private_subnets[0]]
  vpc_security_group_ids = [module.sg.this_security_group_id]
  attach_network_policy  = true

  environment_variables = {
    app_name = "analytics",
    next_url = "esbench_analytics_ps",
    self_url = "esbench_analytics_tc",
    s3     = "sls-video-src"
    en4s   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
    mock   = var.enable_en4s ? aws_instance.en4s_controller[0].private_ip : ""
  }

  tags = {
    Name = "Analytics - transcode stage"
  }
}