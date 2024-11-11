module "pub_sg" {
  source  = "terraform-aws-modules/security-group/aws//modules/ssh"
  version = "~> 3.0"

  name        = "bastion security group"
  description = "Security group for accessing nodes in public subnets"
  vpc_id      = module.vpc.vpc_id

  ingress_cidr_blocks = ["0.0.0.0/0"]
  ingress_rules       = ["ssh-tcp"]
  egress_rules        = ["all-all"]
}

module "sg" {
  source  = "terraform-aws-modules/security-group/aws"
  version = "~> 3.0"

  name        = "Internal security group"
  description = "Security group for internal access in private VPC"
  vpc_id      = module.vpc.vpc_id

  ingress_cidr_blocks = ["10.0.0.0/16"]
  ingress_rules       = ["all-all"]
  egress_rules        = ["all-all"]
}

module "invoke_lambda_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  name = "invoke_lambda_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["lambda:InvokeFunction"]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })

  tags = {
    tag-key = "Policy to invoke Lambda"
  }
}

module "access_s3_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  name = "access_s3_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = [
          "s3:CreateBucket",
          "s3:ListAllMyBuckets",
          "s3:ListBucket",
          "s3:HeadBucket",
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })

  tags = {
    tag-key = "Policy to access S3"
  }
}

module "access_ec2_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  name = "access_ec2_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = [
          "ec2:*",
          ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })

  tags = {
    tag-key = "Policy to access EC2"
  }
}
