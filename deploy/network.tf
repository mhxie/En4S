module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name = "en4s-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["us-west-1b"]
  private_subnets = ["10.0.1.0/24"]
  public_subnets  = ["10.0.101.0/24"]

  # requires internet gateway to update dependencies
  enable_nat_gateway = true
  # access instances without any bastion node
  enable_vpn_gateway = true

  # reuse_nat_ips       = true                    # <= Skip creation of EIPs for the NAT Gateways
  #external_nat_ip_ids = "${aws_eip.nat.*.id}"   # <= IPs specified here as input to the module

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }

  vpc_tags = {
    Name = "En4S VPC"
  }
}

# resource "aws_eip" "nat" {
#  count = 1
#  vpc = true
#}

resource "aws_network_interface" "enis" {
  count           = 24
  subnet_id       = module.vpc.private_subnets[0]
  security_groups = [module.sg.this_security_group_id]
}
