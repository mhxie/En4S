provider "aws" {
  region = "us-west-1"
  profile = "default"
}

resource "aws_placement_group" "cluster_group" {
  name     = "shared-cluster"
  strategy = "cluster"
}

resource "aws_instance" "bastion_node" {
  ami           = var.default_ami
  instance_type = "t2.micro"
  key_name      = "finalkey"
  subnet_id     = module.vpc.public_subnets[0]

  vpc_security_group_ids = [module.pub_sg.this_security_group_id]

  tags = {
    Name = "Bastion Node"
  }
}

resource "aws_eip" "bastion_eip" {
  instance = aws_instance.bastion_node.id
  vpc      = true
}

locals {
  controller_node_enis_idx = 0  # 2 ENIs reserved
  en4s_storage_enis_idx = 2
}

resource "aws_instance" "en4s_controller" {
  count = var.enable_en4s ? var.en4s_controller_count : 0

  ami             = var.en4s_controllerv1_ami
  instance_type   = "m5.large"
  key_name        = "finalkey"
  subnet_id       = module.vpc.private_subnets[0]
  placement_group = aws_placement_group.cluster_group.id

  vpc_security_group_ids = [module.sg.this_security_group_id]

  tags = {
    Name = "En4S Controller - ${count.index}"
  }
}

resource "aws_instance" "en4s_storage" {
  count = var.enable_en4s ? var.en4s_storage_count : 0

  ami           = var.en4s_storage_ami
  instance_type = "i3.xlarge"
  key_name      = "finalkey"
  placement_group = aws_placement_group.cluster_group.id

  network_interface {
    device_index          = 0
    network_interface_id  = aws_network_interface.enis[local.en4s_storage_enis_idx + 2 * count.index].id
    delete_on_termination = false
  }
  network_interface {
    device_index          = 1
    network_interface_id  = aws_network_interface.enis[local.en4s_storage_enis_idx + 2 * count.index + 1].id
    delete_on_termination = false
  }

  tags = {
    Name = "En4S Storage Node - ${count.index}"
  }
}
