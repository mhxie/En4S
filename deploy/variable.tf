variable "default_ami" {
  type    = string
  default = "ami-06604eb73be76c003" # Amazon Linux 2023, w/ Python3.9
}

variable "en4s_controllerv1_ami" {
  type    = string
  default = "" # build your own AMI
}

variable "en4s_storage_ami" {
  type    = string
  # Amazon Linux 2 AMI (HVM) - Kernel 4.14, SSD Volume Type w/ En4S server build
  default = ""  # build your own AMI
}

# En4S Controller
variable "en4s_controller_count" {
  type    = number
  default = 1
}

# En4S Storage Server
variable "en4s_storage_count" {
  type    = number
  default = 2
}

variable "enable_en4s" {
  type    = bool
  default = true
}