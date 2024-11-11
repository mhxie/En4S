# Deployment

## Install terraform, aws-cli and configure

```
pip3 install awscli --upgrade --user
aws configure

curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt-get update && sudo apt-get install terraform
```

## Prepare your key pair in AWS

```
aws --region us-west-2 ec2 \
    create-key-pair \
    --key-name "example"
```
Then modify the corresponding field in the terraform resource `aws_instance`


## Create the test environment on the AWS
```
terraform init
terraform apply # -auto-approve
terraform destroy # -auto-approve
# clean up
rm -rf .terraform* terraform.tfstate* tfplan crash.log builds
```

## High-level introduction

```
❯ tree
.
├── main.tf                  # Primary entrypoint of this benchmark deploy
├── variable.tf              # Shared variables, only VPC is enabled by default
├── version.tf               # Version file of this module
├── output.tf                # Output resource id for external accesses
├── network.tf               # Network-related resources (replaces vpc.tf)
├── storage.tf               # Storage resources includes En4S, Pocket, and S3
├── lambda.tf                # Frontend, microbench, and application lambdas
├── layer.tf                 # Lambda layers configuration
├── security.tf              # Manage resource permissions (replaces sg.tf)
├── env.sh                   # Environment variables for bash
├── fish_env.sh              # Environment variables for fish shell
└── README.md                # Project documentation
```

## Set environment variables

```
source env.sh      # in bash
source fish_env.sh # in fish
```
