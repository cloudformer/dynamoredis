# State lives next to the code (gitignored). Flip the comments to keep it in S3 instead —
# bucket setup: terraform-aws-infra README, "Bootstrap" — then run `terraform init -migrate-state`.
terraform {
  backend "local" {
    path = "state/terraform.tfstate"
  }
  # backend "s3" {
  #   bucket       = "<your-tfstate-bucket>"
  #   key          = "dynamoredis/terraform.tfstate"
  #   region       = "<your-region>"
  #   use_lockfile = true # S3 native state lock (Terraform >= 1.10)
  # }
}
