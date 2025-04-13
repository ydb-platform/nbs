terraform {

  backend "s3" {
    endpoint = "https://storage.ai.nebius.cloud"

    bucket   = "nbs-github-terraform"
    region   = "eu-north1"
    key      = "nbs-github-terraform/nbs-github-terraform.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
  }
}