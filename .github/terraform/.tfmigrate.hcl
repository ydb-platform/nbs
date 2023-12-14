tfmigrate {
  migration_dir = "./migrations"
  history {
    storage "s3" {
      endpoint = "https://storage.ai.nebius.cloud"
      bucket   = "nbs-github-terraform"
      region   = "eu-north1"
      key      = "nbs-github-terraform/migrations/history.json"
      skip_credentials_validation = true
      skip_metadata_api_check     = true
    }
  }
}