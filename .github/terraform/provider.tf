provider "yandex" {
  cloud_id    = var.cloud_id
  zone        = var.zone
  token       = var.iam_token
  max_retries = 10
  endpoint    = "api.nemax.nebius.cloud:443"
}
