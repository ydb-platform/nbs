variable "FOLDER_ID" {
  type    = string
  default = "bjeuq5o166dq4ukv3eec"
}

variable "ZONE" {
  type    = string
  default = "eu-north1-c"
}

variable "SUBNET_ID" {
  type    = string
  default = "f8uh0ml4rhb45nde9p75"
}

variable "LSB_RELEASE" {
  type    = string
  default = "jammy"
}

variable "CCACHE_VERSION" {
  type    = string
  default = "4.8.1"
}

variable "OS_ARCH" {
  type    = string
  default = "x86_64"
}

variable "RUNNER_VERSION" {
  type    = string
  default = "2.316.1"
}

variable "YA_ARCHIVE_URL" {
  type    = string
  default = ""
}

variable "USER_TO_CREATE" {
  type    = string
  default = "github"
}

variable "PASSWORD_HASH" {
  type    = string
  default = ""
}

variable "GITHUB_TOKEN" {
  type    = string
  default = ""
}

variable "ORG" {
  type    = string
  default = "ydb-platform"
}

variable "TEAM" {
  type    = string
  default = "nbs"
}

variable "AWS_ACCESS_KEY_ID" {
  type    = string
  default = ""
}

variable "AWS_SECRET_ACCESS_KEY" {
  type    = string
  default = ""
}

variable "NIGHTLY_RUN_ID" {
  type    = string
  default = "0"
}

variable "BUILD_RUN_ID" {
  type    = string
  default = "0"
}


variable "REPOSITORY" {
  type    = string
  default = ""
}

variable "IMAGE_FAMILY_NAME" {
  type    = string
  default = ""
}

variable "IMAGE_PREFIX" {
  type    = string
  default = ""
}


locals {
  current_date = formatdate("YYMMDD-hhmmss", timestamp())
}

source "yandex" "github-runner" {
  endpoint            = "api.nemax.nebius.cloud:443"
  folder_id           = "${var.FOLDER_ID}"
  zone                = "${var.ZONE}"
  subnet_id           = "${var.SUBNET_ID}"
  use_ipv4_nat        = "true"
  state_timeout       = "60m"
  ssh_username        = "ubuntu"
  source_image_family = "ubuntu-2204-lts"
  image_family        = "github-runner-${var.IMAGE_FAMILY_NAME}"
  image_name          = (
                          var.YA_ARCHIVE_URL != ""
                            ? "${var.IMAGE_PREFIX}-${var.IMAGE_FAMILY_NAME}-${var.NIGHTLY_RUN_ID}"
                            : "${var.IMAGE_PREFIX}-${var.IMAGE_FAMILY_NAME}-build-${var.BUILD_RUN_ID}"
                        )
  image_description   = (
                          var.YA_ARCHIVE_URL != ""
                            ? "repo:${var.REPOSITORY} run_id:${var.BUILD_RUN_ID} created_at:${local.current_date} nightly_run_id:${var.NIGHTLY_RUN_ID} "
                            : "repo:${var.REPOSITORY} run_id:${var.BUILD_RUN_ID} created_at:${local.current_date}"
                        )
  image_pooled        = "true"

  // labels don't work for some reason
  labels = {
    "nightly_run" = "${var.NIGHTLY_RUN_ID}"
    "repository"  = "${var.REPOSITORY}"
  }

  disk_type       = "network-ssd"
  disk_size_gb    = var.YA_ARCHIVE_URL != "" ? 6*93 : 12
  instance_cores  = 60
  instance_mem_gb = 240
}

build {
  sources = ["source.yandex.github-runner"]

  provisioner "shell" {
    environment_vars = [
      "CCACHE_VERSION=${var.CCACHE_VERSION}",
      "LSB_RELEASE=${var.LSB_RELEASE}",
      "RUNNER_VERSION=${var.RUNNER_VERSION}",
      "OS_ARCH=${var.OS_ARCH}",
      "USER_TO_CREATE=${var.USER_TO_CREATE}",
      "PASSWORD_HASH=${var.PASSWORD_HASH}",
      "YA_ARCHIVE_URL=${var.YA_ARCHIVE_URL}",
      "GITHUB_TOKEN=${var.GITHUB_TOKEN}",
      "ORG=${var.ORG}",
      "TEAM=${var.TEAM}",
      "AWS_ACCESS_KEY_ID=${var.AWS_ACCESS_KEY_ID}",
      "AWS_SECRET_ACCESS_KEY=${var.AWS_SECRET_ACCESS_KEY}",
    ]
    script = "./.github/scripts/github-runner.sh"
  }

  post-processor "manifest" {
    output = "manifest.json"
  }
}
