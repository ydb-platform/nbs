packer {
  required_plugins {
    nebius = {
      version = ">= 0.0.4"
      source  = "github.com/nebius/nebius"
    }
  }
}

variable "RUNNER_VERSION" {
  type    = string
  default = "2.333.0"
}

variable "USER_TO_CREATE" {
  type    = string
  default = "github"
}

variable "ORG" {
  type    = string
  default = "ydb-platform"
}

variable "TEAM" {
  type    = string
  default = "nbs"
}

variable "TMPFS_SIZE" {
  type        = string
  default     = "70866960384"  # 66GiB
  description = "tmpfs size (0 to disable). Can be a percentage of available RAM"
}

variable "TMPFS_MOUNT_POINT" {
  type        = string
  default     = "/mnt/ya_make_tmpfs"
  description = "Mount point path for the tmpfs filesystem"
}

variable "PASSWORD_HASH" {
  type    = string
  # we are using secret.VM_USER_PASSWD from GA
  default = env("PASSWORD_HASH")
}

variable "GITHUB_TOKEN" {
  type    = string
  default = env("GITHUB_TOKEN")
}

variable "NEBIUS_IAM_TOKEN" {
  type    = string
  default = env("NEBIUS_IAM_TOKEN")
}

variable "PARENT_ID" {
  type    = string
  # by default we are using vars.POOLED_HEAVY_PARENT_ID
  default = env("PARENT_ID")
}

variable "SUBNET_ID" {
  type    = string
  # by default we are using vars.POOLED_HEAVY_SUBNET_ID
  default = env("SUBNET_ID")
}


locals {
  image_family  = "ubuntu22.04-nbs-github-ci"
  image_version = "${formatdate("YYYYMMDDHHMM", timestamp())}-${timestamp()}"
  tmp_directory = "/tmp/packer"
}

source "nebius-image" "ubuntu2204-nbs-github-ci" {
  communicator = "ssh"
  ssh_username = "ubuntu"
  token      = var.NEBIUS_IAM_TOKEN
  disk {
    size_gibibytes = 16
  }
  base_image {
    family = "ubuntu22.04-driverless"
  }
  network {
    subnet_id = var.SUBNET_ID
    associate_public_ip_address = true
  }
  instance {
    platform = "cpu-d3"
    preset   = "16vcpu-64gb"
  }
  image {
    name                        = "ubuntu22.04-nbs-github-ci-${local.image_version}"
    version                     = local.image_version
    image_family                = "ubuntu22.04-nbs-github-ci"
    cpu_architecture            = "amd64"
    image_family_human_readable = "Ubuntu 22.04 NBS GitHub CI Image"
  }
  parent_id = var.PARENT_ID
}


build {
  sources = ["source.nebius-image.ubuntu2204-nbs-github-ci"]

  provisioner "shell" {
    inline = [
      "mkdir -p ${local.tmp_directory}/etc/"
    ]
  }

  provisioner "file" {
    source      = "${path.cwd}/scripts/requirements.txt"
    destination = "${local.tmp_directory}/requirements.txt"
  }

  provisioner "shell" {
    environment_vars = [
      "RUNNER_VERSION=${var.RUNNER_VERSION}",
      "USER_TO_CREATE=${var.USER_TO_CREATE}",
      "PASSWORD_HASH=${var.PASSWORD_HASH}",
      "GITHUB_TOKEN=${var.GITHUB_TOKEN}",
      "ORG=${var.ORG}",
      "TEAM=${var.TEAM}",
    ]
    execute_command = "sudo {{ .Vars }} bash '{{ .Path }}'"
    scripts         = ["scripts/github-runner.sh"]
  }

  # nvme-loop

  provisioner "file" {
    source      = "${path.cwd}/nvme-loop"
    destination = "${local.tmp_directory}/etc/nvme-loop"
  }

  provisioner "shell" {
    environment_vars = [
      "USER_NAME=${var.USER_TO_CREATE}"
    ]
    execute_command = "sudo {{ .Vars }} bash '{{ .Path }}'"
    inline          = ["${local.tmp_directory}/etc/nvme-loop/install.sh"]
  }

  # tmpfs for tests

  provisioner "shell" {
    environment_vars = [
      "TMPFS_SIZE=${var.TMPFS_SIZE}",
      "TMPFS_MOUNT_POINT=${var.TMPFS_MOUNT_POINT}",
    ]
    execute_command = "sudo {{ .Vars }} bash '{{ .Path }}'"
    scripts         = ["scripts/tmpfs.sh"]
  }

  provisioner "shell-local" {
    inline = ["mkdir -p reports"]
  }

  post-processor "manifest" {
    output = "reports/manifest.json"
    strip_path = true
    custom_data = {
      family = local.image_family
      os_name = "Ubuntu"
      os_version = "22.04"
      linux_kernel = "6.2.0-39"
      cpu_architecture = "amd64"
    }
  }
}
