variable "FOLDER_ID" {
  type = string
  default = "bjeuq5o166dq4ukv3eec"
}

variable "ZONE" {
  type = string
  default = "eu-north1-c"
}

variable "SUBNET_ID" {
  type = string
  default = "f8uh0ml4rhb45nde9p75"
}

variable "LSB_RELEASE" {
  type = string
  default = "jammy"
}

variable "CCACHE_VERSION" {
  type = string
  default = "4.8.1"
}

variable "OS_ARCH" {
  type = string
  default = "x86_64"
}

locals {
  current_date = formatdate("YYMMDD-hhmmss", timestamp())
}

source "yandex" "github-runner" {
  endpoint            = "api.nemax.nebius.cloud:443"
  folder_id           = "${var.FOLDER_ID}"
  source_image_family = "ubuntu-2204-lts"
  ssh_username        = "ubuntu"
  use_ipv4_nat        = "true"
  image_description   = "Github Runner Image"
  image_family        = "github-runner"
  image_name          = "gh-2204-${local.current_date}"
  subnet_id           = "${var.SUBNET_ID}"
  disk_type           = "network-ssd"
  zone                = "${var.ZONE}"
  disk_size_gb        = 8
}

build {
  sources = ["source.yandex.github-runner"]

  provisioner "shell" {
    inline = [
      "set -x",
      # Global Ubuntu things
      "wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | sudo apt-key add -",
      "echo \"deb https://apt.kitware.com/ubuntu/ ${var.LSB_RELEASE} main\" | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null",
      "wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -",
      "echo \"deb https://apt.llvm.org/${var.LSB_RELEASE}/ llvm-toolchain-${var.LSB_RELEASE}-14 main\" | sudo tee /etc/apt/sources.list.d/llvm.list >/dev/null",
      "sudo apt-get update",
      "echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections",
      "sudo apt-get install -y --no-install-recommends git wget gnupg lsb-release curl xz-utils tzdata cmake python3-dev python3-pip ninja-build antlr3 m4 libidn11-dev libaio1 libaio-dev make clang-14 lld-14 llvm-14 file distcc s3cmd qemu-kvm dpkg-dev",
      "sudo pip3 install conan==1.59 pytest==7.1.3  pyinstaller==5.13.2 pytest-timeout pytest-xdist==3.3.1 setproctitle==1.3.2  six pyyaml packaging  cryptography grpcio grpcio-tools PyHamcrest tornado xmltodict pyarrow boto3 moto[server] psutil pygithub==1.59.1",
      "curl -L https://github.com/ccache/ccache/releases/download/v${var.CCACHE_VERSION}/ccache-${var.CCACHE_VERSION}-linux-${var.OS_ARCH}.tar.xz | sudo tar -xJ -C /usr/local/bin/ --strip-components=1 --no-same-owner ccache-${var.CCACHE_VERSION}-linux-${var.OS_ARCH}/ccache",

      # Other packages
      "sudo apt-get install -y git jq tree tmux atop",

      # Clean
      "rm -rf .sudo_as_admin_successful",
      "sudo rm -rf /var/lib/apt/lists/*",
    ]
  }
}
