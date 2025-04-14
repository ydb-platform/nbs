resource "yandex_resourcemanager_folder" "init-folder" {
    name = "init"
    cloud_id = var.cloud_id
    description = "Folder for internal use (ie creating terraform bucket)"

}

resource "yandex_resourcemanager_folder" "ci-folder" {
    name = "gh-ci"
    cloud_id = var.cloud_id
    description = "Folder for external ci/cd related tasks"

}


resource "yandex_iam_service_account" "nbs-github-sa" {
  name = "nbs-github-sa"
  folder_id = yandex_resourcemanager_folder.ci-folder.id
  description = "Service account to create VMs during testing process"
}

resource "yandex_resourcemanager_folder_iam_member" "nbs-github-sa-roles" {
  count       = length(var.nbs-github-sa-roles-list)
  folder_id   = yandex_resourcemanager_folder.ci-folder.id
  role        = var.nbs-github-sa-roles-list[count.index]
  member      = "serviceAccount:${yandex_iam_service_account.nbs-github-sa.id}"
}

resource "yandex_compute_image" "ubuntu-2204-lts" {
  name = "ubuntu-2204-lts"
  description = "ubuntu-2204-lts"
  folder_id = yandex_resourcemanager_folder.ci-folder.id
  source_family = "ubuntu-2204-lts"
}

resource "yandex_compute_image" "ubuntu-2004-lts" {
  name = "ubuntu-2004-lts"
  description = "ubuntu-2004-lts"
  folder_id = yandex_resourcemanager_folder.ci-folder.id
  source_family = "ubuntu-2004-lts"
}

resource "yandex_vpc_network" "network" {
  name      = "default-network"
  folder_id = yandex_resourcemanager_folder.ci-folder.id
}

resource "yandex_vpc_subnet" "subnet" {
  name           = "default-subnet"
  folder_id      = yandex_resourcemanager_folder.ci-folder.id
  v4_cidr_blocks = ["10.10.10.0/24"]
  zone           = var.zone
  network_id     = yandex_vpc_network.network.id
  route_table_id = yandex_vpc_route_table.route_table.id
}

resource "yandex_vpc_gateway" "gateway" {
  name      = "default-gw"
  folder_id = yandex_resourcemanager_folder.ci-folder.id
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "route_table" {
  name       = "default-rt"
  network_id = yandex_vpc_network.network.id
  folder_id  = yandex_resourcemanager_folder.ci-folder.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.gateway.id
  }
}

# resource "yandex_vpc_address" "cache-server-external-ipv4" {
#   folder_id = yandex_resourcemanager_folder.ci-folder.id
#   name      = "cache-server-external-ipv4"

#   external_ipv4_address {
#     zone_id = var.zone
#   }
# }

resource "yandex_compute_filesystem" "cache-filesystem" {
  folder_id = yandex_resourcemanager_folder.ci-folder.id
  name = "ccache"
  size = 512
  type = "network-ssd"
  zone = "eu-north1-c"
}


resource "yandex_compute_instance" "cache-server" {
    name = "cache"
    folder_id = yandex_resourcemanager_folder.ci-folder.id
    platform_id = "standard-v2"

    resources {
      cores = 32
      memory = 64

    }

    boot_disk {
      initialize_params {
        image_id = yandex_compute_image.ubuntu-2204-lts.id
        size = 930
        type = "network-ssd-nonreplicated"
      }
    }

    network_interface {
      subnet_id = yandex_vpc_subnet.subnet.id
      ipv4 = true
      nat = true
    }

    filesystem {
      filesystem_id = yandex_compute_filesystem.cache-filesystem.id
      device_name = "ccache"
      mode = "READ_WRITE"
    }

    metadata = {
        ssh-keys = <<EOS
librarian:ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBKxbsMhTbLLLjU9bLi4CtiVhGO1oQtme/kjYRsOR+4/6nOWNmgVxiydsfeKq5Tg3fYxdo1jSq8M+xhVbO2ZFxe0= librarian
EOS
    }

}

// terraform sa
resource "yandex_iam_service_account" "github-terraform-sa" {
  folder_id   = yandex_resourcemanager_folder.init-folder.id
  name        = "nbs-github-terraform-sa"
  description = "SA for Terraform state"
}

resource "yandex_resourcemanager_folder_iam_member" "github-terraform-sa-editor" {
  folder_id  = yandex_resourcemanager_folder.init-folder.id
  role      = "storage.editor"
  member    = "serviceAccount:${yandex_iam_service_account.github-terraform-sa.id}"
}

// github actions sa

resource "yandex_iam_service_account" "github-actions-sa" {
  folder_id   = yandex_resourcemanager_folder.ci-folder.id
  name        = "github-actions-sa"
  description = "SA for Github Actions usage"
}

resource "yandex_resourcemanager_folder_iam_member" "github-actions-sa-editor" {
  folder_id  = yandex_resourcemanager_folder.ci-folder.id
  role      = "storage.editor"
  member    = "serviceAccount:${yandex_iam_service_account.github-actions-sa.id}"
}
