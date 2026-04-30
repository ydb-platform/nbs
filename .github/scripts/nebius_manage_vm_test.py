import asyncio
import argparse
from types import SimpleNamespace

import pytest

from . import nebius_manage_vm as m


def make_create_args(**overrides):
    args = {
        "parent_id": "parent-id",
        "name": "vm-name",
        "disk_size": 930,
        "disk_type": "network-ssd-nonreplicated",
        "image_id": "image-id",
        "labels": {"test": "librarian"},
        "runner_flavor": "large",
        "platform_id": "cpu-d3",
        "preset": "4vcpu-16gb",
        "subnet_id": "subnet-id",
        "no_public_ip": False,
        "allow_downgrade": False,
        "downgrade_after": 2,
    }
    args.update(overrides)
    return argparse.Namespace(**args)


def test_validate_create_args_accepts_valid_args(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    m.validate_create_args(
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        disk_type="network-ssd-nonreplicated",
        disk_size=930,
    )


def test_validate_create_args_rejects_preset_unavailable_for_platform(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    with pytest.raises(Exception, match="Preset 2vcpu-8gb is not available"):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="2vcpu-8gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=930,
        )


def test_validate_create_args_requires_vm_user_passwd(monkeypatch):
    monkeypatch.delenv("VM_USER_PASSWD", raising=False)
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    with pytest.raises(
        Exception, match="VM_USER_PASSWD environment variable is not set"
    ):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="4vcpu-16gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=930,
        )


def test_validate_create_args_requires_github_token(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(Exception, match="GITHUB_TOKEN environment variable is not set"):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="4vcpu-16gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=930,
        )


def test_validate_create_args_rejects_nonreplicated_disk_size_not_multiple_of_93(
    monkeypatch,
):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    with pytest.raises(ValueError, match="multiple of 93GB"):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="4vcpu-16gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=100,
        )


def test_validate_create_args_allows_other_disk_type_size(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    m.validate_create_args(
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        disk_type="network-ssd",
        disk_size=100,
    )


def test_build_disk_request():
    request = m.build_disk_request(
        parent_id="parent-id",
        name="vm-name",
        disk_size=930,
        image_id="image-id",
    )

    assert request.metadata.parent_id == "parent-id"
    assert request.metadata.name == "disk-vm-name"
    assert request.spec.size_gibibytes == 930
    assert request.spec.source_image_id == "image-id"
    assert request.spec.type == m.DiskSpec.DiskType.NETWORK_SSD_NON_REPLICATED


def test_create_disk_removes_created_disk_when_wait_fails(monkeypatch):
    class FakeDiskOperation:
        resource_id = "disk-id"

        async def wait(self):
            raise m.RequestError(SimpleNamespace())

    class FakeDiskService:
        async def create(self, request):
            self.request = request
            return FakeDiskOperation()

    removed = []

    async def fake_remove_disk_by_id(_sdk, args, disk_id):
        removed.append((_sdk, args, disk_id))

    fake_service = FakeDiskService()
    sdk = object()
    args = make_create_args(apply=True)
    created_clients = []

    def fake_disk_service_client(sdk_arg):
        created_clients.append(sdk_arg)
        return fake_service

    monkeypatch.setattr(m, "DiskServiceClient", fake_disk_service_client)
    monkeypatch.setattr(m, "remove_disk_by_id", fake_remove_disk_by_id)

    with pytest.raises(m.RequestError):
        asyncio.run(m.create_disk(sdk, args))

    assert fake_service.request.metadata.name == "disk-vm-name"
    assert created_clients == [sdk]
    assert removed == [(sdk, args, "disk-id")]


def test_build_vm_labels_adds_runner_metadata_to_copy_of_existing_labels():
    original_labels = {"test": "librarian"}

    labels = m.build_vm_labels(original_labels, "runner-label", "large")

    assert labels == {
        "test": "librarian",
        "runner-label": "runner-label",
        "runner-flavor": "large",
    }
    assert original_labels == {"test": "librarian"}


def test_build_instance_request_with_public_ip():
    labels = {"test": "librarian", "runner-label": "runner-label"}

    request = m.build_instance_request(
        parent_id="parent-id",
        name="vm-name",
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        subnet_id="subnet-id",
        no_public_ip=False,
        disk_id="disk-id",
        user_data="cloud-init",
        labels=labels,
    )

    assert request.metadata.parent_id == "parent-id"
    assert request.metadata.name == "vm-name"
    assert request.metadata.labels == labels
    assert request.spec.cloud_init_user_data == "cloud-init"
    assert request.spec.resources.platform == "cpu-d3"
    assert request.spec.resources.preset == "4vcpu-16gb"
    assert request.spec.boot_disk.existing_disk.id == "disk-id"
    assert request.spec.boot_disk.device_id == "boot"
    assert (
        request.spec.boot_disk.attach_mode == m.AttachedDiskSpec.AttachMode.READ_WRITE
    )

    network_interface = request.spec.network_interfaces[0]
    assert network_interface.name == "eth0"
    assert network_interface.subnet_id == "subnet-id"
    assert network_interface.public_ip_address is not None


def test_build_instance_request_without_public_ip():
    request = m.build_instance_request(
        parent_id="parent-id",
        name="vm-name",
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        subnet_id="subnet-id",
        no_public_ip=True,
        disk_id="disk-id",
        user_data="cloud-init",
        labels={},
    )

    assert request.spec.network_interfaces[0].public_ip_address is None


@pytest.mark.parametrize(
    "attempt,expected_preset",
    [
        pytest.param(0, "16vcpu-64gb", id="first-attempt-does-not-downgrade"),
        pytest.param(1, "16vcpu-64gb", id="before-boundary-does-not-downgrade"),
        pytest.param(2, "8vcpu-32gb", id="boundary-downgrades-one-step"),
    ],
)
def test_maybe_downgrade_preset(attempt, expected_preset):
    preset = m.maybe_downgrade_preset(
        platform_id="cpu-d3",
        preset="16vcpu-64gb",
        allow_downgrade=True,
        downgrade_after=2,
        attempt=attempt,
    )

    assert preset == expected_preset


def test_maybe_downgrade_preset_keeps_minimum_preset():
    preset = m.maybe_downgrade_preset(
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        allow_downgrade=True,
        downgrade_after=2,
        attempt=2,
    )

    assert preset == "4vcpu-16gb"


def test_maybe_downgrade_preset_respects_disabled_flag():
    preset = m.maybe_downgrade_preset(
        platform_id="cpu-d3",
        preset="16vcpu-64gb",
        allow_downgrade=False,
        downgrade_after=2,
        attempt=2,
    )

    assert preset == "16vcpu-64gb"


def test_extract_instance_ips_with_public_ip():
    instance = SimpleNamespace(
        status=SimpleNamespace(
            network_interfaces=[
                SimpleNamespace(
                    ip_address=SimpleNamespace(address="10.0.0.5/32"),
                    public_ip_address=SimpleNamespace(address="198.51.100.10/32"),
                )
            ]
        )
    )

    assert m.extract_instance_ips(instance, no_public_ip=False) == (
        "10.0.0.5",
        "198.51.100.10",
    )


def test_extract_instance_ips_without_public_ip():
    instance = SimpleNamespace(
        status=SimpleNamespace(
            network_interfaces=[
                SimpleNamespace(
                    ip_address=SimpleNamespace(address="10.0.0.5/32"),
                    public_ip_address=None,
                )
            ]
        )
    )

    assert m.extract_instance_ips(instance, no_public_ip=True) == ("10.0.0.5", None)
