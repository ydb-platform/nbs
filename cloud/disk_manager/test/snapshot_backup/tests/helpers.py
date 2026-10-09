"""Fixtures imported by both unittest and YA's namespaced test modules."""

from cloud.disk_manager.test.snapshot_backup.config import Config


def make_config(directory, **overrides):
    values = dict(environment="testing", zone="zone-a", folder_id="test-folder",
                  instance_id="test-vm", source_disk_id="source-disk", target_disk_id="target-disk",
                  source_device="/dev/disk/by-id/virtio-source", target_device="/dev/disk/by-id/virtio-target",
                  compute_profile="compute-robot", storage_profile="storage-reader",
                  bucket="backup-bucket", presign_host="storage.example.test",
                  provider_command="/test/provider", provider_config="/private/provider.json",
                  provider_trace_paths=["/var/lib/provider-trace"],
                  state_dir=str(directory), size_bytes=4 * 1024**2, require_encryption=False)
    values.update(overrides)
    return Config(**values)
