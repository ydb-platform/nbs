"""Explicit configuration for a disposable, opt-in backup test VM."""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path


class Blocked(Exception):
    """A safe-to-log precondition failure; never a successful test."""


@dataclass(frozen=True)
class Config:
    environment: str
    zone: str
    folder_id: str
    instance_id: str
    source_disk_id: str
    target_disk_id: str
    source_device: str
    target_device: str
    compute_profile: str
    storage_profile: str
    bucket: str
    presign_host: str
    state_dir: str
    provider_command: str
    provider_config: str
    provider_trace_paths: list
    size_bytes: int = 1073741824
    prefix: str = ""
    curl: str = "curl"
    key_files: dict = field(default_factory=dict)
    require_encryption: bool = True
    interval_seconds: int = 300
    cycle_timeout_seconds: int = 1800
    request_timeout_seconds: int = 120
    max_cycles: int = 24
    metrics_port: int = 9799

    def __post_init__(self):
        if self.environment not in ("testing", "preprod"):
            raise Blocked("Only an explicitly configured testing/preprod VM is supported")
        identifiers = (self.zone, self.folder_id, self.instance_id,
                       self.source_disk_id, self.target_disk_id)
        if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", x or "")
               for x in identifiers):
            raise Blocked("Invalid resource identity")
        if self.source_disk_id == self.target_disk_id:
            raise Blocked("Source and restore disk IDs must differ")
        for device in (self.source_device, self.target_device):
            if not re.fullmatch(r"/dev/disk/by-id/virtio-[A-Za-z0-9_-]{1,20}", device):
                raise Blocked("Only explicit virtio by-id device paths are allowed")
        if self.source_device == self.target_device:
            raise Blocked("Source and restore device paths must differ")
        if type(self.size_bytes) is not int or not 4 * 1024**2 <= self.size_bytes <= 4 * 1024**3:
            raise Blocked("This bounded smoke test supports 4 MiB to 4 GiB disks")
        for value in (self.interval_seconds, self.cycle_timeout_seconds,
                      self.request_timeout_seconds, self.max_cycles):
            if type(value) is not int or value <= 0:
                raise Blocked("Timeouts and cycle budget must be positive integers")
        if type(self.metrics_port) is not int or not 1024 <= self.metrics_port <= 65535:
            raise Blocked("Invalid unprivileged metrics port")
        if not Path(self.state_dir).is_absolute():
            raise Blocked("An absolute private state directory is required")
        if not isinstance(self.require_encryption, bool) or not isinstance(self.key_files, dict):
            raise Blocked("Invalid encryption configuration")
        if self.require_encryption and not self.key_files:
            raise Blocked("Encrypted backup checking requires explicit KEK file mappings")
        if not self.compute_profile or not self.storage_profile:
            raise Blocked("Explicit noninteractive provider profiles are required")
        if (not Path(self.provider_command).is_absolute()
                or not Path(self.provider_config).is_absolute()):
            raise Blocked("Absolute provider executable and private configuration paths are required")
        if (not isinstance(self.provider_trace_paths, list) or not self.provider_trace_paths
                or any(not isinstance(path, str) or not Path(path).is_absolute()
                       for path in self.provider_trace_paths)):
            raise Blocked("Explicit provider trace directories are required")


def load(path):
    try:
        with open(path, encoding="utf-8") as stream:
            document = json.load(stream)
        return Config(**document)
    except (OSError, ValueError, TypeError):
        raise Blocked("Invalid tester configuration; check the JSON schema/example") from None
