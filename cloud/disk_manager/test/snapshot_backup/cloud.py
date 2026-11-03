"""Small synchronous Compute adapter. Errors never include CLI output."""

import json
import time
from urllib.request import ProxyHandler, build_opener

from .config import Blocked
from .transport import AuthError, BackupError, require_read_only_trace, run_command


class Cloud:
    def __init__(self, config):
        self.config = config

    def call(self, resource, method, request, deadline):
        timeout = min(self.config.request_timeout_seconds, deadline - time.monotonic())
        if timeout <= 0:
            raise Blocked("Compute deadline exceeded")
        try:
            require_read_only_trace(self.config.provider_trace_paths)
        except AuthError as error:
            raise Blocked(str(error)) from None
        command = [self.config.provider_command, "--config", self.config.provider_config]
        envelope = {"version": 1, "operation": "compute", "profile": self.config.compute_profile,
                    "resource": resource, "method": method, "request": request}
        try:
            result = run_command(command, input_data=json.dumps(envelope).encode(),
                                 deadline=deadline, timeout=timeout)
        except BackupError:
            raise Blocked("Compute command failed or timed out; mutation outcome may be unknown") from None
        if result.returncode:
            raise Blocked("Compute command failed; inspect resource status using the operator profile")
        try:
            response = json.loads(result.stdout)
        except ValueError:
            raise Blocked("Invalid Compute response; mutation outcome may be unknown") from None
        if not isinstance(response, dict):
            raise Blocked("Unexpected Compute response")
        return response

    def verify_vm(self, deadline):
        # Pin the execution VM, not merely a remote Compute resource whose ID
        # happens to be in a configuration copied to a different machine.
        try:
            timeout = min(5, deadline - time.monotonic())
            if timeout <= 0:
                raise Blocked("VM identity deadline exceeded")
            with build_opener(ProxyHandler({})).open(
                    "http://169.254.169.254/latest/meta-data/instance-id", timeout=timeout) as response:
                local_id = response.read(1024).decode().strip()
        except Exception:
            raise Blocked("Unable to verify this VM identity through metadata") from None
        if local_id != self.config.instance_id:
            raise Blocked("This VM is not the configured disposable tester")
        vm = self.call("instance", "get", {"instance_id": local_id}, deadline)
        if (vm.get("id") != local_id or vm.get("folder_id") != self.config.folder_id
                or vm.get("zone_id") != self.config.zone or vm.get("status") != "RUNNING"
                or vm.get("labels", {}).get("purpose") != "snapshot-backup-e2e"):
            raise Blocked("Compute VM identity/folder/zone/status/test-purpose mismatch")
        boot_id = vm.get("boot_disk", {}).get("disk_id")
        attached = vm.get("secondary_disks", [])
        for disk_id, device in ((self.config.source_disk_id, self.config.source_device),
                                (self.config.target_disk_id, self.config.target_device)):
            expected_name = device.split("virtio-", 1)[1]
            matches = [d for d in attached if d.get("disk_id") == disk_id]
            if (disk_id == boot_id or len(matches) != 1
                    or matches[0].get("device_name") != expected_name
                    or matches[0].get("mode") != "READ_WRITE"):
                raise Blocked("A disposable data disk is not attached under the expected device name")
            disk = self.call("disk", "get", {"disk_id": disk_id}, deadline)
            if (disk.get("id") != disk_id or disk.get("folder_id") != self.config.folder_id
                    or disk.get("zone_id") != self.config.zone
                    or str(disk.get("size")) != str(self.config.size_bytes)):
                raise Blocked("Disposable disk identity/folder/zone/size mismatch")

    def create_snapshot(self, name, owner, deadline):
        response = self.call("snapshot", "create", {
            "folder_id": self.config.folder_id, "disk_id": self.config.source_disk_id,
            "name": name, "labels": {"snapshot-backup-tester": owner},
        }, deadline)
        # The synchronous adapter returns the Snapshot; accept an explicit
        # operation response wrapper as well, never an operation's own ID.
        resource = response.get("response", response)
        if not isinstance(resource, dict) or resource.get("source_disk_id") != self.config.source_disk_id:
            raise Blocked("Snapshot create outcome unknown; resolve the journalled name before continuing")
        identity = resource.get("id")
        if not isinstance(identity, str) or not identity:
            raise Blocked("Snapshot create returned no resource ID")
        if (resource.get("folder_id") != self.config.folder_id
                or resource.get("name") != name
                or resource.get("labels", {}).get("snapshot-backup-tester") != owner
                or resource.get("status") != "READY"):
            raise Blocked("Created snapshot identity or readiness does not match the request; reconcile by name")
        return identity

    def delete_snapshot(self, snapshot_id, name, owner, deadline):
        snapshot = self.call("snapshot", "get", {"snapshot_id": snapshot_id}, deadline)
        if (snapshot.get("id") != snapshot_id or snapshot.get("name") != name
                or snapshot.get("folder_id") != self.config.folder_id
                or snapshot.get("source_disk_id") != self.config.source_disk_id
                or snapshot.get("labels", {}).get("snapshot-backup-tester") != owner):
            raise Blocked("Refusing to delete a snapshot not owned by this tester")
        response = self.call("snapshot", "delete", {"snapshot_id": snapshot_id}, deadline)
        if response != {}:
            raise Blocked("Snapshot deletion did not return confirmed completion; reconcile the resource")
