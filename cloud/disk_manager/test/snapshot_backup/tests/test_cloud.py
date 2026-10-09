import copy
import io
import json
import subprocess
import time
import unittest
from unittest import mock

from cloud.disk_manager.test.snapshot_backup.cloud import Cloud
from cloud.disk_manager.test.snapshot_backup.config import Blocked
from cloud.disk_manager.test.snapshot_backup.transport import AuthError, BackupTimeout
from cloud.disk_manager.test.snapshot_backup.tests.helpers import make_config


class CloudTests(unittest.TestCase):
    def setUp(self):
        guard = mock.patch("cloud.disk_manager.test.snapshot_backup.cloud.require_read_only_trace")
        guard.start()
        self.addCleanup(guard.stop)
        self.config = make_config("/private/tester", provider_config="/private/provider.json")
        self.cloud = Cloud(self.config)
        self.snapshot = {"id": "snapshot-id", "source_disk_id": self.config.source_disk_id,
                         "folder_id": self.config.folder_id, "name": "tester-name",
                         "labels": {"snapshot-backup-tester": "owner"}, "status": "READY"}
        self.vm = {"id": self.config.instance_id, "folder_id": self.config.folder_id,
                   "labels": {"purpose": "snapshot-backup-e2e"},
                   "zone_id": self.config.zone, "status": "RUNNING", "boot_disk": {"disk_id": "boot"},
                   "secondary_disks": [{"disk_id": self.config.source_disk_id, "device_name": "source",
                                        "mode": "READ_WRITE"},
                                       {"disk_id": self.config.target_disk_id, "device_name": "target",
                                        "mode": "READ_WRITE"}]}

    def test_create_checks_resource_identity_and_never_uses_operation_id(self):
        for response in (self.snapshot, {"id": "operation-id", "response": self.snapshot}):
            with mock.patch.object(self.cloud, "call", return_value=response) as call:
                self.assertEqual(self.cloud.create_snapshot("tester-name", "owner", 100), "snapshot-id")
                self.assertEqual(call.call_args.args[0:2], ("snapshot", "create"))
        for patch in ({"id": ""}, {"source_disk_id": "other"}, {"folder_id": "other"},
                      {"name": "other"}, {"labels": {}}, {"status": "CREATING"}):
            with self.subTest(patch=patch), mock.patch.object(self.cloud, "call", return_value=dict(self.snapshot, **patch)):
                with self.assertRaises(Blocked):
                    self.cloud.create_snapshot("tester-name", "owner", 100)
        with mock.patch.object(self.cloud, "call", return_value={"id": "operation-id", "done": False}):
            with self.assertRaises(Blocked):
                self.cloud.create_snapshot("tester-name", "owner", 100)

    def test_delete_checks_every_ownership_field_before_mutation(self):
        for patch in ({"id": "other"}, {"source_disk_id": "other"}, {"folder_id": "other"},
                      {"name": "other"}, {"labels": {}},
                      {"labels": {"snapshot-backup-tester": "someone-else"}}):
            with self.subTest(patch=patch), mock.patch.object(self.cloud, "call", return_value=dict(self.snapshot, **patch)) as call:
                with self.assertRaises(Blocked):
                    self.cloud.delete_snapshot("snapshot-id", "tester-name", "owner", 100)
                self.assertEqual(call.call_count, 1)
                self.assertEqual(call.call_args.args[1], "get")
        with mock.patch.object(self.cloud, "call", side_effect=[self.snapshot, {}]) as call:
            self.cloud.delete_snapshot("snapshot-id", "tester-name", "owner", 100)
            self.assertEqual([c.args[1] for c in call.call_args_list], ["get", "delete"])

    def test_delete_rejects_unconfirmed_or_error_responses(self):
        for response in ({"id": "operation", "done": False}, {"error": "failed"}, {"done": True}):
            with self.subTest(response=response), \
                    mock.patch.object(self.cloud, "call", side_effect=[self.snapshot, response]):
                with self.assertRaisesRegex(Blocked, "confirmed completion"):
                    self.cloud.delete_snapshot("snapshot-id", "tester-name", "owner", 100)

    def verify(self, vm=None, disk_patch=None, metadata_id=None):
        def call(resource, method, request, deadline):
            if resource == "instance":
                return vm if vm is not None else self.vm
            return dict({"id": request["disk_id"], "folder_id": self.config.folder_id,
                         "zone_id": self.config.zone, "size": str(self.config.size_bytes)}, **(disk_patch or {}))
        response = io.BytesIO((metadata_id or self.config.instance_id).encode())
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch("cloud.disk_manager.test.snapshot_backup.cloud.build_opener", return_value=opener), \
                mock.patch.object(self.cloud, "call", side_effect=call):
            self.cloud.verify_vm(time.monotonic() + 30)

    def test_verify_requires_local_metadata_and_expected_attachments(self):
        self.verify()
        with self.assertRaises(Blocked):
            self.verify(metadata_id="another-vm")
        for patch in ({"id": "other"}, {"folder_id": "other"}, {"zone_id": "other"},
                      {"labels": {}}, {"labels": {"purpose": "service-vm"}},
                      {"status": "STOPPED"}, {"boot_disk": {"disk_id": self.config.source_disk_id}},
                      {"secondary_disks": []}):
            with self.subTest(patch=patch), self.assertRaises(Blocked):
                self.verify(vm=dict(self.vm, **patch))
        for patch in ({"device_name": "unexpected"}, {"mode": "READ_ONLY"}):
            vm = copy.deepcopy(self.vm)
            vm["secondary_disks"][0].update(patch)
            with self.subTest(patch=patch), self.assertRaises(Blocked):
                self.verify(vm=vm)
        vm = copy.deepcopy(self.vm)
        del vm["secondary_disks"][0]["mode"]
        with self.assertRaises(Blocked):
            self.verify(vm=vm)
        for patch in ({"size": "1"}, {"id": "other"}, {"zone_id": "other"}, {"folder_id": "other"}):
            with self.subTest(patch=patch), self.assertRaises(Blocked):
                self.verify(disk_patch=patch)

    def test_call_uses_versioned_adapter_protocol_without_shell_or_retries(self):
        def run(command, **kwargs):
            self.assertEqual(command, ["/test/provider", "--config", "/private/provider.json"])
            self.assertGreater(kwargs["timeout"], 0)
            self.assertNotIn("shell", kwargs)
            self.assertEqual(json.loads(kwargs["input_data"]), {
                "version": 1, "operation": "compute", "profile": "compute-robot",
                "resource": "disk", "method": "get", "request": {"disk_id": "disk"},
            })
            return subprocess.CompletedProcess(command, 0, b'{"id":"disk"}')
        with mock.patch("cloud.disk_manager.test.snapshot_backup.cloud.run_command", side_effect=run) as run:
            self.assertEqual(self.cloud.call("disk", "get", {"disk_id": "disk"}, time.monotonic() + 30), {"id": "disk"})
            run.assert_called_once()

    def test_errors_and_timeouts_are_sanitized_and_not_retried(self):
        for result in (subprocess.CompletedProcess([], 1, b"private-secret"),
                       subprocess.CompletedProcess([], 0, b""),
                       subprocess.CompletedProcess([], 0, b"private-secret"),
                       subprocess.CompletedProcess([], 0, b"[]")):
            with mock.patch("cloud.disk_manager.test.snapshot_backup.cloud.run_command", return_value=result) as run:
                with self.assertRaises(Blocked) as caught:
                    self.cloud.call("snapshot", "create", {}, time.monotonic() + 30)
                self.assertNotIn("private-secret", str(caught.exception))
                run.assert_called_once()
        with mock.patch("cloud.disk_manager.test.snapshot_backup.cloud.run_command", side_effect=BackupTimeout("private-command")) as run:
            with self.assertRaises(Blocked) as caught:
                self.cloud.call("snapshot", "create", {}, time.monotonic() + 30)
            self.assertNotIn("private-command", str(caught.exception))
            run.assert_called_once()

    def test_unprotected_trace_blocks_before_compute_command(self):
        with mock.patch("cloud.disk_manager.test.snapshot_backup.cloud.require_read_only_trace",
                        side_effect=AuthError("Use the hardened systemd unit")), \
                mock.patch("cloud.disk_manager.test.snapshot_backup.cloud.run_command") as run:
            with self.assertRaisesRegex(Blocked, "systemd"):
                self.cloud.call("snapshot", "create", {}, time.monotonic() + 30)
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
