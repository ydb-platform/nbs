from pathlib import Path
import signal
import tempfile
import time
import unittest
from unittest import mock

from cloud.disk_manager.test.snapshot_backup.config import Blocked
from cloud.disk_manager.test.snapshot_backup.cloud import Cloud
from cloud.disk_manager.test.snapshot_backup.runner import cleanup, record_child_exit, run_cycle, serve
from cloud.disk_manager.test.snapshot_backup.state import CASES, KEY_CASES, State, required_cases
from cloud.disk_manager.test.snapshot_backup.tests.helpers import make_config
from cloud.disk_manager.test.snapshot_backup.transport import (
    AuthError, InvalidBackup, PendingBackup, TransportError,
)


class FakeCloud:
    def __init__(self, config):
        self.config = config
        self.created = []
        self.deleted = []
        self.verified = 0
        self.create_error = None
        self.delete_error = None

    def verify_vm(self, deadline):
        self.verified += 1

    def create_snapshot(self, name, owner, deadline):
        # A durable intent must already exist even if the subsequent API times out.
        state = State(self.config.state_dir).data
        assert state["active"]
        assert state["resources"][-1] == {"id": None, "name": name, "owner": owner, "deleted": False}
        self.created.append((name, owner))
        if self.create_error:
            raise self.create_error
        return "snapshot-" + str(len(self.created))

    def delete_snapshot(self, identity, name, owner, deadline):
        if self.delete_error:
            raise self.delete_error
        self.deleted.append((identity, name, owner))


class FakeDevices:
    def __init__(self):
        self.filled = []
        self.verified = []
        self.validations = 0
        self.current_digest = None
        self.restore_error = None

    def validate(self):
        self.validations += 1

    def fill(self, seed, case, deadline):
        self.filled.append((seed, case))
        self.current_digest = {"full": "a", "changed": "b", "zero": "0"}[case] * 64
        return self.current_digest

    def restore_and_verify(self, image, expected, deadline):
        assert Path(image).read_bytes() == expected.encode()
        self.verified.append(expected)
        if self.restore_error:
            raise self.restore_error


class FakeReader:
    def __init__(self):
        self.requests = []
        self.error = None
        self.pending_count = 0
        self.key_requests = []
        self.key_error = None

    def restore(self, identity, disk_id, size, destination, *, deadline, expected_sha256):
        self.requests.append((identity, disk_id, expected_sha256))
        if self.pending_count:
            self.pending_count -= 1
            raise PendingBackup("not yet published")
        if self.error:
            raise self.error
        Path(destination).write_bytes(expected_sha256.encode())

    def check_key_rejection(self, identity, disk_id, size, destination, *, deadline,
                            expected_sha256, wrong_key):
        self.key_requests.append((identity, expected_sha256, wrong_key))
        if self.key_error:
            raise self.key_error


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.config = make_config(self.directory.name)
        self.cloud = FakeCloud(self.config)
        self.devices = FakeDevices()
        self.reader = FakeReader()

    def cycle(self, **kwargs):
        return run_cycle(self.config, cloud=self.cloud, devices=self.devices, reader=self.reader, **kwargs)

    def state(self):
        return State(self.directory.name)

    def test_complete_cycle_covers_full_changed_unchanged_zero_and_only_own_cleanup(self):
        self.assertEqual(self.cycle(), 0)
        state = self.state().data
        self.assertEqual(state["status"], "pass")
        self.assertEqual(state["cases"], dict.fromkeys(CASES, "pass"))
        self.assertFalse(state["active"])
        self.assertFalse(state["failure_latched"])
        self.assertEqual((state["attempts"], state["cycles"]), (1, 1))
        self.assertEqual([case for _, case in self.devices.filled], ["full", "changed", "zero"])
        self.assertEqual(len({seed for seed, _ in self.devices.filled}), 1)
        self.assertEqual(self.devices.verified, [value * 64 for value in ("a", "b", "b", "0", "b", "a")])
        self.assertEqual(len(self.cloud.created), 4)
        self.assertEqual(len(self.cloud.deleted), 4)
        self.assertTrue(all(r["deleted"] for r in state["resources"]))
        self.assertGreaterEqual(self.cloud.verified, 9)
        self.assertEqual(sorted(p.name for p in Path(self.directory.name).iterdir()), ["cycle.lock", "state.json"])

    def test_missing_map_is_polled_without_duplicate_snapshot_creation(self):
        self.reader.pending_count = 2
        sleeps = []
        self.assertEqual(self.cycle(sleep=sleeps.append), 0)
        self.assertEqual(sleeps, [5, 5])
        self.assertEqual(len(self.cloud.created), 4)
        self.assertEqual([r[0] for r in self.reader.requests[:3]], ["snapshot-1"] * 3)

    def test_deleted_backups_are_read_after_cleanup_and_retained_across_restart(self):
        original_restore = self.reader.restore

        def restore(identity, *args, **kwargs):
            # Fresh restores happen once; every subsequent read of that ID
            # must follow confirmed deletion of its source snapshot.
            if any(request[0] == identity for request in self.reader.requests):
                self.assertIn(identity, [item[0] for item in self.cloud.deleted])
            return original_restore(identity, *args, **kwargs)
        with mock.patch.object(self.reader, "restore", side_effect=restore):
            self.assertEqual(self.cycle(), 0)
            previous = self.state().data["retained_backup"]
            self.assertEqual(previous["id"], "snapshot-2")
            self.assertEqual(self.cycle(), 0)
        self.assertEqual(self.reader.requests[-1], (previous["id"], previous["disk_id"], previous["sha256"]))
        self.assertEqual(self.state().data["retained_backup"]["id"], "snapshot-6")
        self.assertEqual(len(self.cloud.created), 8)

    def test_retained_missing_map_is_failed_immediately_and_anchor_is_not_replaced(self):
        self.assertEqual(self.cycle(), 0)
        previous = self.state().data["retained_backup"]
        original_restore = self.reader.restore

        def restore(identity, *args, **kwargs):
            if identity == previous["id"]:
                raise PendingBackup("previously published map missing")
            return original_restore(identity, *args, **kwargs)
        with mock.patch.object(self.reader, "restore", side_effect=restore):
            self.assertEqual(self.cycle(), 1)
        state = self.state().data
        self.assertEqual(state["cases"]["retained"], "fail")
        self.assertEqual(state["retained_backup"], previous)
        self.assertEqual(state["cycles"], 1)
        self.assertFalse(state["active"])

    def test_post_delete_corruption_is_failed_not_reported_as_cleanup_success(self):
        original_restore = self.reader.restore

        def restore(*args, **kwargs):
            if self.cloud.deleted:
                raise InvalidBackup("referenced backup chunk missing")
            return original_restore(*args, **kwargs)
        with mock.patch.object(self.reader, "restore", side_effect=restore):
            self.assertEqual(self.cycle(), 1)
        state = self.state().data
        self.assertEqual(state["cases"]["after_delete"], "fail")
        self.assertNotIn("retained_backup", state)
        self.assertEqual(state["last_success"], 0)

    def test_retained_scope_change_blocks_before_cloud_or_disk_work(self):
        self.assertEqual(self.cycle(), 0)
        state = self.state()
        state.data["retained_backup"]["bucket"] = "another-bucket"
        state.save()
        verified, fills = self.cloud.verified, len(self.devices.filled)
        self.assertEqual(self.cycle(), 2)
        self.assertEqual((self.cloud.verified, len(self.devices.filled)), (verified, fills))

    def test_encrypted_cycle_requires_both_negative_key_checks(self):
        self.config = make_config(self.directory.name, require_encryption=True, key_files={"key": "/private/test.key"})
        self.cloud.config = self.config
        self.assertEqual(self.cycle(), 0)
        self.assertEqual(self.reader.key_requests, [
            ("snapshot-2", "b" * 64, False), ("snapshot-2", "b" * 64, True),
        ])
        self.assertEqual(self.state().data["cases"], dict.fromkeys(required_cases(self.config), "pass"))
        record_child_exit(self.config, 0, 0)
        self.assertEqual(self.state().data["status"], "pass")

    def test_key_probe_errors_cannot_be_counted_as_success(self):
        for error, expected_code in ((InvalidBackup("key unexpectedly accepted"), 1),
                                     (TransportError("unavailable"), 2), (AuthError("forbidden"), 2)):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory:
                config = make_config(directory, require_encryption=True, key_files={"key": "/private/test.key"})
                reader = FakeReader()
                reader.key_error = error
                code = run_cycle(config, cloud=FakeCloud(config), devices=FakeDevices(), reader=reader)
                self.assertEqual(code, expected_code)
                state = State(directory).data
                self.assertEqual(state["last_success"], 0)
                self.assertNotEqual(state["cases"][KEY_CASES[0]], "pass")

    def test_supervisor_rejects_legacy_pass_without_new_required_checks(self):
        self.config = make_config(self.directory.name, require_encryption=True, key_files={"key": "/private/test.key"})
        state = self.state()
        state.data["cases"] = dict.fromkeys(CASES, "pass")
        state.finish("pass", "missing negative key checks")
        record_child_exit(self.config, 0, 0)
        self.assertEqual(self.state().data["status"], "blocked")

    def test_pending_backup_deadline_is_failed_not_green(self):
        self.config = make_config(self.directory.name, cycle_timeout_seconds=10)
        self.cloud.config = self.config
        clock = [100.0]
        self.reader.error = PendingBackup("not yet published")

        def sleep(seconds):
            clock[0] += seconds
        with mock.patch("time.monotonic", side_effect=lambda: clock[0]):
            self.assertEqual(self.cycle(sleep=sleep), 1)
        state = self.state().data
        self.assertEqual(state["status"], "fail")
        self.assertEqual(state["cases"]["full"], "fail")
        self.assertEqual(state["last_success"], 0)
        self.assertTrue(state["failure_latched"])
        self.assertTrue(state["active"])
        self.assertEqual(len(self.cloud.created), 1)
        self.assertFalse(self.cloud.deleted)

    def test_integrity_error_keeps_journal_then_known_resources_are_reconciled_before_retry(self):
        self.reader.error = InvalidBackup("fixture mismatch")
        self.assertEqual(self.cycle(), 1)
        state = self.state().data
        self.assertEqual(state["resources"][0]["id"], "snapshot-1")
        self.assertTrue(state["active"])
        self.assertEqual(state["status"], "fail")
        self.reader.error = None
        self.assertEqual(self.cycle(), 0)
        self.assertEqual(len(self.cloud.created), 5)
        self.assertEqual(len(self.cloud.deleted), 5)
        self.assertEqual(self.cloud.deleted[0][0], "snapshot-1")
        self.assertEqual(self.state().data["attempts"], 2)
        self.assertTrue(self.state().data["failure_latched"])

    def test_unknown_create_is_journalled_and_never_blindly_retried_or_deleted(self):
        self.cloud.create_error = Blocked("mutation outcome unknown")
        self.assertEqual(self.cycle(), 2)
        state = self.state()
        self.assertIsNone(state.data["resources"][0]["id"])
        with self.assertRaises(Blocked):
            cleanup(self.config, state, self.cloud, time.monotonic() + 30)
        self.assertTrue(self.state().data["active"])
        self.assertFalse(self.cloud.deleted)
        self.assertEqual(self.cycle(), 2)
        self.assertEqual(len(self.cloud.created), 1)

    def test_auth_transport_and_local_preconditions_are_blocked(self):
        for error in (AuthError("no key"), TransportError("no storage"), Blocked("precondition")):
            with self.subTest(error=type(error).__name__):
                # Each subtest has its own private state and fake cloud resources.
                with tempfile.TemporaryDirectory() as directory:
                    config = make_config(directory)
                    reader = FakeReader()
                    reader.error = error
                    code = run_cycle(config, cloud=FakeCloud(config), devices=FakeDevices(), reader=reader)
                    state = State(directory).data
                    self.assertEqual(code, 2)
                    self.assertEqual(state["status"], "blocked")
                    self.assertEqual(state["cases"]["full"], "blocked")
                    self.assertEqual(state["last_success"], 0)

    def test_cleanup_failure_never_reports_a_passing_cycle(self):
        self.cloud.delete_error = Blocked("ownership could not be verified")
        self.assertEqual(self.cycle(), 2)
        state = self.state().data
        self.assertEqual(state["status"], "blocked")
        self.assertEqual(state["last_success"], 0)
        self.assertTrue(state["active"])
        self.assertTrue(all(not resource["deleted"] for resource in state["resources"]))

    def test_unconfirmed_provider_delete_preserves_resource_journal(self):
        self.reader.error = InvalidBackup("fixture mismatch")
        self.assertEqual(self.cycle(), 1)
        state = self.state()
        resource = state.data["resources"][0]
        snapshot = {"id": resource["id"], "name": resource["name"],
                    "folder_id": self.config.folder_id, "source_disk_id": self.config.source_disk_id,
                    "labels": {"snapshot-backup-tester": resource["owner"]}}
        cloud = Cloud(self.config)
        with mock.patch.object(cloud, "call", side_effect=[snapshot, {"done": False}]):
            with self.assertRaises(Blocked):
                cleanup(self.config, state, cloud, time.monotonic() + 30)
        self.assertFalse(self.state().data["resources"][0]["deleted"])
        self.assertTrue(self.state().data["active"])

    def test_known_cleanup_is_durable_skips_deleted_and_never_clears_failure_latch(self):
        self.reader.error = InvalidBackup("mismatch")
        self.cycle()
        state = self.state()
        cleanup(self.config, state, self.cloud, time.monotonic() + 30)
        self.assertEqual(len(self.cloud.deleted), 1)
        cleanup(self.config, self.state(), self.cloud, time.monotonic() + 30)
        self.assertEqual(len(self.cloud.deleted), 1)
        self.assertFalse(self.state().data["active"])
        self.assertTrue(self.state().data["failure_latched"])
        self.reader.error = None
        self.assertEqual(self.cycle(), 0)
        self.assertTrue(self.state().data["failure_latched"])

    def test_retention_budget_blocks_before_any_cloud_or_device_work(self):
        state = self.state()
        state.data.update(attempts=self.config.max_cycles, status="pass", cases=dict.fromkeys(CASES, "pass"))
        state.save()
        self.assertEqual(self.cycle(), 2)
        self.assertEqual(self.cloud.verified, 0)
        self.assertFalse(self.devices.filled)
        self.assertEqual(self.state().data["cases"], {})

    def test_journal_is_reloaded_after_acquiring_lock(self):
        # Another process completes a mutation after this runner constructs
        # State but before it acquires the lock. Its durable intent must survive.
        def lock_acquired(*args):
            state = self.state()
            state.data.update(active=True, attempts=7,
                              resources=[{"id": None, "name": "inflight", "owner": "other", "deleted": False}])
            state.save()
        with mock.patch("fcntl.flock", side_effect=lock_acquired):
            self.assertEqual(self.cycle(), 2)
        self.assertFalse(self.cloud.created)
        self.assertEqual(self.state().data["attempts"], 7)
        self.assertEqual(self.state().data["resources"][0]["name"], "inflight")

    def test_busy_lock_never_starts_cloud_or_writes_devices(self):
        with mock.patch("fcntl.flock", side_effect=BlockingIOError), self.assertRaises(Blocked):
            self.cycle()
        self.assertEqual(self.cloud.verified, 0)
        self.assertFalse(self.devices.filled)

    def test_child_early_exit_never_reuses_previous_pass(self):
        for code in (0, 1, 2, -9):
            with self.subTest(code=code):
                self.assertEqual(self.cycle(), 0)
                previous = self.state().data["last_completion"]
                record_child_exit(self.config, previous, code)
                state = self.state().data
                self.assertEqual(state["status"], "blocked")
                self.assertEqual(state["cases"], dict.fromkeys(CASES, "blocked"))
                self.assertTrue(state["failure_latched"])

    def test_child_new_matching_result_is_preserved(self):
        for status, code in (("pass", 0), ("fail", 1), ("blocked", 2)):
            with self.subTest(status=status):
                state = self.state()
                state.data["cases"] = dict.fromkeys(CASES, status)
                state.finish(status, "child result")
                expected = self.state().data
                record_child_exit(self.config, 0, code)
                self.assertEqual(self.state().data, expected)

    def test_child_exit_status_mismatch_and_partial_pass_are_blocked(self):
        for cases, active, code in ((dict.fromkeys(CASES, "pass"), False, 2),
                                    ({"full": "pass"}, False, 0),
                                    (dict.fromkeys(CASES, "pass"), True, 0)):
            with self.subTest(cases=cases, active=active, code=code):
                state = self.state()
                state.data.update(cases=cases, active=active)
                state.finish("pass", "inconsistent child")
                record_child_exit(self.config, 0, code)
                self.assertEqual(self.state().data["status"], "blocked")

    def test_supervisor_never_overwrites_another_actors_journal(self):
        self.assertEqual(self.cycle(), 0)
        expected = self.state().data
        with mock.patch("fcntl.flock", side_effect=BlockingIOError), self.assertRaises(BlockingIOError):
            record_child_exit(self.config, expected["last_completion"], 2)
        self.assertEqual(self.state().data, expected)

    def test_hard_killed_cycle_exits_service_instead_of_starting_another_cycle(self):
        child = mock.Mock(returncode=-signal.SIGKILL)
        child.poll.return_value = -signal.SIGKILL
        server = mock.Mock()
        with mock.patch("cloud.disk_manager.test.snapshot_backup.runner.threading.Thread"), \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.ThreadingHTTPServer", return_value=server), \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.signal.signal"), \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.subprocess.Popen", return_value=child) as popen:
            with self.assertRaisesRegex(Blocked, "hardened service"):
                serve(self.config, Path(self.directory.name) / "config.json")
        popen.assert_called_once()
        server.shutdown.assert_called_once()
        self.assertEqual(self.state().data["status"], "blocked")

    def test_supervisor_cancellation_stops_process_group_and_keeps_reconciliation_journal(self):
        state = self.state()
        state.data.update(active=True, status="running", cases={"full": "running"},
                          resources=[{"id": "snapshot", "name": "owned", "owner": "owner", "deleted": False}])
        state.save()

        class Event:
            stopped = False

            def is_set(self):
                return self.stopped

            def set(self):
                self.stopped = True

            def wait(self, seconds):
                self.stopped = True  # Simulate SIGTERM while the first child is running.
                return True

        class Child:
            pid = 123456  # Never sent to the OS: killpg is mocked below.
            alive = True
            returncode = None

            def poll(self):
                return None if self.alive else self.returncode

            def wait(self, timeout=None):
                self.alive = False
                self.returncode = -signal.SIGTERM
                return self.returncode

        child = Child()
        server = mock.Mock()
        event = Event()
        with mock.patch("cloud.disk_manager.test.snapshot_backup.runner.threading.Event", return_value=event), \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.threading.Thread"), \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.ThreadingHTTPServer", return_value=server) as server_type, \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.signal.signal"), \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.subprocess.Popen", return_value=child) as popen, \
                mock.patch("cloud.disk_manager.test.snapshot_backup.runner.os.killpg") as kill:
            serve(self.config, Path(self.directory.name) / "config.json")
        popen.assert_called_once()
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        kill.assert_called_once_with(child.pid, signal.SIGTERM)
        self.assertEqual(server_type.call_args.args[0], ("127.0.0.1", self.config.metrics_port))
        server.shutdown.assert_called_once()
        server.server_close.assert_called_once()
        final = self.state().data
        self.assertEqual(final["status"], "blocked")
        self.assertTrue(final["active"])
        self.assertTrue(final["failure_latched"])
        self.assertEqual(final["last_success"], 0)
        self.assertEqual(final["resources"][0]["id"], "snapshot")
        self.assertEqual(final["cases"]["full"], "blocked")


if __name__ == "__main__":
    unittest.main()
