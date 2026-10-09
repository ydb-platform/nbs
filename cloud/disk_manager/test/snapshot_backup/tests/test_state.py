import json
from pathlib import Path
import tempfile
import unittest

from cloud.disk_manager.test.snapshot_backup.config import Blocked
from cloud.disk_manager.test.snapshot_backup.state import CASES, KEY_CASES, State, metrics
from cloud.disk_manager.test.snapshot_backup.tests.helpers import make_config


class StateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.config = make_config(self.directory.name)

    def test_default_is_pending_never_green(self):
        state = State(self.directory.name)
        self.assertEqual(state.data["status"], "pending")
        self.assertEqual(state.data["last_success"], 0)
        rendered = metrics(self.config, state.data, 123)
        self.assertIn('status="pass"} 0', rendered)
        for case in CASES:
            self.assertIn('case="' + case + '",status="pending"} 1', rendered)

    def test_failure_latch_survives_restart_and_later_success(self):
        state = State(self.directory.name)
        state.finish("fail", "integrity mismatch")
        restarted = State(self.directory.name)
        self.assertTrue(restarted.data["failure_latched"])
        self.assertEqual(restarted.data["last_success"], 0)
        restarted.finish("pass", "verified")
        loaded = State(self.directory.name)
        self.assertTrue(loaded.data["failure_latched"])
        self.assertEqual(loaded.data["failures"], 1)
        self.assertEqual(loaded.data["cycles"], 1)
        self.assertGreater(loaded.data["last_success"], 0)

    def test_blocked_is_not_success_and_preserves_unresolved_intent(self):
        state = State(self.directory.name)
        state.data["active"] = True
        resource = {"id": None, "name": "created-maybe", "owner": "owner", "deleted": False}
        state.data["resources"] = [resource]
        state.finish("blocked", "timeout")
        restarted = State(self.directory.name)
        self.assertEqual(restarted.data["resources"], [resource])
        self.assertTrue(restarted.data["active"])
        self.assertEqual(restarted.data["last_success"], 0)
        rendered = metrics(self.config, restarted.data, 123)
        self.assertIn('status="blocked"} 1', rendered)
        self.assertIn('snapshot_backup_resource_reconciliation_required{environment="testing",zone="zone-a",suite="snapshot_backup"} 1', rendered)

    def test_state_file_is_atomic_private_and_no_temporary_files_remain(self):
        state = State(self.directory.name)
        state.save()
        self.assertEqual(state.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(Path(self.directory.name).iterdir()), [state.path])
        self.assertEqual(json.loads(state.path.read_text()), state.data)

    def test_world_accessible_or_symlink_directory_is_rejected(self):
        child = Path(self.directory.name) / "public"
        child.mkdir(mode=0o755)
        with self.assertRaises(Blocked):
            State(child)
        link = Path(self.directory.name) / "link"
        link.symlink_to(child)
        with self.assertRaises(Blocked):
            State(link)

    def test_corrupt_journal_is_blocked_not_reset(self):
        path = Path(self.directory.name) / "state.json"
        for value in ('not-json', '{"schema":2,"status":"pass"}', '{"schema":1,"status":"other"}'):
            path.write_text(value)
            with self.assertRaises(Blocked):
                State(self.directory.name)
            self.assertEqual(path.read_text(), value)

    def test_incomplete_or_wrong_typed_journal_is_blocked(self):
        state = State(self.directory.name)
        default = dict(state.data)
        variants = [{"schema": 1, "status": "pass"}, [], None]
        for patch in ({"attempts": "1"}, {"attempts": True}, {"failures": -1},
                      {"active": 1}, {"last_success": float("nan")},
                      {"last_completion": float("inf")}, {"cases": []},
                      {"cases": {"unexpected": "pass"}}, {"resources": {}},
                      {"resources": [{"id": 123, "name": "name", "owner": "owner", "deleted": False}]}):
            variants.append(dict(default, **patch))
        for value in variants:
            with self.subTest(value=value):
                state.path.write_text(json.dumps(value))
                with self.assertRaises(Blocked):
                    State(self.directory.name)

    def test_metrics_have_fixed_cardinality_and_no_resource_ids_or_reasons(self):
        state = State(self.directory.name)
        state.data.update(attempts=2, status="blocked", reason="sensitive-value",
                          resources=[{"id": "unique-id", "name": "unique-name", "owner": "unique-owner"}])
        rendered = metrics(self.config, state.data, 123)
        for private in ("sensitive-value", "unique-id", "unique-name", "unique-owner"):
            self.assertNotIn(private, rendered)
        self.assertIn("budget_remaining_cycles", rendered)
        self.assertEqual(sum('snapshot_backup_status{' in line for line in rendered.splitlines()), 5)

    def test_key_case_metrics_are_only_required_for_encrypted_configuration(self):
        state = State(self.directory.name)
        plain = metrics(self.config, state.data, 123)
        config = make_config(self.directory.name, require_encryption=True, key_files={"key": "/private/test.key"})
        encrypted = metrics(config, state.data, 123)
        for case in KEY_CASES:
            self.assertNotIn('case="' + case + '"', plain)
            self.assertIn('case="' + case + '",status="pending"} 1', encrypted)

    def test_invalid_retained_reference_is_rejected_without_resetting_journal(self):
        state = State(self.directory.name)
        for reference in ({}, [], {"id": "old", "sha256": "not-a-digest"}):
            state.data["retained_backup"] = reference
            state.save()
            with self.assertRaises(Blocked):
                State(self.directory.name)


if __name__ == "__main__":
    unittest.main()
