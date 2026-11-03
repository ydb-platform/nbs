import json
from pathlib import Path
import tempfile
import unittest

from cloud.disk_manager.test.snapshot_backup.config import Blocked, load
from cloud.disk_manager.test.snapshot_backup.tests.helpers import make_config


class ConfigTests(unittest.TestCase):
    def test_minimal_testing_and_preprod_config(self):
        for environment in ("testing", "preprod"):
            self.assertEqual(make_config("/private/tester", environment=environment).environment, environment)

    def test_invalid_destructive_targets_are_rejected(self):
        for patch in ({"environment": "prod"}, {"source_disk_id": "target-disk"},
                      {"target_device": "/dev/disk/by-id/virtio-source"},
                      {"source_device": "/dev/sda"}, {"target_device": "/etc/passwd"},
                      {"source_disk_id": "../disk"}, {"zone": 'zone\"bad'},
                      {"state_dir": "relative"}):
            with self.subTest(patch=patch), self.assertRaises(Blocked):
                make_config("/private/tester", **patch)

    def test_limits_and_encryption_requirements(self):
        for patch in ({"size_bytes": 1024}, {"size_bytes": 8 * 1024**3},
                      {"size_bytes": True}, {"max_cycles": 0}, {"max_cycles": True},
                      {"cycle_timeout_seconds": -1}, {"interval_seconds": 0},
                      {"request_timeout_seconds": 0}, {"metrics_port": 80},
                      {"metrics_port": 65536}, {"require_encryption": True},
                      {"compute_profile": ""}, {"storage_profile": ""}):
            with self.subTest(patch=patch), self.assertRaises(Blocked):
                make_config("/private/tester", **patch)

    def test_provider_configuration_requires_absolute_paths_and_trace_inventory(self):
        for patch in ({"provider_command": "provider"}, {"provider_config": "relative.json"},
                      {"provider_trace_paths": []}, {"provider_trace_paths": "not-a-list"},
                      {"provider_trace_paths": ["relative"]}, {"provider_trace_paths": [None]}):
            with self.subTest(patch=patch), self.assertRaises(Blocked):
                make_config("/private/tester", **patch)

    def test_load_is_explicit_and_bad_json_never_leaks_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            config = make_config(directory)
            path.write_text(json.dumps(vars(config)))
            self.assertEqual(load(path), config)
            for value in ("private-secret not JSON", "[]", '{"unknown":"private-secret"}'):
                path.write_text(value)
                with self.assertRaises(Blocked) as caught:
                    load(path)
                self.assertNotIn("private-secret", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
