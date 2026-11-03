"""Durable resource journal and low-cardinality Prometheus exposition."""

import json
import os
import re
import tempfile
import time
from pathlib import Path

from .config import Blocked


DATA_CASES = ("full", "changed", "unchanged", "zero")
CASES = DATA_CASES + ("after_delete", "retained")
KEY_CASES = ("missing_key", "wrong_key")
STATUSES = ("pending", "running", "pass", "fail", "blocked")


def required_cases(config):
    return CASES + KEY_CASES if config.require_encryption else CASES


class State:
    def __init__(self, directory):
        self.directory = Path(directory)
        if self.directory.is_symlink():
            raise Blocked("State directory must not be a symlink")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.directory.stat().st_mode & 0o077:
            raise Blocked("State directory must be private (0700)")
        self.path = self.directory / "state.json"
        self.data = self.read()

    def read(self):
        if not self.path.exists():
            return {"schema": 1, "attempts": 0, "cycles": 0, "failures": 0,
                    "failure_latched": False, "status": "pending", "cases": {},
                    "resources": [], "active": False, "last_success": 0,
                    "last_completion": 0, "cycle_started": 0}
        try:
            with self.path.open(encoding="utf-8") as stream:
                value = json.load(stream)
            if not isinstance(value, dict) or value["schema"] != 1 or value["status"] not in STATUSES:
                raise ValueError()
            for key in ("attempts", "cycles", "failures"):
                if type(value[key]) is not int or value[key] < 0:
                    raise ValueError()
            for key in ("active", "failure_latched"):
                if type(value[key]) is not bool:
                    raise ValueError()
            for key in ("last_success", "last_completion", "cycle_started"):
                if type(value[key]) not in (float, int) or not 0 <= value[key] < float("inf"):
                    raise ValueError()
            if not isinstance(value["cases"], dict) or any(
                    k not in CASES + KEY_CASES or v not in STATUSES for k, v in value["cases"].items()):
                raise ValueError()
            if not isinstance(value["resources"], list):
                raise ValueError()
            for resource in value["resources"]:
                if (not isinstance(resource, dict)
                        or not isinstance(resource["name"], str)
                        or not isinstance(resource["owner"], str)
                        or type(resource["deleted"]) is not bool
                        or (resource["id"] is not None and not isinstance(resource["id"], str))):
                    raise ValueError()
            retained = value.get("retained_backup")
            if retained is not None:
                if (not isinstance(retained, dict)
                        or any(not isinstance(retained.get(key), str) or not retained[key]
                               for key in ("id", "disk_id", "bucket", "presign_host"))
                        or not isinstance(retained.get("prefix"), str)
                        or type(retained.get("size_bytes")) is not int or retained["size_bytes"] <= 0
                        or not isinstance(retained.get("sha256"), str)
                        or not re.fullmatch(r"[0-9a-f]{64}", retained["sha256"])):
                    raise ValueError()
            return value
        except (OSError, ValueError, KeyError, TypeError):
            raise Blocked("Invalid resource journal; operator reconciliation required") from None

    def save(self):
        descriptor, path = tempfile.mkstemp(prefix=".state-", dir=self.directory)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(self.data, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(path, self.path)
            descriptor = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def finish(self, status, reason):
        self.data.update(status=status, reason=reason, last_completion=time.time())
        if status == "pass":
            self.data["last_success"] = time.time()
            self.data["cycles"] += 1
        else:
            self.data["failures"] += 1
            self.data["failure_latched"] = True
        self.save()


def metrics(config, data, heartbeat):
    labels = 'environment="%s",zone="%s",suite="snapshot_backup"' % (config.environment, config.zone)
    samples = []

    def add(name, value, extra=""):
        samples.append("snapshot_backup_" + name + "{" + labels + extra + "} " + str(value))
    add("runner_heartbeat_timestamp_seconds", heartbeat)
    add("last_success_timestamp_seconds", data.get("last_success", 0))
    add("last_completion_timestamp_seconds", data.get("last_completion", 0))
    add("cycle_started_timestamp_seconds", data.get("cycle_started", 0))
    add("attempts_total", data.get("attempts", 0))
    add("passed_cycles_total", data.get("cycles", 0))
    add("failed_or_blocked_cycles_total", data.get("failures", 0))
    add("failure_latched", int(data.get("failure_latched", False)))
    add("resource_reconciliation_required", int(data.get("active", False)))
    add("budget_remaining_cycles", max(0, config.max_cycles - data.get("attempts", 0)))
    for status in STATUSES:
        add("status", int(data.get("status", "pending") == status), ',status="' + status + '"')
    for case in required_cases(config):
        for status in STATUSES:
            add("case_status", int(data.get("cases", {}).get(case, "pending") == status),
                ',case="' + case + '",status="' + status + '"')
    return "\n".join(samples) + "\n"
