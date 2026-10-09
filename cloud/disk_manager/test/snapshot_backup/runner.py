"""One bounded synthetic backup cycle and a continuously observable supervisor."""

import fcntl
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .cloud import Cloud
from .config import Blocked
from .devices import Devices
from .reader import BackupReader
from .state import DATA_CASES, KEY_CASES, State, metrics, required_cases
from .transport import (AuthError, BackupError, BackupTimeout, InvalidBackup,
                        PendingBackup, TransportError, PresignedObjectStore)


def make_reader(config):
    keys = {}
    for key_id, path in config.key_files.items():
        try:
            # Limit read size: a wrong path must not load an arbitrary large file.
            with open(path, "rb") as stream:
                keys[key_id] = stream.read(4096)
        except OSError:
            raise Blocked("Cannot read a configured backup KEK file") from None
    store = PresignedObjectStore(config.storage_profile, config.bucket, config.presign_host,
                                 config.prefix, executable=config.provider_command,
                                 config_path=config.provider_config,
                                 trace_paths=config.provider_trace_paths, curl_executable=config.curl,
                                 request_timeout=config.request_timeout_seconds)
    return BackupReader(store, keys, require_encryption=config.require_encryption)


def cleanup(config, state, cloud, deadline):
    for resource in state.data["resources"]:
        if resource.get("deleted"):
            continue
        if not resource.get("id"):
            raise Blocked("Unknown create outcome: reconcile the journalled snapshot name manually")
        cloud.delete_snapshot(resource["id"], resource["name"], resource["owner"], deadline)
        resource["deleted"] = True
        state.save()
    state.data["active"] = False
    state.save()


def backup_reference(config, snapshot_id, digest):
    return dict(id=snapshot_id, sha256=digest, disk_id=config.source_disk_id,
                size_bytes=config.size_bytes, bucket=config.bucket,
                presign_host=config.presign_host, prefix=config.prefix)


def restore_published(config, reference, reader, devices, cloud, destination, deadline):
    try:
        reader.restore(reference["id"], reference["disk_id"], reference["size_bytes"],
                       destination, deadline=deadline, expected_sha256=reference["sha256"])
    except PendingBackup:
        raise InvalidBackup("A previously verified backup map disappeared") from None
    cloud.verify_vm(deadline)
    devices.validate()
    devices.restore_and_verify(destination, reference["sha256"], deadline)
    destination.unlink()


def run_cycle(config, *, cloud=None, devices=None, reader=None, sleep=time.sleep):
    state = State(config.state_dir)
    with (state.directory / "cycle.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Blocked("Another cycle holds the tester lock") from None
        state.data = state.read()
        cloud = cloud or Cloud(config)
        devices = devices or Devices(config)
        deadline = time.monotonic() + config.cycle_timeout_seconds
        current_case = None
        try:
            state.data["cases"] = {}
            retained = state.data.get("retained_backup")
            if retained is not None and retained != backup_reference(config, retained["id"], retained["sha256"]):
                raise Blocked("Retained backup belongs to a different configuration; reconcile before changing scope")
            if state.data["active"]:
                if (state.data["status"] in ("fail", "blocked")
                        and state.data["resources"]
                        and all(resource.get("id") for resource in state.data["resources"])):
                    # Known resource identities can be checked and cleaned up
                    # before retrying. Unknown create outcomes remain blocked.
                    cleanup(config, state, cloud, deadline)
                else:
                    raise Blocked("Previous cycle has unresolved resources; run cleanup/reconcile first")
            if state.data["attempts"] >= config.max_cycles:
                raise Blocked("Backup retention budget exhausted; review retained objects before extending")
            cloud.verify_vm(deadline)
            devices.validate()
            reader = reader or make_reader(config)
            state.data.update(active=True, resources=[], cases={}, status="running",
                              cycle_started=time.time(), reason="cycle started")
            state.data["attempts"] += 1
            state.save()
            seed = os.urandom(32)
            owner = uuid.uuid4().hex
            expected = None
            references = {}
            with tempfile.TemporaryDirectory(prefix="cycle-", dir=state.directory) as temporary:
                for case in DATA_CASES:
                    current_case = case
                    state.data["cases"][case] = "running"
                    state.save()
                    # Recheck attached IDs immediately before destructive writes.
                    cloud.verify_vm(deadline)
                    devices.validate()
                    if case != "unchanged":
                        expected = devices.fill(seed, case, deadline)
                    name = "backup-e2e-" + owner[:16] + "-" + case
                    resource = {"name": name, "owner": owner, "id": None, "deleted": False}
                    # Persist the intent BEFORE the API call. A timeout may mean
                    # the resource exists; never blindly retry snapshot creation.
                    state.data["resources"].append(resource)
                    state.save()
                    resource["id"] = cloud.create_snapshot(name, owner, deadline)
                    state.save()
                    destination = Path(temporary) / (case + ".raw")
                    while True:
                        try:
                            reader.restore(resource["id"], config.source_disk_id, config.size_bytes,
                                           destination, deadline=deadline, expected_sha256=expected)
                            break
                        except PendingBackup:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                raise BackupTimeout("Backup did not become readable before its deadline")
                            sleep(min(5, remaining))
                    cloud.verify_vm(deadline)
                    devices.validate()
                    devices.restore_and_verify(destination, expected, deadline)
                    destination.unlink()
                    state.data["cases"][case] = "pass"
                    state.save()
                    references[case] = backup_reference(config, resource["id"], expected)
                if config.require_encryption:
                    for case in KEY_CASES:
                        current_case = case
                        state.data["cases"][case] = "running"
                        state.save()
                        reference = references["changed"]
                        reader.check_key_rejection(
                            reference["id"], reference["disk_id"], reference["size_bytes"],
                            Path(temporary) / (case + ".raw"), deadline=deadline,
                            expected_sha256=reference["sha256"], wrong_key=case == "wrong_key")
                        state.data["cases"][case] = "pass"
                        state.save()
                # Only our labelled snapshots are deleted. The VM, source/target
                # disks and backup bucket are never deleted by this loop.
                current_case = "after_delete"
                state.data["cases"][current_case] = "running"
                state.save()
                cleanup(config, state, cloud, deadline)
                for case, reference in (("after_delete", references["changed"]),
                                        ("retained", retained or references["full"])):
                    current_case = case
                    state.data["cases"][case] = "running"
                    state.save()
                    restore_published(config, reference, reader, devices, cloud,
                                      Path(temporary) / (case + ".raw"), deadline)
                    state.data["cases"][case] = "pass"
                    state.save()
                # Replace the anchor only after all checks pass. Preserve the
                # old reference across failures and process/service restarts.
                state.data["retained_backup"] = references["changed"]
                state.save()
            state.finish("pass", "backup restore, retained data, key checks and cleanup passed")
            return 0
        except InvalidBackup:
            if current_case:
                state.data["cases"][current_case] = "fail"
            state.finish("fail", "backup integrity or restored data mismatch; retain journal for investigation")
            return 1
        except BackupTimeout:
            if current_case:
                state.data["cases"][current_case] = "fail"
            state.finish("fail", "backup cycle deadline exceeded; retain journal for investigation")
            return 1
        except (Blocked, AuthError, TransportError, BackupError, OSError, ValueError) as error:
            if current_case:
                state.data["cases"][current_case] = "blocked"
            reason = str(error) if isinstance(error, (Blocked, BackupError)) else "local precondition failed"
            state.finish("blocked", reason)
            return 2


def reconcile_known(config):
    state = State(config.state_dir)
    with (state.directory / "cycle.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state.data = state.read()
        cleanup(config, state, Cloud(config), time.monotonic() + config.cycle_timeout_seconds)


def record_child_exit(config, previous_completion, returncode, interruption=None):
    """Never let an old passing journal stand in for this child's result."""
    state = State(config.state_dir)
    with (state.directory / "cycle.lock").open("a") as lock:
        # If another actor owns the journal, fail the supervisor instead of
        # overwriting that actor's resource intents. Missing exporter => alert.
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state.data = state.read()
        expected = {0: "pass", 1: "fail", 2: "blocked"}.get(returncode)
        recorded = (state.data["last_completion"] != previous_completion
                    and state.data["status"] == expected)
        if returncode == 0:
            recorded = (recorded and not state.data["active"]
                        and state.data["cases"] == dict.fromkeys(required_cases(config), "pass"))
        if interruption or not recorded:
            # Previous passing case results also belong to the old cycle.
            state.data["cases"] = dict.fromkeys(required_cases(config), "blocked")
            state.finish("blocked", interruption or "cycle exited without a matching durable result")


def serve(config, config_path):
    state = State(config.state_dir)
    stop = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            try:
                body = metrics(config, state.read(), time.time()).encode()
            except Exception:
                self.send_error(503)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    # Export only to a host-local monitoring agent; no unauthenticated cloud port.
    server = ThreadingHTTPServer(("127.0.0.1", config.metrics_port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        while not stop.is_set():
            previous_completion = state.read()["last_completion"]
            child = subprocess.Popen(
                [sys.executable, "-m", "cloud.disk_manager.test.snapshot_backup",
                 "--config", str(Path(config_path).resolve()), "once"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, start_new_session=True)
            end = time.monotonic() + config.cycle_timeout_seconds + 30
            while child.poll() is None and time.monotonic() < end and not stop.wait(1):
                pass
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    # Do not start another cycle if the kernel cannot stop this
                    # process (e.g. blocked disk I/O). Metrics remain reachable.
                    while child.poll() is None and not stop.wait(1):
                        pass
                record_child_exit(config, previous_completion, child.returncode,
                                  "cycle interrupted or supervisor deadline reached")
            else:
                record_child_exit(config, previous_completion, child.returncode)
            if child.returncode is not None and child.returncode < 0 and not stop.is_set():
                # A hard-killed child cannot unwind its adapter cleanup. Exit
                # the hardened unit so KillMode=control-group clears every
                # descendant before systemd restarts the service.
                raise Blocked("Cycle exited without cleanup; restart the hardened service")
            stop.wait(config.interval_seconds)
    finally:
        server.shutdown()
        server.server_close()
