"""Expose a host directory through filestore-vhost's local service.

State is retained across restarts of the same socket. Never destroy the local
filesystem: its root is a symlink into the caller's source/build directory.
"""
import argparse
import json
import os
import signal
import socket
import subprocess
import time
from pathlib import Path


class Stopped(Exception):
    pass


def stop(signum, frame):
    raise Stopped()


def prepare_config(socket_path, shared_dir, state_dir=None):
    shared_dir = Path(shared_dir).resolve(strict=True)
    if not shared_dir.is_dir():
        raise ValueError("shared directory is not a directory: {}".format(shared_dir))
    state = Path(state_dir or (socket_path + ".state")).absolute()
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name in ("root", "sessions", "endpoints", "handles", "cache", "directories"):
        (state / name).mkdir(exist_ok=True)
    root = state / "root" / "nfs_shared"
    if root.is_symlink():
        if root.resolve() != shared_dir:
            raise ValueError("socket state belongs to another shared directory")
    else:
        root.symlink_to(shared_dir, target_is_directory=True)
    config = state / "vhost.txt"
    config.write_text('''VhostServiceConfig {
  ServiceEndpoints {}
  LocalServiceConfig {
    RootPath: %(root)s
    StatePath: %(sessions)s
    PathPrefix: "nfs_"
    OpenNodeByHandleEnabled: false
  }
  EndpointStorageType: ENDPOINT_STORAGE_FILE
  EndpointStorageDir: %(endpoints)s
  HandleOpsQueuePath: %(handles)s
  WriteBackCachePath: %(cache)s
  DirectoryHandlesStoragePath: %(directories)s
  DirectoryHandlesInitialDataSize: 1048576
}
''' % {name: json.dumps(str(state / name)) for name in (
        "root", "sessions", "endpoints", "handles", "cache", "directories")})
    return config


def run_client(binary, port, args, daemon, timeout=60):
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        if daemon.poll() is not None:
            raise RuntimeError("filestore-vhost exited with code {}".format(daemon.returncode))
        try:
            result = subprocess.run(
                [binary, *args, "--server-port", str(port)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=5)
            if result.returncode == 0:
                return
            last_error = result.stdout.decode(errors="replace")
        except subprocess.TimeoutExpired as error:
            last_error = str(error)
        time.sleep(0.1)
    raise RuntimeError("filestore-client {} failed: {}".format(args[0], last_error))


def run(args):
    config = prepare_config(args.socket_path, args.shared_dir, args.state_dir)
    # Hold both reservations until immediately before starting the daemon.
    with socket.socket() as endpoint, socket.socket() as local:
        endpoint.bind(("127.0.0.1", 0))
        local.bind(("127.0.0.1", 0))
        endpoint_port = endpoint.getsockname()[1]
        local_port = local.getsockname()[1]
    daemon = None
    try:
        daemon = subprocess.Popen([
            args.vhost_binary, "--service", "local", "--app-config", str(config),
            "--server-port", str(endpoint_port),
            "--local-service-port", str(local_port),
        ])
        run_client(args.client_binary, local_port, [
            "create", "--filesystem", "shared", "--cloud", "test",
            "--folder", "test", "--block-size", "4096", "--blocks-count", "26214400",
        ], daemon)
        run_client(args.client_binary, endpoint_port, [
            "startendpoint", "--filesystem", "shared",
            "--socket-path", args.socket_path, "--client-id", "virtiofs-shared",
        ], daemon)
        return daemon.wait()
    finally:
        # A second signal must not interrupt child reaping.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if daemon is not None and daemon.poll() is None:
            daemon.terminate()
            try:
                daemon.wait(timeout=5)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait()
        # Preserve session/handle state for migration, but remove stale sockets.
        for suffix in ("", ".pid"):
            try:
                os.unlink(args.socket_path + suffix)
            except FileNotFoundError:
                pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket-path", required=True)
    parser.add_argument("--shared-dir", required=True)
    parser.add_argument("--state-dir", help="Persistent state directory (default: SOCKET.state)")
    parser.add_argument("--vhost-binary", required=True)
    parser.add_argument("--client-binary", required=True)
    # Accepted for compatibility with the former virtiofsd runner.
    parser.add_argument("--sandbox", choices=["none"], default="none")
    args = parser.parse_args()
    args.socket_path = os.path.abspath(args.socket_path)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        return run(args)
    except Stopped:
        return 0
