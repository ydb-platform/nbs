import errno
import hashlib
import json
import logging
import os
import signal
import stat
import subprocess
import time
from pathlib import Path

import yatest.common as common
from yatest.common.network import PortManager

from contrib.ydb.tests.library.harness.daemon import Daemon

logger = logging.getLogger(__name__)


def daemon_log_files(prefix, cwd):
    files = [
        ("stdout_file", ".out"),
        ("stderr_file", ".err"),
    ]

    ret = {}
    for tag, suffix in files:
        name = os.path.abspath(os.path.join(cwd, prefix + suffix))
        with open(name, mode='w'):
            pass

        ret[tag] = name

    return ret


class VirtioFsServer:
    def __init__(self, virtiofs_server_binary, socket_path, fspath):
        self.virtiofs_server_binary = virtiofs_server_binary
        self.socket_path = socket_path
        self.fspath = fspath

    @property
    def daemon(self):
        return self.virtiofs_server

    @property
    def pid(self):
        return self.daemon.daemon.process.pid

    def start(self, output_path, tag):
        # Keep the export symlink out of collected test artifacts; reuse state
        # when restarting the same socket during migration.
        state = Path(common.work_path("virtiofs-state-" + hashlib.sha256(
            os.path.abspath(self.socket_path).encode()).hexdigest()[:16]))
        shared = Path(self.fspath).resolve(strict=True)
        if not shared.is_dir():
            raise ValueError("shared directory is not a directory: {}".format(shared))
        paths = ("root", "sessions", "endpoints", "handles", "cache", "directories")
        for name in paths:
            (state / name).mkdir(parents=True, exist_ok=True)
        export = state / "root" / "nfs_shared"
        if export.is_symlink():
            if export.resolve() != shared:
                raise ValueError("socket state belongs to another shared directory")
        else:
            export.symlink_to(shared, target_is_directory=True)
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
''' % {name: json.dumps(str(state / name)) for name in paths})
        with PortManager() as ports:
            endpoint_port = ports.get_port()
            self.local_port = ports.get_port()
            self.virtiofs_server = Daemon(
                [self.virtiofs_server_binary, "--service", "local",
                 "--app-config", str(config), "--server-port", str(endpoint_port),
                 "--local-service-port", str(self.local_port)],
                output_path,
                timeout=180,
                **daemon_log_files(prefix="virtiofs-server-{}".format(tag), cwd=output_path))
            try:
                self.virtiofs_server.start()
                self._run_client(self.local_port, [
                    "create", "--filesystem", "shared", "--cloud", "test",
                    "--folder", "test", "--block-size", "4096", "--blocks-count", "26214400"])
                self._run_client(endpoint_port, [
                    "startendpoint", "--filesystem", "shared",
                    "--socket-path", os.path.abspath(self.socket_path), "--client-id", "virtiofs-shared"])
            except BaseException:
                self.stop(tag)
                raise

    def _run_client(self, port, args):
        deadline = time.monotonic() + 60
        error = ""
        while time.monotonic() < deadline:
            if not self.daemon.is_alive():
                raise RuntimeError("filestore-vhost exited during startup")
            try:
                result = subprocess.run(
                    [common.binary_path("cloud/filestore/apps/client/filestore-client"),
                     *args, "--server-port", str(port)],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=5)
                if result.returncode == 0:
                    return
                error = result.stdout.decode(errors="replace")
            except subprocess.TimeoutExpired as e:
                error = str(e)
            time.sleep(0.1)
        raise RuntimeError("filestore-client {} failed: {}".format(args[0], error))

    def stop(self, tag=None, timeout=10):
        daemon = self.daemon.daemon
        if daemon is None:
            return

        self.stop_pid(
            tag or self.socket_path,
            daemon.process.pid,
            is_alive=self.daemon.is_alive,
            timeout=timeout)

    def wait_for_socket(self, tag=None, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.daemon.is_alive():
                raise RuntimeError(
                    "virtiofs-server {} exited before creating socket {}".format(
                        tag or self.socket_path,
                        self.socket_path))

            try:
                mode = os.stat(self.socket_path).st_mode
                if stat.S_ISSOCK(mode):
                    return
            except OSError:
                pass

            time.sleep(0.1)

        raise RuntimeError(
            "virtiofs-server {} did not create socket {} in {} seconds".format(
                tag or self.socket_path,
                self.socket_path,
                timeout))

    @staticmethod
    def stop_pid(tag, pid, is_alive=None, timeout=10):
        logger.info("stop virtiofs-server %s with pid %s", tag, pid)
        try:
            pid = int(pid)
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            logger.info("virtiofs-server %s pid %s already exited", tag, pid)
            return
        except ValueError:
            logger.warning("invalid virtiofs-server %s pid %s", tag, pid)
            return

        if VirtioFsServer._wait_stopped(tag, pid, is_alive, timeout):
            return

        logger.warning(
            "virtiofs-server %s pid %s did not stop after SIGTERM", tag, pid)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return

        VirtioFsServer._wait_stopped(tag, pid, is_alive, timeout)

    @staticmethod
    def cleanup_socket_files(socket_path):
        VirtioFsServer._unlink_path(socket_path)
        VirtioFsServer._unlink_path("{}.pid".format(socket_path))

    @staticmethod
    def _wait_stopped(tag, pid, is_alive, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not VirtioFsServer._pid_alive(pid, is_alive):
                return True

            time.sleep(0.1)

        logger.warning("virtiofs-server %s pid %s is still alive", tag, pid)
        return False

    @staticmethod
    def _pid_alive(pid, is_alive):
        if is_alive:
            return is_alive()

        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False

        return True

    @staticmethod
    def _unlink_path(path):
        try:
            os.unlink(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise


class VirtioFsServerSet:
    def __init__(self, virtiofs_server_binary, output_path, env_get, env_set):
        self.virtiofs_server_binary = virtiofs_server_binary
        self.output_path = output_path
        self.env_get = env_get
        self.env_set = env_set
        self.servers = {}

    def restart(self, mount_paths, seqno):
        for tag, path, socket_path in mount_paths:
            if not socket_path:
                continue

            self.stop(tag)
            VirtioFsServer.cleanup_socket_files(socket_path)

            logger.info("restart virtiofs-server %s on %s", tag, socket_path)
            virtiofs = VirtioFsServer(
                self.virtiofs_server_binary,
                socket_path,
                path)
            virtiofs.start(
                self.output_path,
                "{}-migration-{}".format(tag, seqno))
            virtiofs.wait_for_socket(tag)

            self.servers[tag] = virtiofs
            self.env_set("VIRTIOFS_PID_{}".format(tag), str(virtiofs.pid))

    def stop(self, tag):
        virtiofs = self.servers.pop(tag, None)
        if virtiofs:
            virtiofs.stop(tag)
            return

        pid = self.env_get("VIRTIOFS_PID_{}".format(tag))
        if pid:
            VirtioFsServer.stop_pid(tag, pid)

    def stop_all(self):
        for tag in list(self.servers):
            self.stop(tag)
