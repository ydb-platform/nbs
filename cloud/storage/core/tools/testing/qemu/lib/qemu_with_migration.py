import atexit
import errno
import logging
import os
import signal
import stat
import time

import yatest.common as common

from cloud.storage.core.tools.testing.virtiofs_server.lib import VirtioFsServer

from .common import (
    env_with_guest_index,
    get_mount_paths,
    get_qemu_bios,
    get_qemu_firmware,
    get_qemu_kvm,
    get_chardev_reconnect,
    get_virtiofs_migration,
)
from .qemu import Qemu
from .recipe import recipe_get_env

EMU_NET = "10.0.2.0/24"
QEMU_HOST = "10.0.2.2"
VIRTIOFS_SERVER_BINARY = (
    "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")

logger = logging.getLogger(__name__)


class QemuWithMigration:
    def __init__(self, socket_generator):
        self.qemu = Qemu(
            qemu_kvm=get_qemu_kvm(),
            qemu_firmware=get_qemu_firmware(),
            qemu_bios=get_qemu_bios(),
            rootfs=common.build_path("cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img"),
            kernel=None,
            kcmdline=None,
            initrd=None,
            mem="4G",
            proc=8,
            virtio='fs',
            qemu_options=[],
            vhost_socket="",
            enable_kvm=True,
            use_virtiofs_server=True,
            chardev_reconnect=get_chardev_reconnect(),
            virtiofs_migration=get_virtiofs_migration())

        self.socket_generator = socket_generator
        self.virtiofs_servers = {}
        atexit.register(self._stop_restarted_virtiofs_servers)

    def start(self):
        self.socket = self.socket_generator(0, False)

        self.qemu.set_mount_paths(get_mount_paths())
        self.qemu.set_vhost_socket(self.socket)
        self.qemu.start()

    def migrate(self, count, timeout):
        for migration in range(0, count):
            self.socket = self.socket_generator(migration, False)
            self.qemu.migrate(
                migration,
                self.socket,
                before_restore=self._restart_virtiofs_servers)
            time.sleep(timeout)

    def _restart_virtiofs_servers(self):
        for tag, path, socket_path in self.qemu.mount_paths:
            if not socket_path:
                continue

            self._stop_virtiofs_server(tag)
            self._unlink_virtiofs_files(socket_path)

            logger.info("restart virtiofs-server %s on %s", tag, socket_path)
            virtiofs = VirtioFsServer(
                common.binary_path(VIRTIOFS_SERVER_BINARY),
                socket_path,
                path)
            virtiofs.start(
                common.output_path(),
                "{}-migration-{}".format(tag, self.qemu.seqno))
            self._wait_virtiofs_socket(tag, socket_path, virtiofs)

            self.virtiofs_servers[tag] = virtiofs
            os.environ[env_with_guest_index(
                "VIRTIOFS_PID_{}".format(tag),
                self.qemu.inst_index)] = str(virtiofs.daemon.daemon.process.pid)

    def _stop_virtiofs_server(self, tag):
        virtiofs = self.virtiofs_servers.pop(tag, None)
        if virtiofs:
            self._stop_virtiofs_daemon(tag, virtiofs)
            return

        pid = recipe_get_env(
            "VIRTIOFS_PID_{}".format(tag),
            self.qemu.inst_index)
        if not pid:
            return

        self._stop_virtiofs_pid(tag, pid)

    def _stop_restarted_virtiofs_servers(self):
        for tag in list(self.virtiofs_servers):
            self._stop_virtiofs_server(tag)

    def _stop_virtiofs_daemon(self, tag, virtiofs):
        daemon = virtiofs.daemon.daemon
        if daemon is None:
            return

        self._stop_virtiofs_pid(
            tag,
            daemon.process.pid,
            is_alive=virtiofs.daemon.is_alive)

    def _stop_virtiofs_pid(self, tag, pid, is_alive=None, timeout=10):
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

        if self._wait_virtiofs_stopped(tag, pid, is_alive, timeout):
            return

        logger.warning(
            "virtiofs-server %s pid %s did not stop after SIGTERM", tag, pid)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return

        self._wait_virtiofs_stopped(tag, pid, is_alive, timeout)

    def _wait_virtiofs_stopped(self, tag, pid, is_alive, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._virtiofs_pid_alive(pid, is_alive):
                return True

            time.sleep(0.1)

        logger.warning("virtiofs-server %s pid %s is still alive", tag, pid)
        return False

    def _virtiofs_pid_alive(self, pid, is_alive):
        if is_alive:
            return is_alive()

        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False

        return True

    def _unlink_virtiofs_files(self, socket_path):
        self._unlink_path(socket_path)
        self._unlink_path("{}.pid".format(socket_path))

    def _unlink_path(self, path):
        try:
            os.unlink(path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    def _wait_virtiofs_socket(self, tag, socket_path, virtiofs, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not virtiofs.daemon.is_alive():
                raise RuntimeError(
                    "virtiofs-server {} exited before creating socket {}".format(
                        tag,
                        socket_path))

            try:
                mode = os.stat(socket_path).st_mode
                if stat.S_ISSOCK(mode):
                    return
            except OSError:
                pass

            time.sleep(0.1)

        raise RuntimeError(
            "virtiofs-server {} did not create socket {} in {} seconds".format(
                tag,
                socket_path,
                timeout))
