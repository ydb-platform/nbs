import atexit
import logging
import os
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
            VirtioFsServer.cleanup_socket_files(socket_path)

            logger.info("restart virtiofs-server %s on %s", tag, socket_path)
            virtiofs = VirtioFsServer(
                common.binary_path(VIRTIOFS_SERVER_BINARY),
                socket_path,
                path)
            virtiofs.start(
                common.output_path(),
                "{}-migration-{}".format(tag, self.qemu.seqno))
            virtiofs.wait_for_socket(tag)

            self.virtiofs_servers[tag] = virtiofs
            os.environ[env_with_guest_index(
                "VIRTIOFS_PID_{}".format(tag),
                self.qemu.inst_index)] = str(virtiofs.pid)

    def _stop_virtiofs_server(self, tag):
        virtiofs = self.virtiofs_servers.pop(tag, None)
        if virtiofs:
            virtiofs.stop(tag)
            return

        pid = recipe_get_env(
            "VIRTIOFS_PID_{}".format(tag),
            self.qemu.inst_index)
        if not pid:
            return

        VirtioFsServer.stop_pid(tag, pid)

    def _stop_restarted_virtiofs_servers(self):
        for tag in list(self.virtiofs_servers):
            self._stop_virtiofs_server(tag)
