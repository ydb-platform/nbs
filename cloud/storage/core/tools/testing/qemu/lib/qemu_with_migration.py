import atexit
import os
import time

import yatest.common as common

from cloud.storage.core.tools.testing.virtiofs_server.lib import VirtioFsServerSet

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

VIRTIOFS_SERVER_BINARY = (
    "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")


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
        self.virtiofs_servers = VirtioFsServerSet(
            common.binary_path(VIRTIOFS_SERVER_BINARY),
            common.output_path(),
            env_get=self._get_env,
            env_set=self._set_env)
        atexit.register(self.virtiofs_servers.stop_all)

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
        self.virtiofs_servers.restart(self.qemu.mount_paths, self.qemu.seqno)

    def _get_env(self, key):
        return recipe_get_env(key, self.qemu.inst_index)

    def _set_env(self, key, value):
        os.environ[env_with_guest_index(key, self.qemu.inst_index)] = value
