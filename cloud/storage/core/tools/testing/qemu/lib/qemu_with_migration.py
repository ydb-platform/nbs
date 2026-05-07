import logging
import yatest.common as common
import time

from .common import get_mount_paths, get_qemu_kvm, get_qemu_firmware, is_arm
from .qemu import Qemu

EMU_NET = "10.0.2.0/24"
QEMU_HOST = "10.0.2.2"

logger = logging.getLogger(__name__)


class QemuWithMigration:
    def __init__(self, socket_generator):
        self.qemu = Qemu(
            qemu_kvm=get_qemu_kvm(),
            qemu_firmware=get_qemu_firmware(),
            rootfs=common.build_path("cloud/storage/core/tools/testing/qemu/image/rootfs.img"),
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
            is_arm=is_arm())

        self.socket_generator = socket_generator

    def start(self):
        self.socket = self.socket_generator(0, False)

        self.qemu.set_mount_paths(get_mount_paths())
        self.qemu.set_vhost_socket(self.socket)
        self.qemu.start()

    def migrate(self, count, timeout):
        for migration in range(0, count):
            self.socket = self.socket_generator(migration, False)
            self.qemu.migrate(migration, self.socket)
            time.sleep(timeout)
