import os
import logging
import yatest.common as common
import time
import tarfile

from .common import get_mount_paths
from .qemu import Qemu

EMU_NET = "10.0.2.0/24"
QEMU_HOST = "10.0.2.2"

logger = logging.getLogger(__name__)


def _get_bindir():
    return common.build_path(
        "cloud/storage/core/tools/testing/qemu/bin")


def _unpack_qemu_bin(bindir):
    with tarfile.open(os.path.join(bindir, "qemu-bin.tar.gz")) as tf:
        tf.extractall(bindir)


def _get_qemu_kvm():
    bindir = _get_bindir()
    qemu_kvm = os.path.join(bindir, "usr", "bin", "qemu-system-x86_64")
    if not os.path.exists(qemu_kvm):
        _unpack_qemu_bin(bindir)

    return qemu_kvm


def _get_qemu_firmware():
    bindir = _get_bindir()
    qemu_firmware = os.path.join(bindir, "usr", "share", "qemu")
    if not os.path.exists(qemu_firmware):
        _unpack_qemu_bin(bindir)

    return qemu_firmware


class QemuWithMigration:
    def __init__(self, socket_generator):
        self.qemu = Qemu(
            qemu_kmv=_get_qemu_kvm(),
            qemu_firmware=_get_qemu_firmware(),
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
            use_virtiofs_server=True)

        self.socket_generator = socket_generator
        self.filestore_client = getattr(socket_generator, 'client', None)
        self.previous_socket = None

    def start(self):
        self.socket = self.socket_generator(0, False)
        self.previous_socket = self.socket

        self.qemu.set_mount_paths(get_mount_paths())
        self.qemu.set_vhost_socket(self.socket)
        self.qemu.start()

    def migrate(self, count, timeout):
        for migration in range(0, count):
            self.qemu._save_to_file()

            if self.previous_socket and self.filestore_client:
                try:
                    self.filestore_client.stop_endpoint(self.previous_socket)
                    time.sleep(0.5)
                except Exception as e:
                    logger.warning("Failed to stop previous endpoint %s: %s", self.previous_socket, e)

            self.socket = self.socket_generator(migration, False)
            self.previous_socket = self.socket

            self.qemu._restore_from_file(migration, self.socket)
            self.qemu.seqno += 1

            time.sleep(timeout)
