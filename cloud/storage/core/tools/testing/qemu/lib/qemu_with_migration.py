import os
import logging
import yatest.common as common
import time
import tarfile

import core.config

from .qemu import Qemu
from cloud.filestore.tests.python.lib.client import NfsCliClient, create_endpoint


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


def get_mount_paths():
    if "TEST_TOOL" in os.environ:
        test_tool_dir = os.path.dirname(os.environ["TEST_TOOL"])
    else:
        test_tool_dir = core.config.tool_root()

    if 'ASAN_SYMBOLIZER_PATH' in os.environ:
        toolchain = os.path.dirname(os.path.dirname(
            os.environ['ASAN_SYMBOLIZER_PATH']))

    mounts = [("source_path", common.source_path(), os.getenv("VIRTIOFS_SOCKET_{}".format("source_path"))),  # need to mound original source root as test environment has links into it
              ("build_path", common.build_path(), os.getenv("VIRTIOFS_SOCKET_{}".format("build_path"))),
              ("test_tool", test_tool_dir, os.getenv("VIRTIOFS_SOCKET_{}".format("test_tool"))),
              ("toolchain", toolchain, os.getenv("VIRTIOFS_SOCKET_{}".format("toolchain")))]
    if common.ram_drive_path():
        mounts.append(tuple(("tmpfs_path", common.ram_drive_path(), None)))
    return mounts


class QemuWithMigration:
    def __init__(self):
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
            enable_kvm=True)

        self.port = os.getenv("NFS_SERVER_PORT")
        self.vhost_port = os.getenv("NFS_VHOST_PORT")

        client_path = common.binary_path(
            "cloud/filestore/client/filestore-client")

        self.client = NfsCliClient(
            client_path,
            self.port,
            vhost_port=self.vhost_port,
            verbose=True,
            cwd=common.output_path())

    def start(self):
        self.socket = create_endpoint(
            self.client,
            "nfs_test",
            "/tmp",
            "test.vhost",
            os.getenv("NFS_VHOST_ENDPOINT_STORAGE_DIR", None),
            0,
            False)

        self.qemu.set_mount_paths(get_mount_paths())
        self.qemu.set_vhost_socket(self.socket)
        self.qemu.set_enable_migration(True)
        self.qemu.start()

    def migrate(self, count, timeout):
        for migration in range(0, count):
            self.socket = create_endpoint(
                self.client,
                "nfs_test",
                "/tmp",
                "test.vhost",
                os.getenv("NFS_VHOST_ENDPOINT_STORAGE_DIR", None),
                0,
                False)

            self.qemu.migrate(migration, self.socket)
            time.sleep(timeout)
