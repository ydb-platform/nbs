import json
import logging
import os
import stat
import time
import uuid
import yatest.common
import yatest.common.network

from contrib.ydb.tests.library.harness.daemon import Daemon
from .qmp import QmpClient

logger = logging.getLogger(__name__)

SSH_PORT = 22

QEMU_NET = "10.0.2.0/24"
QEMU_HOST = "10.0.2.2"


def daemon_log_files(prefix, id, cwd):
    files = [
        ("stdout_file", ".out"),
        ("stderr_file", ".err"),
    ]

    ret = {}
    for tag, suffix in files:
        name = os.path.abspath(os.path.join(cwd, prefix + '.' + str(id) + suffix))
        with open(name, mode='w'):
            pass

        ret[tag] = name

    return ret


def prepare_root_image(src_image, dest_path, backup):
    if backup is True:
        logger.info("copy image '{}' to {}".format(
            src_image,
            dest_path))
        os.system('cp {} {}'.format(src_image, dest_path))

        new_root_fs_image = dest_path + '/' + os.path.basename(src_image)
        os.chmod(new_root_fs_image, stat.S_IRWXU + stat.S_IRWXG + stat.S_IRWXO)
        return new_root_fs_image
    else:
        return src_image


def create_qmp_socket():
    filepath = '/tmp/{}'.format(uuid.uuid4())
    return filepath


def kvm_available():
    try:
        return os.path.exists("/dev/kvm")
    except Exception:
        return False


class QemuException(Exception):

    def __init__(self, msg):
        super(QemuException, self).__init__(
            "[[bad]]{}[[rst]]".format(msg))


class Qemu:
    def __init__(self,
                 qemu_kmv,
                 qemu_firmware,
                 rootfs,
                 kernel,
                 kcmdline,
                 initrd,
                 mem,
                 proc,
                 virtio,
                 vhost_socket,
                 qemu_options,
                 enable_kvm,
                 backup_rootfs=False,
                 inst_index=0,
                 shared_nic_port=0,
                 use_virtiofs_server=False):

        self.ssh_port = 0
        self.qmp = None
        self.mount_paths = None
        self.seqno = 0
        self.qemu_bin = None

        self.qemu_kmv = qemu_kmv
        self.qemu_firmware = qemu_firmware
        self.rootfs = rootfs
        self.kernel = kernel
        self.kcmdline = kcmdline
        self.initrd = initrd
        self.mem = mem
        self.proc = proc
        self.virtio = virtio
        self.qemu_options = qemu_options
        self.virtio_options = self._get_virtio_options(self.virtio, vhost_socket)
        self.enable_kvm = enable_kvm
        self.backup_rootfs = backup_rootfs
        self.inst_index = inst_index
        self.shared_nic_port = shared_nic_port
        self.use_virtiofs_server = use_virtiofs_server

    def prepare_mount_paths(self, ssh):
        for tag, path, _ in self.mount_paths:
            ssh("sudo mkdir -p {path}".format(path=path))
            if self.use_virtiofs_server:
                ssh("sudo mount -t virtiofs {tag} {path}".format(tag=tag, path=path))
            else:
                ssh("sudo mount -t  {tag} {path} -o trans=virtio,version=9p2000.L,cache=loose,rw".format(
                    tag=tag, path=path))

            # the code below fixes situation where /home is a link to /place/home and source and build roots can come
            # in both ways, e.g. build root can be in a form of /place/home/... and source root /home/...
            # to fix it, we create links if the real path is different from the given path
            real_path = os.path.realpath(path)
            if real_path != path:
                ssh("sudo mkdir -p {} && sudo ln -s {} {}".format(
                    os.path.dirname(real_path), path, real_path))

    def _get_virtio_options(self, virtio, vhost_socket):
        if virtio == "fs":
            return self._get_virtiofs_options(vhost_socket)
        elif virtio == "blk":
            return self._get_virtioblk_options(vhost_socket)
        elif virtio == "nfs" or virtio == "none":
            return []
        else:
            raise QemuException("Invalid virtio type")

    def _get_virtiofs_options(self, vhost_socket):
        if vhost_socket is None:
            raise QemuException("Cannot find nfs vhost socket path")

        cmd = ["-chardev",
               "socket,id=vhost0,path={},reconnect=1".format(vhost_socket)]
        cmd += ["-device",
                "vhost-user-fs-pci,chardev=vhost0,id=vhost-user-fs0,tag=fs0,queue-size=512,migration=external"]
        return cmd

    def _get_virtioblk_options(self, vhost_socket):
        if vhost_socket is None:
            raise QemuException("Cannot find nbs vhost socket path")

        cmd = ["-chardev", "socket,id=vhost0,path={}".format(vhost_socket)]
        cmd += ["-device",
                "vhost-user-blk-pci,chardev=vhost0,id=vhost-user-blk0,num-queues=1"]
        return cmd

    def stop(self):
        if self.qmp is not None:
            self.qmp.close()

    def set_ssh_port(self, port=0):
        self.ssh_port = port

    def set_vhost_socket(self, socket):
        self.virtio_options = self._get_virtio_options(self.virtio, socket)

    def set_mount_paths(self, mount_paths):
        self.mount_paths = mount_paths

    def _save_to_file(self):
        self.qmp.command("migrate", uri="exec:cat > state_file_{}.bin".format(self.seqno))

        status = self.qmp.command("query-migrate")
        logger.info("migrate_status {}".format(json.dumps(status)))
        while status['status'] in ('active', 'setup', 'device'):
            time.sleep(1)
            status = self.qmp.command("query-migrate")
            logger.info("migrate_status {}".format(json.dumps(status)))

        if status['status'] != "completed":
            raise self.QemuException(status['status'])

        self.qmp.close()
        self.qemu_bin.kill()

    def _restore_from_file(self, id, vhost_socket):
        self.virtio_options = self._get_virtio_options(self.virtio, vhost_socket)
        cmd = self._create_cmd() + ["-incoming", "defer"]

        self.qemu_bin = Daemon(
            cmd,
            yatest.common.work_path(),
            timeout=180,
            **daemon_log_files(prefix="qemu-bin", id=id, cwd=yatest.common.output_path()))
        self.qemu_bin.start()

        self.qmp = QmpClient(self.qmp_socket)

        self.qmp.command("migrate-incoming",
                         uri="exec:cat state_file_{}.bin".format(self.seqno))

        status = self.qmp.command("query-status")
        while status['status'] == 'inmigrate':
            time.sleep(1)
            status = self.qmp.command("query-status")
        self.qmp.command("cont")

    def migrate(self, id, vhost_socket):
        self._save_to_file()
        self._restore_from_file(id, vhost_socket)
        self.seqno += 1

    def _create_cmd(self):
        qemu_serial_log = yatest.common.output_path(
            os.path.basename(self.rootfs) + "_{}_serial.out".format(self.inst_index))

        self.qmp_socket = create_qmp_socket()

        cmd = [
            self.qemu_kmv,
            "-nodefaults",
            "-msg", "timestamp=on",
            "-smp", str(self.proc),
            "-m", str(self.mem),
            "-object", "memory-backend-memfd,id=mem,size={},share=on".format(self.mem),
            "-numa", "node,memdev=mem",
            "-netdev", "user,id=netdev0,net={},host={},hostfwd=tcp::{}-:{}".format(QEMU_NET, QEMU_HOST, self.ssh_port, SSH_PORT),
            "-device", "virtio-net-pci,netdev=netdev0,id=net0",
            "-device", "virtio-rng-pci",
            "-serial", "file:{}".format(qemu_serial_log),
            "-nographic",
            "-drive", "format=qcow2,file={},id=hdd0,if=none,aio=native,cache=none".format(self.new_root_fs_image),
            "-device", "virtio-blk-pci,id=vblk0,drive=hdd0,num-queues={},bootindex=1".format(self.proc),
            "-L", self.qemu_firmware,
            "-qmp", "unix:{},server,nowait".format(self.qmp_socket),
        ]

        if self.shared_nic_port:
            nic_mac = "52:54:00:12:56:{:02x}".format(self.inst_index)
            if self.inst_index == 0:
                sock_arg = f"listen=:{self.shared_nic_port}"
            else:
                sock_arg = f"connect=:{self.shared_nic_port}"

            cmd += [
                "-netdev", "socket,id=netdev1,{}".format(sock_arg),
                "-device", "virtio-net-pci,netdev=netdev1,id=net1,mac={}".format(nic_mac),
            ]

        if self.backup_rootfs is False:
            cmd += ["-snapshot"]

        if self.enable_kvm:
            cmd += ["-cpu", "host", "-enable-kvm"]
        else:
            cmd += ["-cpu", "max"]

        # Place custom options before virtfs so custom devices will have predictable pci buses
        cmd += self.qemu_options

        if self.virtio_options:
            cmd += self.virtio_options

        for tag, path, vhost_socket in self.mount_paths:
            if self.use_virtiofs_server:
                cmd += ["-chardev",
                        "socket,id={},path={},reconnect=1".format(tag, vhost_socket)]
                cmd += ["-device",
                        "vhost-user-fs-pci,chardev={},id=vhost-user-{},tag={},queue-size=512,migration=external".format(tag, tag, tag)]
            else:
                cmd += ["-virtfs",
                        "local,path={path},mount_tag={tag},security_model=none".format(tag=tag, path=path)]

        if self.kernel:
            cmd += ["-kernel", self.kernel]
            if self.initrd:
                cmd += ["-initrd", self.initrd]
            if self.kcmdline:
                cmd += ["-append", self.kcmdline]

        return cmd

    @property
    def daemon(self):
        return self.qemu_bin

    def start(self):
        if self.qemu_bin is not None:
            return self.qemu_bin.daemon.process.pid

        if self.enable_kvm and not kvm_available():
            msg = "/dev/kvm is not available"
            logger.error(msg)
            raise QemuException(msg)

        self.new_root_fs_image = prepare_root_image(
            self.rootfs,
            yatest.common.output_path(),
            self.backup_rootfs)

        cmd = self._create_cmd()

        logger.info("launching qemu: '{}'".format(" ".join(cmd)))

        self.qemu_bin = Daemon(
            cmd,
            yatest.common.work_path(),
            timeout=180,
            **daemon_log_files(prefix="qemu-bin", id=0, cwd=yatest.common.output_path()))
        self.qemu_bin.start()

        self.qmp = QmpClient(self.qmp_socket)

        return self.qemu_bin.daemon.process.pid

    def get_ssh_port(self):
        if self.ssh_port != 0:
            return self.ssh_port

        ssh_port = None
        usernet_info = self.qmp.command("human-monitor-command",
                                        **{"command-line": "info usernet"})
        for line in usernet_info.splitlines():
            fields = line.split()
            if fields[0] == "TCP[HOST_FORWARD]" and fields[5] == "22":
                if ssh_port is not None:
                    raise Exception("Guest has multiple 22 ports")
                ssh_port = int(fields[3])

        self.ssh_port = ssh_port
        return self.ssh_port
