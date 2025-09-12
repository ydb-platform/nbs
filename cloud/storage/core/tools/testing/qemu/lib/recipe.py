import os
import signal
import logging
import argparse
import yatest.common
import yatest.common.network
import retrying
import shlex
import tarfile

import library.python.fs as fs
from library.python.retry import retry
import library.python.testing.recipe

from .qemu import Qemu
from .common import SshToGuest, get_mount_paths, env_with_guest_index
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

logger = logging.getLogger(__name__)
pm = yatest.common.network.PortManager()


SSH_PORT = 22

QEMU_NET = "10.0.2.0/24"
QEMU_HOST = "10.0.2.2"

PID_FILE = "qemu.pid"
ERR_LOG_FILE_NAMES_FILE = "qemu.err_log_files"


class QemuKvmRecipeException(Exception):

    def __init__(self, msg):
        super(QemuKvmRecipeException, self).__init__(
            "[[bad]]{}[[rst]]".format(msg))


def _start(argv):
    args = _parse_args(argv)
    if args.instance_count > 1:
        args.shared_nic_port = pm.get_port()
    else:
        args.shared_nic_port = 0

    for inst_index in range(args.instance_count):
        start_instance(args, inst_index)


# Retry is a workaround for the issue when ssh onto the qemu instance fails
# with "Connection timed out during banner exchange". See CLOUD-142732
@retry(max_times=10)
def start(argv):
    try:
        logger.info("Trying to start qemu")
        _start(argv)
    except Exception as e:
        logger.exception("Failed to start qemu")
        try:
            logger.info("Trying to stop qemu")
            stop(argv)
        except Exception:
            logger.exception("Failed to stop qemu")
        raise e


def start_instance(args, inst_index):
    logger.info("start instance with index = {}".format(inst_index))
    virtio = _get_vm_virtio(args)
    mount_paths = get_mount_paths(inst_index=inst_index)

    vhost_sockets = []
    nbs_devices_count = int(os.getenv("VIRTIOFS_SERVER_COUNT"))
    for nbs_index in range(nbs_devices_count):
        if virtio == "fs":
            vhost_sockets.append(recipe_get_env("NFS_VHOST_SOCKET", nbs_index))
        elif virtio == "blk":
            vhost_sockets.append(recipe_get_env("NBS_VHOST_SOCKET", nbs_index))

    use_virtiofs_server = _get_vm_use_virtiofs_server(args)

    qemu = Qemu(qemu_kmv=_get_qemu_kvm(args),
                qemu_firmware=_get_qemu_firmware(args),
                rootfs=_get_rootfs(args),
                kernel=_get_kernel(args),
                kcmdline=_get_kcmdline(args),
                initrd=_get_initrd(args),
                mem=_get_vm_mem(args),
                proc=_get_vm_proc(args),
                virtio=virtio,
                qemu_options=_get_qemu_options(args),
                vhost_socket=None,
                enable_kvm=_get_vm_enable_kvm(args),
                inst_index=inst_index,
                shared_nic_port=args.shared_nic_port,
                use_virtiofs_server=use_virtiofs_server,
                vhost_sockets=vhost_sockets)

    qemu.set_mount_paths(mount_paths)
    qemu.set_ssh_port(pm.get_port())
    qemu.start()

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, qemu.qemu_bin.stderr_file_name)

    with open(PID_FILE, "a+") as f:
        print(qemu.qemu_bin.daemon.process.pid, file=f)

    ssh = SshToGuest(user=_get_ssh_user(args),
                     port=qemu.get_ssh_port(),
                     key=_get_ssh_key(args))

    recipe_set_env("QEMU_FORWARDING_PORT", str(ssh.port), inst_index)

    _wait_ssh(qemu, ssh)

    for tag, path, _ in mount_paths:
        ssh("sudo mkdir -p {path}".format(path=path))
        if use_virtiofs_server:
            ssh("sudo mount -t virtiofs {tag} {path}".format(tag=tag, path=path))
        else:
            ssh("sudo mount -t 9p {tag} {path} -o trans=virtio,version=9p2000.L,cache=loose,rw".format(
                tag=tag, path=path))

        # the code below fixes situation where /home is a link to /place/home and source and build roots can come
        # in both ways, e.g. build root can be in a form of /place/home/... and source root /home/...
        # to fix it, we create links if the real path is different from the given path
        real_path = os.path.realpath(path)
        if real_path != path:
            ssh("sudo mkdir -p {} && sudo ln -s {} {}".format(
                os.path.dirname(real_path), path, real_path))

    _prepare_test_environment(ssh, virtio, inst_index)

    if args.invoke_test:
        recipe_set_env("TEST_COMMAND_WRAPPER",
                       " ".join(ssh.get_command("sudo /run_test.sh")),
                       inst_index)

    ready_flag_path = recipe_get_env("QEMU_SET_READY_FLAG", inst_index)
    if ready_flag_path is not None:
        open(ready_flag_path, 'a')


def stop(argv):
    with open(PID_FILE, "r") as f:
        pids = f.read().strip().splitlines()
        logger.info('Stopping qemu instances with pids: %s', ', '.join(pids))
        for pid in pids:
            logger.info("will kill process with pid `%s`", pid)
            try:
                os.kill(int(pid), signal.SIGTERM)
            except OSError:
                logger.exception("While killing pid `%s`", pid)
                raise
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError(
            "Errors occurred during qemu-kvm execution: {}".format(errors)
        )


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--qemu-bin", help="Path to qemu executable (Arcadia related)")
    parser.add_argument(
        "--qemu-firmware", help="Path to qemu firmware directory (Arcadia related)")
    parser.add_argument(
        "--kernel", help="Path to kernel image (Arcadia related)")
    parser.add_argument(
        "--kcmdline", help="Path to kernel cmdline config (Arcadia related)")
    parser.add_argument(
        "--initrd", help="Path to initrd image (Arcadia related)")
    parser.add_argument(
        "--rootfs", help="Path to rootfs image (Arcadia related)")
    parser.add_argument(
        "--mem", help="Amount of memory for running VM (default is 2G)")
    parser.add_argument(
        "--proc", help="Amount of processors for running VM (default is 2)")
    parser.add_argument(
        "--ssh-key", help="Path to ssh key to access VM (Arcadia related)")
    parser.add_argument("--ssh-user", help="ssh user name to access VM")
    parser.add_argument("--qemu-options", help="Custom options for qemu")
    parser.add_argument("--virtio", default="none")
    parser.add_argument("--enable-kvm", default="True")
    parser.add_argument(
        "--instance-count", help="Number of qemu instances to start")
    parser.add_argument("--invoke-test", default="Invoke test from qemu after starting")
    parser.add_argument("--use-virtiofs-server", default="False")

    args = parser.parse_args(argv)
    if args.instance_count == "$QEMU_INSTANCE_COUNT":
        args.instance_count = 1
    else:
        args.instance_count = int(args.instance_count)

    if args.invoke_test == "$QEMU_INVOKE_TEST":
        args.invoke_test = True
    else:
        args.invoke_test = _str_to_bool(args.invoke_test)

    return args


def _get_bindir():
    return yatest.common.build_path(
        "cloud/storage/core/tools/testing/qemu/bin")


def _unpack_qemu_bin(bindir):
    with tarfile.open(os.path.join(bindir, "qemu-bin.tar.gz")) as tf:
        tf.extractall(bindir)


def _get_qemu_kvm(args):
    if not args.qemu_bin or args.qemu_bin == "$QEMU_BIN":
        # QEMU_BIN not defined externally, use the one from the resource
        bindir = _get_bindir()
        qemu_kvm = os.path.join(bindir, "usr", "bin", "qemu-system-x86_64")
        if not os.path.exists(qemu_kvm):
            _unpack_qemu_bin(bindir)
    else:
        qemu_kvm = yatest.common.build_path(args.qemu_bin)
        if not os.path.exists(qemu_kvm):
            qemu_kvm = yatest.common.source_path(args.qemu_bin)

    if not os.path.exists(qemu_kvm):
        raise QemuKvmRecipeException(
            "cannot find qemu-kvm binary by path '{}'".format(qemu_kvm))

    return qemu_kvm


def _get_qemu_firmware(args):
    if not args.qemu_firmware or args.qemu_firmware == "$QEMU_FIRMWARE":
        # QEMU_FIRMWARE not defined externally, use the one from the resource
        bindir = _get_bindir()
        qemu_firmware = os.path.join(bindir, "usr", "share", "qemu")
        if not os.path.exists(qemu_firmware):
            _unpack_qemu_bin(bindir)
    else:
        qemu_firmware = yatest.common.build_path(args.qemu_firmware)

    if not os.path.exists(qemu_firmware):
        raise QemuKvmRecipeException(
            "cannot find qemu firmware directory by path '{}'".format(qemu_firmware))

    return qemu_firmware


def _get_rootfs(args):
    if args.rootfs == "$QEMU_ROOTFS":
        raise QemuKvmRecipeException(
            "Rootfs image is not set for the recipe, please, follow the docs to know how to fix it")
    rootfs = yatest.common.build_path(args.rootfs)
    if not os.path.exists(rootfs):
        rootfs = yatest.common.source_path(args.rootfs)

    if not os.path.exists(rootfs):
        raise QemuKvmRecipeException(
            "Cannot find rootfs image by '{}'".format(rootfs))
    return rootfs


def _get_kernel(args):
    if not args.kernel or args.kernel == "$QEMU_KERNEL":
        return None

    kernel = yatest.common.build_path(args.kernel)
    if not os.path.exists(kernel):
        kernel = yatest.common.source_path(args.kernel)

    if not os.path.exists(kernel):
        raise QemuKvmRecipeException(
            "Cannot find kernel image by '{}'".format(kernel))
    return kernel


def _get_kcmdline(args):
    if args.kcmdline is None or args.kcmdline == "$QEMU_KCMDLINE":
        return None

    kcmdline = yatest.common.build_path(args.kcmdline)
    if not os.path.exists(kcmdline):
        kcmdline = yatest.common.source_path(args.kcmdline)

    if not os.path.exists(kcmdline):
        raise QemuKvmRecipeException(
            "Cannot find kcmdline image by '{}'".format(kcmdline))

    return open(kcmdline).read().strip()


def _get_initrd(args):
    if not args.initrd or args.initrd == "$QEMU_INITRD":
        return None
    initrd = yatest.common.build_path(args.initrd)
    if not os.path.exists(initrd):
        raise QemuKvmRecipeException(
            "Cannot find initrd image by '{}'".format(initrd))
    return initrd


def _get_vm_mem(args):
    if args.mem == "$QEMU_MEM":
        return "4G"
    return args.mem


def _get_vm_proc(args):
    if args.proc == "$QEMU_PROC":
        return "8"
    return args.proc


def _get_vm_virtio(args):
    if args.virtio == "$QEMU_VIRTIO":
        return "none"
    return args.virtio


def _str_to_bool(s):
    return s.lower() in ['true', '1', 't', 'y', 'yes']


def _get_vm_enable_kvm(args):
    if args.enable_kvm == "$QEMU_ENABLE_KVM":
        return True
    return _str_to_bool(args.enable_kvm)


def _get_qemu_options(args):
    if args.qemu_options == "$QEMU_OPTIONS":
        return []
    return args.qemu_options.split()


def _get_nbs_device_path(instance_idx):
    disk_id = recipe_get_env("NBS_DISK_ID", instance_idx)

    if not disk_id:
        raise QemuKvmRecipeException("Cannot determine nbs disk id")

    return "/dev/disk/by-id/virtio-{}".format(disk_id)


def _get_ssh_key(args):
    if args.ssh_key == "$QEMU_SSH_KEY":
        raise QemuKvmRecipeException(
            "ssh key is not set for the recipe, please, foolow the docs to know how to fix it")
    ssh_key = yatest.common.build_path(args.ssh_key)
    if not os.path.exists(ssh_key):
        ssh_key = yatest.common.source_path(args.ssh_key)
    if not os.path.exists(ssh_key):
        raise QemuKvmRecipeException(
            "Cannot find ssh key in either build or source root by '{}'".format(args.ssh_key))
    new_ssh_key = yatest.common.work_path(os.path.basename(ssh_key))
    fs.copy_file(ssh_key, new_ssh_key)
    os.chmod(new_ssh_key, 0o0600)
    library.python.testing.recipe.set_env("QEMU_SSH_KEY", new_ssh_key)
    return new_ssh_key


def _get_vm_use_virtiofs_server(args):
    if args.use_virtiofs_server == "$QEMU_USE_VIRTIOFS_SERVER":
        return False
    return _str_to_bool(args.use_virtiofs_server)


def _prepare_test_environment(ssh, virtio, instance_idx):
    # Workaround for DISKMAN-63
    if "TMPDIR" in os.environ:
        ssh("sudo mkdir -m a=rwx -p {}".format(os.environ['TMPDIR']))

    vm_env = os.environ
    vm_env['TEST_ENV_WRAPPER'] = ' '.join(ssh.get_command(""))

    vm_env["INST_INDEX"] = str(instance_idx)

    if virtio == "fs":
        # Mount virtiofs directory
        mount_path = "/mnt/fs0"
        vm_env["NFS_MOUNT_PATH"] = mount_path
        library.python.testing.recipe.set_env("NFS_MOUNT_PATH", mount_path)

        ssh("sudo mkdir -p {}".format(mount_path))
        ssh("sudo mount -t virtiofs fs0 {} -o rw".format(mount_path),
            timeout=300)

        # Sanity check
        ssh("sudo touch {}/.test".format(mount_path), timeout=300)
    elif virtio == "blk":
        if os.getenv("NBS_INSTANCE_COUNT") == None:
            nbs_devices_count = 1
        else:
            nbs_devices_count = int(os.getenv("NBS_INSTANCE_COUNT"))

        for nbs_index in range(nbs_devices_count):
            # Use virtio-blk device
            ssh("sudo lsblk")
            vm_env[env_with_guest_index("NBS_DEVICE_PATH", nbs_index)] = _get_nbs_device_path(nbs_index)
            recipe_set_env(
                "NBS_DEVICE_PATH", _get_nbs_device_path(nbs_index), nbs_index)
    elif virtio == "nfs":
        # Mount NFS share
        mount_path = "/mnt/nfs{}".format(instance_idx)
        vm_env[env_with_guest_index("NFS_MOUNT_PATH", instance_idx)] = mount_path
        recipe_set_env("NFS_MOUNT_PATH", mount_path, instance_idx)
        nfs_port = os.getenv("NFS_GANESHA_PORT")

        ssh("sudo mkdir -p {}".format(mount_path))
        ssh("sudo mount -t nfs -o proto=tcp,port={} {}:/ {}".format(nfs_port, QEMU_HOST, mount_path),
            timeout=300)

        # Sanity check
        ssh("sudo touch {}/.test".format(mount_path), timeout=300)
    elif virtio == "none":
        # nothing to do
        pass
    else:
        raise QemuKvmRecipeException("Invalid virtio type")

    library.python.testing.recipe.set_env(
        "TEST_ENV_WRAPPER", vm_env['TEST_ENV_WRAPPER'])

    run_test_sh = '\n'.join(
        [
            "#!/bin/bash"
        ] + [
            "export {}={}".format(k, shlex.quote(v))
            for k, v in vm_env.items()
        ] + [
            '"$@"',
            "exit_code=$?",
            "sync",
            "exit $exit_code"
        ]
    )
    ssh("sudo tee /run_test.sh <<'RUN-TEST' && sudo chmod +x /run_test.sh\n{}\nRUN-TEST".format(run_test_sh))


def _get_ssh_user(args):
    if args.ssh_user == "$QEMU_SSH_USER":
        raise QemuKvmRecipeException(
            "ssh user is not set for the recipe, please, follow the docs to know how to fix it")
    return args.ssh_user


def _should_retry(exception):
    return not isinstance(exception, QemuKvmRecipeException)


@retrying.retry(stop_max_delay=360000, wait_fixed=1000, retry_on_exception=_should_retry)
def _wait_ssh(daemon, ssh):
    if not daemon.qemu_bin.is_alive():
        raise QemuKvmRecipeException("qemu is dead")

    ssh("exit 0")


def recipe_set_env(key, val, guest_index=0):
    library.python.testing.recipe.set_env(env_with_guest_index(key, guest_index), val)


def recipe_get_env(key, guest_index=0):
    return os.getenv(env_with_guest_index(key, guest_index))
