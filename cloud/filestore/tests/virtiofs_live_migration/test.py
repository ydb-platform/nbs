import contextlib
import os
import pathlib
import shlex
import shutil
import time
import uuid

import pytest
import yatest.common as common
import yatest.common.network

from cloud.filestore.config.vhost_pb2 import (
    TServiceEndpoint,
    TVhostAppConfig,
    TVhostServiceConfig,
)
from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.common import wait_for
from cloud.filestore.tests.python.lib.daemon_config import (
    FilestoreVhostConfigGenerator,
)
from cloud.filestore.tests.python.lib.vhost import (
    FilestoreVhost,
    wait_for_filestore_vhost,
)
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType
from cloud.storage.core.tools.testing.qemu.lib.common import (
    SshToGuest,
    get_qemu_firmware,
    get_qemu_kvm,
    is_arm as is_arm_host,
)
from cloud.storage.core.tools.testing.qemu.lib.qemu import Qemu


CLIENT_ID = "localhost@0"
VHOST_QUEUE_COUNT = 9
QEMU_REQUEST_QUEUE_COUNT = VHOST_QUEUE_COUNT - 1
QEMU_MEMORY = "2G"
QEMU_SMP = 16
TEST_TIMEOUT_SECONDS = 300
GUEST_USER = "qemu"
VIRTIOFS_TAG = "fs0"
GUEST_MOUNT_PATH = "/mnt/{}".format(VIRTIOFS_TAG)
DEFAULT_QEMU_ROOTFS = \
    "cloud/storage/core/tools/testing/qemu/image-noble/rootfs.img"
DEFAULT_QEMU_SSH_KEY = "cloud/storage/core/tools/testing/qemu/keys/id_rsa"
FIO_WRITE_OUTPUT_PATH = "/tmp/fio-write.out"
FIO_WRITE_RC_PATH = "/tmp/fio-write.rc"
FIO_VERIFY_OUTPUT_PATH = "/tmp/fio-verify.out"
FIO_BLOCK_SIZE = "32k"
FIO_SIZE = "1G"
FIO_NUMJOBS = 8
FIO_NRFILES = 8
FIO_TIMEOUT_SECONDS = 3600


def get_ssh_key(run_id):
    ssh_key = common.source_path(DEFAULT_QEMU_SSH_KEY)
    if not os.path.exists(ssh_key):
        raise RuntimeError("cannot find default qemu ssh private key")

    prepared_ssh_key = common.work_path("qemu-ssh-key.{}".format(run_id))
    shutil.copy(ssh_key, prepared_ssh_key)
    os.chmod(prepared_ssh_key, 0o0600)

    return prepared_ssh_key


def filestore_client(vhost_port=None):
    return FilestoreCliClient(
        common.binary_path("cloud/filestore/apps/client/filestore-client"),
        os.getenv("NFS_SERVER_PORT"),
        vhost_port=vhost_port,
        verbose=True,
        cwd=common.output_path())


def create_filestore(filesystem_id):
    filestore_client().create(filesystem_id,
                              "test_cloud",
                              "test_folder")


def destroy_filestore(filesystem_id):
    try:
        filestore_client().destroy(filesystem_id)
    except Exception as e:
        print("failed to destroy filestore {}: {}".format(
            filesystem_id,
            e), flush=True)


def tmp_socket_path(run_id, kind, identifier):
    return os.path.abspath(os.path.join(
        "/tmp",
        "qemu-{}-{}.{}".format(kind, identifier, run_id)))


def migration_file_path():
    return "{}/migration-check.txt".format(GUEST_MOUNT_PATH)


def fio_directory_path():
    return "{}/fio".format(GUEST_MOUNT_PATH)


def read_migration_file(ssh):
    command = "sudo cat {}".format(migration_file_path())
    return execute_command(ssh, command)


def mount_virtiofs(ssh):
    execute_command(ssh, "sudo mkdir -p {}".format(GUEST_MOUNT_PATH))
    execute_command(ssh, "sudo mount -t virtiofs {} {} -o rw".format(
        VIRTIOFS_TAG,
        GUEST_MOUNT_PATH))


def create_migration_file(ssh, migration_text):
    write_command = "printf %s {} | sudo tee {} >/dev/null && sync".format(
        shlex.quote(migration_text),
        migration_file_path())

    execute_command(ssh, write_command)
    assert read_migration_file(ssh) == migration_text
    print("Created {} with run id {}".format(
        migration_file_path(), migration_text.rstrip()), flush=True)


def build_fio_write_command():
    return " ".join([
        "fio",
        "--name=virtiofs-write",
        "--directory={}".format(fio_directory_path()),
        "--rw=randwrite",
        "--ioengine=io_uring",
        "--iodepth=32",
        "--direct=0",
        "--bs={}".format(FIO_BLOCK_SIZE),
        "--size={}".format(FIO_SIZE),
        "--numjobs={}".format(FIO_NUMJOBS),
        "--nrfiles={}".format(FIO_NRFILES),
        "--verify=crc32c",
        "--do_verify=0",
        "--verify_fatal=1",
        "--end_fsync=1",
        "--status-interval=10",
        "--output={}".format(FIO_WRITE_OUTPUT_PATH),
    ])


def start_fio_write(ssh):
    mount_virtiofs(ssh)
    prepare_command = "sudo mkdir -p {}".format(fio_directory_path())
    execute_command(ssh, prepare_command)

    fio_command = build_fio_write_command()
    fio_command_with_rc = "{}; echo $? > {}".format(
        fio_command,
        FIO_WRITE_RC_PATH)
    start_fio_command = "sudo nohup sh -c {} >/dev/null 2>&1 &".format(
        shlex.quote(fio_command_with_rc))

    execute_command(ssh, start_fio_command)


def fio_finished(ssh):
    command = "if test -f {}; then sudo cat {}; fi".format(
        FIO_WRITE_RC_PATH,
        FIO_WRITE_RC_PATH)
    rc = execute_command(ssh, command).strip()
    if not rc:
        return False

    assert rc == "0"
    return True


def verify_fio(ssh):
    fio_command = " ".join([
        "sudo",
        "fio",
        "--name=virtiofs-write",
        "--directory={}".format(fio_directory_path()),
        "--rw=read",
        "--bs={}".format(FIO_BLOCK_SIZE),
        "--size={}".format(FIO_SIZE),
        "--numjobs={}".format(FIO_NUMJOBS),
        "--nrfiles={}".format(FIO_NRFILES),
        "--ioengine=psync",
        "--direct=1",
        "--verify=crc32c",
        "--verify_only=1",
        "--verify_fatal=1",
        "--output={}".format(FIO_VERIFY_OUTPUT_PATH),
    ])
    result = ssh(fio_command, timeout=FIO_TIMEOUT_SECONDS)
    assert result.returncode == 0


def socket_to_uri(socket):
    return "unix:{}".format(socket)


def unlink_socket(socket):
    if socket:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(socket)


def generate_socket_path(request, test_ctx):
    socket = tmp_socket_path(test_ctx.run_id, "migration", uuid.uuid4().hex)
    request.addfinalizer(lambda: unlink_socket(socket))
    return socket


def create_qemu(vhost_socket, incoming_uri=None):
    qemu_options = []
    if not is_arm_host():
        qemu_options += ["-machine", "q35"]
    if incoming_uri:
        qemu_options += ["-incoming", incoming_uri]

    qemu = Qemu(
        qemu_kvm=get_qemu_kvm(),
        qemu_firmware=get_qemu_firmware(),
        rootfs=common.build_path(DEFAULT_QEMU_ROOTFS),
        kernel=None,
        kcmdline=None,
        initrd=None,
        mem=QEMU_MEMORY,
        proc=QEMU_SMP,
        virtio="fs",
        vhost_socket=vhost_socket,
        qemu_options=qemu_options,
        enable_kvm=True,
        num_request_queues=QEMU_REQUEST_QUEUE_COUNT)
    qemu.set_ssh_port(yatest.common.network.PortManager().get_port())
    qemu.set_mount_paths([])
    return qemu


def stop_qemu(qemu):
    if getattr(qemu, "_was_stopped", False):
        return

    qemu._was_stopped = True
    daemon = qemu.daemon
    try:
        qemu.stop()
    finally:
        try:
            if daemon is not None:
                daemon.stop()
        finally:
            unlink_socket(getattr(qemu, "qmp_socket", None))


def start_qemu(request, vhost_socket, incoming_uri=None):
    qemu = create_qemu(vhost_socket, incoming_uri=incoming_uri)
    qemu.start()
    request.addfinalizer(lambda: stop_qemu(qemu))
    return qemu


def ssh_to_guest(qemu, ssh_key):
    return SshToGuest(
        user=GUEST_USER,
        port=qemu.get_ssh_port(),
        key=ssh_key)


def wait_ssh(test_ctx, qemu):
    ssh = ssh_to_guest(qemu, test_ctx.ssh_key)
    deadline = time.time() + TEST_TIMEOUT_SECONDS
    last_error = None
    while time.time() < deadline:
        try:
            ssh("true")
            return ssh
        except Exception as e:
            last_error = e
            time.sleep(1)

    raise RuntimeError("guest ssh is not ready: {}".format(last_error))


def execute_command(ssh, command):
    result = ssh(command, timeout=300)
    assert result.returncode == 0
    return result.stdout.decode("utf-8")


def wait_file_exists(path):
    if not wait_for(
            lambda: os.path.exists(path),
            timeout_seconds=TEST_TIMEOUT_SECONDS):
        raise RuntimeError("file is not ready: {}".format(path))


def migrate_qemu(src, dst_migration_socket):
    uri = socket_to_uri(dst_migration_socket)
    print("Migrating {} -> {}".format(
        src.qmp.get_socket_path(),
        uri), flush=True)

    wait_file_exists(dst_migration_socket)
    src.migrate_to_uri(uri)
    print("Migration status: completed", flush=True)


def start_vhost_endpoint(request, test_ctx, port, mount_seqno, client_id):
    socket = tmp_socket_path(test_ctx.run_id, "vfs", uuid.uuid4().hex)

    filestore_client(port).start_endpoint(
        test_ctx.filesystem_id,
        socket,
        mount_seqno,
        readonly=False,
        persistent=True,
        client_id=client_id,
        vhost_queue_count=VHOST_QUEUE_COUNT)

    request.addfinalizer(lambda: stop_vhost_endpoint(test_ctx, port, socket))
    return socket


def stop_vhost_endpoint(test_ctx, port, socket):
    if socket in test_ctx.stopped_vhost_sockets:
        return

    try:
        filestore_client(port).stop_endpoint(socket)
    except Exception as e:
        print("failed to stop endpoint {}: {}".format(socket, e), flush=True)
    finally:
        test_ctx.stopped_vhost_sockets.add(socket)
        unlink_socket(socket)


def start_vhost(request, run_id, name):
    endpoint_storage_dir = os.path.join(
        common.work_path(),
        "endpoints-{}-{}".format(name, run_id))
    pathlib.Path(endpoint_storage_dir).mkdir(parents=True, exist_ok=True)

    config = TVhostAppConfig()
    config.VhostServiceConfig.CopyFrom(TVhostServiceConfig())
    config.VhostServiceConfig.EndpointStorageType = \
        EEndpointStorageType.ENDPOINT_STORAGE_FILE
    config.VhostServiceConfig.EndpointStorageDir = endpoint_storage_dir
    config.VhostServiceConfig.ServiceEndpoints.append(TServiceEndpoint())

    configurator = FilestoreVhostConfigGenerator(
        binary_path=common.binary_path(
            "cloud/filestore/apps/vhost/filestore-vhost"),
        app_config=config,
        service_type="kikimr",
        verbose=True,
        kikimr_port=os.getenv("KIKIMR_SERVER_PORT"),
        domain=os.getenv("NFS_DOMAIN"))

    for config_name in [
        "diag.txt",
        "domains.txt",
        "dyn_ns.txt",
        "log.txt",
        "names.txt",
        "storage-nolocal.txt",
        "sys.txt",
    ]:
        shutil.copy(
            os.path.join(os.getenv("NFS_CONFIG_DIR"), config_name),
            os.path.join(configurator.configs_dir, config_name))

    vhost = FilestoreVhost(configurator)
    vhost.start()
    request.addfinalizer(vhost.stop)
    wait_for_filestore_vhost(vhost, configurator.port)

    return configurator.port


class VirtiofsLiveMigrationTestCtx(object):
    def __init__(self):
        self.run_id = uuid.uuid4().hex
        self.filesystem_id = "virtiofs_live_migration_{}".format(self.run_id)
        self.ssh_key = get_ssh_key(self.run_id)
        self.vhost_ports = None
        self.stopped_vhost_sockets = set()


@pytest.fixture
def test_ctx(request):
    ctx = VirtiofsLiveMigrationTestCtx()
    create_filestore(ctx.filesystem_id)
    request.addfinalizer(lambda: destroy_filestore(ctx.filesystem_id))
    ctx.vhost_ports = (
        start_vhost(request, ctx.run_id, "src"),
        start_vhost(request, ctx.run_id, "dst"))
    return ctx


def test_virtiofs_live_migration_simple(request, test_ctx):
    src_port, dst_port = test_ctx.vhost_ports
    migration_text = "{}\n".format(test_ctx.run_id)

    src_socket = start_vhost_endpoint(request, test_ctx, src_port, 0, CLIENT_ID)
    print("Source vhost socket: {}".format(src_socket), flush=True)

    active_qemu = start_qemu(request, src_socket)
    active_ssh = wait_ssh(test_ctx, active_qemu)
    mount_virtiofs(active_ssh)
    create_migration_file(active_ssh, migration_text)

    dst_socket = start_vhost_endpoint(request, test_ctx, dst_port, 1, CLIENT_ID)
    print("Destination vhost socket: {}".format(dst_socket), flush=True)

    dst_migration_socket = generate_socket_path(request, test_ctx)
    target_qemu = start_qemu(
        request,
        dst_socket,
        incoming_uri=socket_to_uri(dst_migration_socket))

    migrate_qemu(active_qemu, dst_migration_socket)
    stop_qemu(active_qemu)
    stop_vhost_endpoint(test_ctx, src_port, src_socket)

    active_qemu = target_qemu

    active_ssh = wait_ssh(test_ctx, active_qemu)
    assert read_migration_file(active_ssh) == migration_text
    print("Verified {} after migration".format(
        migration_file_path()), flush=True)

    stop_qemu(active_qemu)
    stop_vhost_endpoint(test_ctx, dst_port, dst_socket)


def test_virtiofs_live_migration_fio(request, test_ctx):
    src_port, dst_port = test_ctx.vhost_ports
    mount_seq_no = 0
    migration_count = 0

    print("Starting fio migration test", flush=True)

    src_socket = start_vhost_endpoint(request, test_ctx, src_port, mount_seq_no, CLIENT_ID)
    active_qemu = start_qemu(request, src_socket)
    active_ssh = wait_ssh(test_ctx, active_qemu)
    start_fio_write(active_ssh)
    mount_seq_no += 1

    deadline = time.time() + FIO_TIMEOUT_SECONDS
    while not fio_finished(active_ssh):
        if time.time() >= deadline:
            raise RuntimeError("fio did not finish in time")

        dst_socket = start_vhost_endpoint(request, test_ctx, dst_port, mount_seq_no, CLIENT_ID)
        dst_migration_socket = generate_socket_path(request, test_ctx)
        target_qemu = start_qemu(
            request,
            dst_socket,
            incoming_uri=socket_to_uri(dst_migration_socket))
        mount_seq_no += 1

        migrate_qemu(active_qemu, dst_migration_socket)
        migration_count += 1
        stop_qemu(active_qemu)
        stop_vhost_endpoint(test_ctx, src_port, src_socket)

        active_qemu = target_qemu
        src_socket = dst_socket
        src_port, dst_port = dst_port, src_port
        active_ssh = wait_ssh(test_ctx, active_qemu)

    if migration_count == 0:
        raise RuntimeError("fio finished before first migration")

    print("Completed {} fio migrations before fio finished".format(migration_count), flush=True)
    verify_fio(active_ssh)
    stop_qemu(active_qemu)
    stop_vhost_endpoint(test_ctx, src_port, src_socket)
