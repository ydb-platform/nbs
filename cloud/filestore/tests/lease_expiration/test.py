import logging
import os
import pathlib
import tarfile
import tempfile
import uuid

from retrying import retry

import library.python.fs as fs

import yatest.common as common

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType
from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest, get_mount_paths
from cloud.storage.core.tools.testing.qemu.lib.qemu import Qemu

from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.vhost_pb2 import \
    TVhostAppConfig, TVhostServiceConfig, TServiceEndpoint
from cloud.filestore.tests.python.lib.client import FilestoreCliClient, create_endpoint
from cloud.filestore.tests.python.lib.daemon_config import FilestoreServerConfigGenerator, FilestoreVhostConfigGenerator
from cloud.filestore.tests.python.lib.server import FilestoreServer, wait_for_filestore_server
from cloud.filestore.tests.python.lib.vhost import FilestoreVhost, wait_for_filestore_vhost

logger = logging.getLogger(__name__)

RETRY_COUNT = 100
WAIT_TIMEOUT = 1000  # 1sec


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def mkdir(ssh: SshToGuest, dir: str):
    return ssh("sudo mkdir -p {}".format(dir))


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def mount(ssh: SshToGuest, dir: str):
    return ssh("sudo mount -t virtiofs fs0 {} -o rw".format(dir))


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def create_file(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"sudo touch {dir}/{file_name}")


@retry(stop_max_attempt_number=RETRY_COUNT, wait_fixed=WAIT_TIMEOUT)
def touch_file_in_background(ssh: SshToGuest, dir: str, file_name: str):
    return ssh(f"tmux new-session -d 'while true; do sudo dd if=/dev/urandom of={dir}/{file_name} bs=1M count=1 oflag=direct status=none; done'")


def get_qemu_bindir():
    return common.build_path(
        "cloud/storage/core/tools/testing/qemu/bin")


def unpack_qemu_bindir(bindir):
    with tarfile.open(os.path.join(bindir, "qemu-bin.tar.gz")) as tf:
        tf.extractall(bindir)


def get_qemu_kvm():
    bindir = get_qemu_bindir()
    qemu_kvm = os.path.join(bindir, "usr", "bin", "qemu-system-x86_64")
    if not os.path.exists(qemu_kvm):
        unpack_qemu_bindir(bindir)

    return qemu_kvm


def get_qemu_firmware():
    bindir = get_qemu_bindir()
    qemu_firmware = os.path.join(bindir, "usr", "share", "qemu")
    if not os.path.exists(qemu_firmware):
        unpack_qemu_bindir(bindir)

    return qemu_firmware


class QemuWithWorkload:
    def __init__(self, vhost_socket):
        rootfs = common.build_path(
            "cloud/storage/core/tools/testing/qemu/image/rootfs.img"
        )
        self.qemu = Qemu(
            qemu_kmv=get_qemu_kvm(),
            qemu_firmware=get_qemu_firmware(),
            rootfs=rootfs,
            kernel=None,
            kcmdline=None,
            initrd=None,
            mem="4G",
            proc=8,
            virtio='fs',
            qemu_options=[],
            vhost_socket=vhost_socket,
            enable_kvm=True)

    def start(self):
        self.qemu.set_mount_paths(get_mount_paths())
        self.qemu.start()

        ssh_key = common.source_path(
            "cloud/storage/core/tools/testing/qemu/keys/id_rsa"
        )
        new_ssh_key = common.work_path(os.path.basename(ssh_key))
        fs.copy_file(ssh_key, new_ssh_key)
        os.chmod(new_ssh_key, 0o0600)

        ssh = SshToGuest(user="qemu",
                         port=self.qemu.get_ssh_port(),
                         key=new_ssh_key)

        virtiofs_mount_path = "/mnt/fs0"
        mkdir(ssh, virtiofs_mount_path)
        mount(ssh, virtiofs_mount_path)
        # Sanity check
        create_file(ssh, virtiofs_mount_path, "file")
        # Actual workload
        for i in range(3):
            touch_file_in_background(ssh, virtiofs_mount_path, "file")

    def stop(self):
        self.qemu.stop()


def test_vhost_lease_expiration():
    kikimr_binary_path = common.binary_path("cloud/storage/core/tools/testing/ydb/bin/ydbd")

    kikimr_configurator = KikimrConfigGenerator(
        erasure=None,
        use_in_memory_pdisks=True,
        binary_path=kikimr_binary_path,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
        ],
    )

    kikimr_cluster = kikimr_cluster_factory(configurator=kikimr_configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    filestore_server_binary_path = common.binary_path(
        "cloud/filestore/apps/server/filestore-server"
    )

    server_config = TServerAppConfig()
    server_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    server_unix_socket_path = str(
        pathlib.Path(tempfile.mkdtemp(dir="/tmp")) / "filestore.sock")
    server_config.ServerConfig.UnixSocketPath = server_unix_socket_path

    domain = kikimr_configurator.domains_txt.Domain[0].Name

    filestore_server_configurator = FilestoreServerConfigGenerator(
        binary_path=filestore_server_binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=True,
        kikimr_port=kikimr_port,
        domain=domain,
    )
    filestore_server_configurator.generate_configs(
        kikimr_configurator.domains_txt,
        kikimr_configurator.names_txt
    )

    filestore_server = FilestoreServer(
        configurator=filestore_server_configurator,
        kikimr_binary_path=kikimr_binary_path,
        dynamic_storage_pools=kikimr_configurator.dynamic_storage_pools,
    )
    filestore_server.start()

    wait_for_filestore_server(
        filestore_server,
        filestore_server_configurator.port
    )

    filestore_vhost_binary_path = common.binary_path(
        "cloud/filestore/apps/vhost/filestore-vhost")

    uid = str(uuid.uuid4())

    endpoint_storage_dir = common.work_path() + '/endpoints-' + uid
    pathlib.Path(endpoint_storage_dir).mkdir(parents=True, exist_ok=True)

    config = TVhostAppConfig()
    config.ServerConfig.CopyFrom(server_config.ServerConfig)

    config.VhostServiceConfig.CopyFrom(TVhostServiceConfig())
    config.VhostServiceConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    config.VhostServiceConfig.EndpointStorageDir = endpoint_storage_dir
    config.VhostServiceConfig.ServiceEndpoints.append(TServiceEndpoint())

    filestore_vhost_configurator = FilestoreVhostConfigGenerator(
        binary_path=filestore_vhost_binary_path,
        app_config=config,
        service_type="kikimr",
        verbose=True,
        kikimr_port=kikimr_port,
        domain=domain,
    )

    filestore_vhost = FilestoreVhost(filestore_vhost_configurator)
    filestore_vhost.start()

    wait_for_filestore_vhost(
        filestore_vhost,
        filestore_vhost_configurator.port
    )

    filestore_client_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")

    client = FilestoreCliClient(
        filestore_client_path,
        filestore_server_configurator.port,
        vhost_port=filestore_vhost_configurator.port,
        verbose=True,
        cwd=common.output_path())

    filesystem_id = "fs0"
    client.create(filesystem_id, "test_cloud", "test_folder")

    vhost_socket = create_endpoint(
        client,
        filesystem_id,
        "/tmp",
        "test.vhost",
        endpoint_storage_dir,
    )

    qemu = QemuWithWorkload(vhost_socket)
    qemu.start()

    kikimr_cluster.stop()
    filestore_server.stop()
    filestore_vhost.stop()

    qemu.stop()
