import logging
import pytest
import subprocess
import tempfile

from pathlib import Path

import yatest.common as common

import ydb.tests.library.common.yatest_common as yatest_common
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.client_pb2 import TClientConfig, TClientAppConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import (
    thread_count
)
from cloud.storage.core.protos.endpoints_pb2 import (
    EEndpointStorageType,
)

from google.protobuf.text_format import MessageToString


BLOCKSTORE_CLIENT_PATH = common.binary_path(
    "cloud/blockstore/apps/client/blockstore-client")
ENDPOINT_PROXY_PATH = common.binary_path(
    "cloud/blockstore/apps/endpoint_proxy/blockstore-endpoint-proxy")


def init(with_netlink=True, with_endpoint_proxy=True):
    server_config_patch = TServerConfig()
    server_config_patch.NbdEnabled = True
    if with_endpoint_proxy:
        ep_socket = "ep-%s.sock" % hash(common.context.test_name)
        server_config_patch.EndpointProxySocketPath = ep_socket
    elif with_netlink:
        server_config_patch.NbdNetlink = True
        server_config_patch.NbdRequestTimeout = 120
        server_config_patch.NbdConnectionTimeout = 120
    endpoints_dir = Path(common.output_path()) / f"endpoints-{hash(common.context.test_name)}"
    endpoints_dir.mkdir(exist_ok=True)
    server_config_patch.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server_config_patch.EndpointStorageDir = str(endpoints_dir)
    server_config_patch.AllowAllRequestsViaUDS = True
    # We run inside qemu, so do not need to cleanup
    temp_dir = tempfile.TemporaryDirectory(dir="/tmp")
    logging.info("Created temporary dir %s", temp_dir.name)
    sockets_dir = Path(temp_dir.name)
    server_config_patch.UnixSocketPath = str(sockets_dir / "grpc.sock")
    server_config_patch.VhostEnabled = False
    server_config_patch.NbdDevicePrefix = "/dev/nbd"
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(server_config_patch)
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    subprocess.check_call(["modprobe", "nbd"], timeout=20)
    env = LocalLoadTest(
        endpoint="",
        server_app_config=server,
        storage_config_patches=None,
        use_in_memory_pdisks=True,
        with_endpoint_proxy=with_endpoint_proxy,
        with_netlink=with_netlink)

    client_config_path = Path(yatest_common.output_path()) / "client-config.txt"
    client_config = TClientAppConfig()
    client_config.ClientConfig.CopyFrom(TClientConfig())
    client_config.ClientConfig.RetryTimeout = 1
    client_config.ClientConfig.Host = "localhost"
    client_config.ClientConfig.InsecurePort = env.nbs_port
    client_config_path.write_text(MessageToString(client_config))

    def run(*args, **kwargs):
        args = [BLOCKSTORE_CLIENT_PATH, *args, "--config", str(client_config_path)]
        script_input = kwargs.get("input")
        if script_input is not None:
            script_input = script_input + "\n"

        logging.info("running command: %s" % args)
        result = subprocess.run(
            args,
            cwd=kwargs.get("cwd"),
            check=False,
            capture_output=True,
            input=script_input,
            text=True,
        )

        logging.info("Stdout: %s", result.stdout)
        logging.info("Stderr: %s", result.stderr)
        return result

    return env, run


def cleanup_after_test(env: LocalLoadTest):
    if env is None:
        return
    env.tear_down()


def log_called_process_error(exc):
    logging.error(
        "Failed %s, stdout: %s, stderr: %s",
        str(exc.args),
        exc.stderr,
        exc.stdout,
        exc_info=exc,
    )


@pytest.mark.parametrize('with_netlink,with_endpoint_proxy',
                         [(True, False), (True, True), (False, False), (False, True)])
def test_resize_device(with_netlink, with_endpoint_proxy):
    env, run = init(with_netlink, with_endpoint_proxy)
    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 10000
    volume_size = blocks_count * block_size
    nbd_device = "/dev/nbd0"
    socket_path = "/tmp/nbd.sock"
    try:
        result = run(
            "createvolume",
            "--disk-id",
            volume_name,
            "--blocks-count",
            str(blocks_count),
            "--block-size",
            str(block_size),
        )
        assert result.returncode == 0

        result = run(
            "startendpoint",
            "--disk-id",
            volume_name,
            "--socket",
            socket_path,
            "--ipc-type",
            "nbd",
            "--persistent",
            "--nbd-device",
            nbd_device
        )
        assert result.returncode == 0

        result = common.execute(
            ["blockdev", "--getsize64", nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        assert result.returncode == 0
        disk_size = int(result.stdout)
        assert volume_size == disk_size

        result = common.execute(
            ["mkfs", "-t", "ext4", "-E", "nodiscard", nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        assert result.returncode == 0

        mount_dir = Path("/tmp/mount")
        mount_dir.mkdir(parents=True, exist_ok=True)
        result = common.execute(
            ["mount", nbd_device, str(mount_dir)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        assert result.returncode == 0

        new_volume_size = 2 * volume_size
        result = run(
            "resizevolume",
            "--disk-id",
            volume_name,
            "--blocks-count",
            str(new_volume_size // block_size)
        )
        assert result.returncode == 0

        result = run(
            "refreshendpoint",
            "--socket",
            socket_path
        )
        assert result.returncode == 0

        result = common.execute(
            ["blockdev", "--getsize64", nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        assert result.returncode == 0
        disk_size = int(result.stdout)
        assert new_volume_size == disk_size

        result = common.execute(
            ["resize2fs", nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        assert result.returncode == 0

    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise
    finally:
        run(
            "stopendpoint",
            "--socket",
            socket_path,
        )

        run(
            "destroyvolume",
            "--disk-id",
            volume_name,
        )

        result = common.execute(
            ["umount", str(mount_dir)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        cleanup_after_test(env)
