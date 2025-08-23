import logging
import subprocess
import tempfile

from pathlib import Path

import yatest.common as common

import contrib.ydb.tests.library.common.yatest_common as yatest_common
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
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


def init(stored_endpoints_path=None):
    # We run inside qemu, so do not need to cleanup
    temp_dir = tempfile.TemporaryDirectory(dir="/tmp")
    logging.info("Created temporary dir %s", temp_dir.name)
    sockets_dir = Path(temp_dir.name)
    server = TServerAppConfig()
    server.ServerConfig.NbdEnabled = False
    server.ServerConfig.UnixSocketPath = str(sockets_dir / "grpc.sock")
    server.ServerConfig.VhostEnabled = True
    server.ServerConfig.NbdEnabled = False
    server.ServerConfig.VhostDiscardEnabled = True
    server.ServerConfig.VhostOptimalIoSize = 4 * 1024 * 1024
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    endpoints_dir = Path(common.output_path()) / f"endpoints-{hash(common.context.test_name)}"
    endpoints_dir.mkdir(exist_ok=True)
    server.ServerConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server.ServerConfig.EndpointStorageDir = str(endpoints_dir)
    server.ServerConfig.AllowAllRequestsViaUDS = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    if stored_endpoints_path:
        stored_endpoints_path.mkdir(exist_ok=True)
    env = LocalLoadTest(
        endpoint="",
        server_app_config=server,
        storage_config_patches=None,
        use_in_memory_pdisks=True,
        with_endpoint_proxy=False,
        with_netlink=False,
        stored_endpoints_path=stored_endpoints_path)

    client_config_path = Path(yatest_common.output_path()) / "client-config.txt"
    client_config = TClientAppConfig()
    client_config.ClientConfig.CopyFrom(TClientConfig())
    client_config.ClientConfig.Host = "localhost"
    client_config.ClientConfig.InsecurePort = env.nbs_port
    client_config_path.write_text(MessageToString(client_config))

    def run(*args, **kwargs):
        args = [BLOCKSTORE_CLIENT_PATH,
                *args,
                "--grpc-trace",
                "--config",
                str(client_config_path)]
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


def test_start_vhost_endpoint():
    env, run = init()

    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 1024
    socket_path = "/tmp/vhost.sock"

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
            "vhost",
        )
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

        result = run(
            "destroyvolume",
            "--disk-id",
            volume_name,
            input=volume_name,
        )

        cleanup_after_test(env)
