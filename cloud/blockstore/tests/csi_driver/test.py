import logging
import os
import subprocess

from pathlib import Path

import yatest.common as common

import contrib.ydb.tests.library.common.yatest_common as yatest_common
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


BINARY_PATH = common.binary_path("cloud/blockstore/apps/client/blockstore-client")


class CsiLoadTest(LocalLoadTest):

    def __init__(
            self,
            sockets_dir: str,
            grpc_unix_socket_path: str,
            *args,
            **kwargs,
    ):
        super(CsiLoadTest, self).__init__(*args, **kwargs)
        self.csi = NbsCsiDriverRunner(sockets_dir, grpc_unix_socket_path)
        self.csi.start()

    def tear_down(self):
        self.csi.stop()
        super(CsiLoadTest, self).tear_down()


def setup():
    server_config_patch = TServerConfig()
    server_config_patch.NbdEnabled = True
    endpoints_dir = Path(common.output_path()) / f"endpoints-{hash(common.context.test_name)}"
    endpoints_dir.mkdir(exist_ok=True)
    server_config_patch.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server_config_patch.EndpointStorageDir = str(endpoints_dir)
    server_config_patch.AllowAllRequestsViaUDS = True
    sockets_dir = Path("/run/nbsd")
    sockets_dir.mkdir(exist_ok=True)
    server_config_patch.UnixSocketPath = str(sockets_dir / "grpc.sock")
    server_config_patch.VhostEnabled = False
    server_config_patch.NbdDevicePrefix = "/dev/nbd"
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(server_config_patch)
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    subprocess.check_call(["modprobe", "nbd"], timeout=20)
    env = CsiLoadTest(
        sockets_dir=str(sockets_dir),
        grpc_unix_socket_path=server_config_patch.UnixSocketPath,
        endpoint="",
        server_app_config=server,
        storage_config_patches=None,
        use_in_memory_pdisks=True)

    client_config_path = Path(yatest_common.output_path()) / "client-config.txt"
    client_config = TClientAppConfig()
    client_config.ClientConfig.CopyFrom(TClientConfig())
    client_config.ClientConfig.RetryTimeout = 1
    client_config.ClientConfig.Host = "localhost"
    client_config.ClientConfig.InsecurePort = env.nbs_port
    client_config_path.write_text(MessageToString(client_config))

    def run(*args, **kwargs):
        args = [BINARY_PATH, *args, "--config", str(client_config_path)]
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
        return result
    return env, run


class NbsCsiDriverRunner:

    def __init__(self, sockets_dir: str, grpc_unix_socket_path: str):
        csi_driver_dir = Path(
            common.binary_path("cloud/blockstore/tools/csi_driver/"),
        )
        self._binary_path = csi_driver_dir / "cmd/nbs-csi-driver/nbs-csi-driver"
        self._client_binary_path = csi_driver_dir / "client/csi-client"
        self._sockets_dir = sockets_dir
        self._endpoint = "csi.sock"
        self._grpc_unix_socket_path = grpc_unix_socket_path
        self._proc = None
        self._csi_driver_output = os.path.join(common.output_path(), "driver_output.txt")
        self._log_file = None

    def start(self):
        self._log_file = open(self._csi_driver_output, "w")
        self._proc = subprocess.Popen(
            [
                str(self._binary_path),
                "--name=nbs.csi.nebius.ai",
                "--version",
                "v1",
                "--node-id=localhost",
                "--nbs-socket", self._grpc_unix_socket_path,
                f"--sockets-dir={self._sockets_dir}",
                f"--endpoint={self._endpoint}"
            ],
            stdout=self._log_file,
            stderr=self._log_file,
        )

    def _client_run(self, *args):
        result = subprocess.run(
            [
                str(self._client_binary_path),
                *args,
                "--endpoint",
                self._endpoint,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        logging.info("Stdout: %s", result.stdout)
        logging.info("Stderr: %s", result.stderr)

    def _node_run(self, *args):
        return self._client_run("node", *args)

    def _controller_run(self, *args):
        return self._client_run("controller", *args)

    def create_volume(self, name: str, size: int):
        return self._controller_run("createvolume", "--name", name, "--size", str(size))

    def delete_volume(self, name: str):
        return self._controller_run("deletevolume", "--id", name)

    def publish_volume(self, pod_id: str, volume_id: str, pod_name: str):
        return self._node_run(
            "publishvolume",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
            "--pod-name",
            pod_name,
        )

    def unpublish_volume(self, pod_id: str, volume_id: str):
        return self._node_run(
            "unpublishvolume",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
        )

    def stop(self):
        self._proc.kill()
        self._log_file.close()


def tear_down(env: CsiLoadTest):
    if env is None:
        return
    env.csi.stop()
    env.tear_down()


def log_called_process_error(exc):
    logging.error(
        "Failed %s, stdout: %s, stderr: %s",
        str(exc.args),
        exc.stderr,
        exc.stdout,
        exc_info=exc,
    )


def test_nbs_csi_driver_mounted_disk_protected_from_deletion():
    env, run = setup()
    try:
        volume_name = "example-disk"
        volume_size = 10 * 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.publish_volume(pod_id, volume_name, pod_name)
        result = run(
            "destroyvolume",
            "--disk-id",
            volume_name,
            input=volume_name,
            code=1,
        )
        logging.info("Stdout: %s", result.stdout)
        logging.info("Stderr: %s", result.stderr)
        if result.returncode != 1:
            raise AssertionError("Destroyvolume must return exit code 1")
        assert "E_REJECTED" in result.stdout
        try:
            env.csi.unpublish_volume(pod_id, volume_name)
        except subprocess.CalledProcessError as e:
            log_called_process_error(e)
        try:
            env.csi.delete_volume(volume_name)
        except subprocess.CalledProcessError as e:
            log_called_process_error(e)
    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise
    finally:
        tear_down(env)
