import logging
import os
import subprocess

from pathlib import Path

import yatest.common as common

import contrib.ydb.tests.library.common.yatest_common as yatest_common
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import (
    thread_count
)
from cloud.storage.core.protos.endpoints_pb2 import (
    EEndpointStorageType,
)

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

    env.results_path = yatest_common.output_path() + "/results.txt"
    env.results_file = open(env.results_path, "w")

    def run(*args, **kwargs):
        args = [BINARY_PATH] + list(args) + [
            "--host", "localhost",
            "--port", str(env.nbs_port)]
        input = kwargs.get("input")
        if input is not None:
            input = (input + "\n").encode("utf8")

        logging.info("running command: %s" % args)
        process = subprocess.Popen(
            args,
            stdout=env.results_file,
            stdin=subprocess.PIPE,
            cwd=kwargs.get("cwd")
        )
        process.communicate(input=input)

        assert process.returncode == kwargs.get("code", 0)
    return env, run


class NbsCsiDriverRunner:
    def __init__(self, sockets_dir: str, grpc_unix_socket_path: str):
        self._binary_path = common.binary_path("cloud/blockstore/tools/csi_driver/cmd/nbs-csi-driver/nbs-csi-driver")
        self._client_binary_path = common.binary_path("cloud/blockstore/tools/csi_driver/client/csi-client")
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
                self._binary_path,
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
        subprocess.check_call(
            [
                self._client_binary_path,
                *args,
                "--endpoint",
                self._endpoint,
            ]
        )

    def _node_run(self, *args):
        return self._client_run("node", *args)

    def _controller_run(self, *args):
        return self._client_run("controller", *args)

    def create_volume(self, name: str, size: int):
        return self._controller_run("createvolume", "--name", name, "--size", str(size))

    def delete_volume(self, name: str):
        return self._controller_run("deletevolume", "--name", name)

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


def tear_down(env):
    if env is None:
        return
    env.results_file.close()
    env.csi_driver.stop()
    env.tear_down()


def test_nbs_csi_driver_mounted_disk_protected_from_deletion():
    env, run = setup()
    try:
        volume_name = "example-disk"
        volume_size = 10 * 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.publish_volume(pod_id, volume_name, pod_name)
        run(
            "destroyvolume",
            "--disk-id",
            volume_name,
            input=volume_name,
            code=1,
        )
        env.csi.unpublish_volume(pod_id, volume_name)
        env.csi.delete_volume(volume_name)
    finally:
        tear_down(env)
