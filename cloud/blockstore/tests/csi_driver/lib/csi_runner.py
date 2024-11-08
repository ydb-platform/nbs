import contextlib
import logging
import os
import subprocess
import tempfile
import time
import json

from pathlib import Path

import yatest.common as common

import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test
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
            sockets_temporary_directory: tempfile.TemporaryDirectory,
            vm_mode: bool,
            *args,
            **kwargs,
    ):
        super(CsiLoadTest, self).__init__(*args, **kwargs)
        self.sockets_temporary_directory = sockets_temporary_directory
        self.csi = NbsCsiDriverRunner(sockets_dir, grpc_unix_socket_path, vm_mode)
        self.csi.start()

    def tear_down(self):
        self.csi.stop()
        super(CsiLoadTest, self).tear_down()
        self.sockets_temporary_directory.cleanup()


class NbsCsiDriverRunner:

    def __init__(self, sockets_dir: str, grpc_unix_socket_path: str, vm_mode: bool):
        csi_driver_dir = Path(
            common.binary_path("cloud/blockstore/tools/csi_driver/"),
        )
        self._binary_path = csi_driver_dir / "cmd/nbs-csi-driver/nbs-csi-driver"
        self._client_binary_path = csi_driver_dir / "client/csi-client"
        self._sockets_dir = sockets_dir
        self._endpoint = Path(sockets_dir) / "csi.sock"
        self._grpc_unix_socket_path = grpc_unix_socket_path
        self._proc = None
        self._csi_driver_output = os.path.join(get_unique_path_for_current_test(
            output_path=common.output_path(),
            sub_folder=""), "driver_output.txt")
        self._log_file = None
        self._vm_mode = vm_mode

    def start(self):
        self._log_file = open(self._csi_driver_output, "w")
        args = [
            str(self._binary_path),
            "--name=nbs.csi.nebius.ai",
            "--version",
            "v1",
            "--node-id=localhost",
            "--nbs-socket", self._grpc_unix_socket_path,
            f"--sockets-dir={self._sockets_dir}",
            f"--endpoint={str(self._endpoint)}",
            "--nfs-vhost-port=0",
            "--nfs-server-port=0",
        ]
        if self._vm_mode:
            args += ["--vm-mode=true"]

        self._proc = subprocess.Popen(
            args,
            stdout=self._log_file,
            stderr=self._log_file,
        )
        self._wait_socket()

    def _client_run(self, *args):
        result = subprocess.run(
            [
                str(self._client_binary_path),
                *args,
                "--endpoint",
                str(self._endpoint),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        logging.info("Stdout: %s", result.stdout)
        logging.info("Stderr: %s", result.stderr)
        return result.stdout

    def _wait_socket(self, timeout_sec=60):
        started_at = time.monotonic()
        while True:
            if self._endpoint.exists():
                return
            if time.monotonic() - started_at > timeout_sec:
                raise TimeoutError("Timeout getting socket")
            time.sleep(0.1)

    def _node_run(self, *args):
        return self._client_run("node", *args)

    def _controller_run(self, *args):
        return self._client_run("controller", *args)

    def create_volume(self, name: str, size: int):
        return self._controller_run("createvolume", "--name", name, "--size", str(size))

    def delete_volume(self, name: str):
        return self._controller_run("deletevolume", "--id", name)

    def stage_volume(self, volume_id: str, access_type: str):
        return self._node_run(
            "stagevolume",
            "--volume-id",
            volume_id,
            "--access-type",
            access_type,
        )

    def unstage_volume(self, volume_id: str):
        return self._node_run(
            "unstagevolume",
            "--volume-id",
            volume_id,
        )

    def publish_volume(
            self,
            pod_id: str,
            volume_id: str,
            pod_name: str,
            access_type: str,
            fs_type: str = "",
            readonly: bool = False):
        args = [
            "publishvolume",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
            "--pod-name",
            pod_name,
            "--fs-type",
            fs_type,
            "--access-type",
            access_type,
        ]
        if readonly:
            args += ["--readonly"]
        return self._node_run(*args)

    def unpublish_volume(self, pod_id: str, volume_id: str, access_type: str):
        return self._node_run(
            "unpublishvolume",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
            "--access-type",
            access_type,
        )

    def stop(self):
        if self._proc is not None:
            logging.info("Process exit code: %s", self._proc.returncode)
            logging.info("Process pid %d", self._proc.pid)
        self._proc.kill()
        self._log_file.close()

    def volumestats(self, pod_id: str, volume_id: str):
        ret = self._node_run(
            "volumestats",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
        )
        return json.loads(ret)

    def expand_volume(self, pod_id: str, volume_id: str, size: int, access_type: str):
        return self._node_run(
            "expandvolume",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
            "--size",
            str(size),
            "--access-type",
            access_type,
        )


@contextlib.contextmanager
def called_process_error_logged():
    try:
        yield
    except subprocess.CalledProcessError as e:
        log_called_process_error(e)


def log_called_process_error(exc):
    logging.error(
        "Failed %s, stdout: %s, stderr: %s",
        str(exc.args),
        exc.stderr,
        exc.stdout,
        exc_info=exc,
    )


def cleanup_after_test(
        env: CsiLoadTest,
        volume_name: str = "",
        access_type: str = "mount",
        pods: list[str] = []):
    if env is None:
        return

    # sleep 1 second to distingish dmesg logs before and after test failure
    time.sleep(1)

    for pod_id in pods:
        with called_process_error_logged():
            env.csi.unpublish_volume(pod_id, volume_name, access_type)

    with called_process_error_logged():
        env.csi.unstage_volume(volume_name)
    with called_process_error_logged():
        env.csi.delete_volume(volume_name)

    env.tear_down()


def init(vm_mode: bool = False, retry_timeout_ms: int | None = None):
    server_config_patch = TServerConfig()
    server_config_patch.NbdEnabled = True
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
    server_config_patch.VhostEnabled = True
    server_config_patch.NbdDevicePrefix = "/dev/nbd"
    ep_socket = "ep-%s.sock" % hash(common.context.test_name)
    server_config_patch.EndpointProxySocketPath = ep_socket
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(server_config_patch)
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    subprocess.check_call(["modprobe", "nbd"], timeout=20)
    env = CsiLoadTest(
        sockets_dir=str(sockets_dir),
        grpc_unix_socket_path=server_config_patch.UnixSocketPath,
        sockets_temporary_directory=temp_dir,
        vm_mode=vm_mode,
        endpoint="",
        server_app_config=server,
        storage_config_patches=None,
        use_in_memory_pdisks=True,
        with_endpoint_proxy=True,
        with_netlink=True)

    client_config_path = Path(yatest_common.output_path()) / "client-config.txt"
    client_config = TClientAppConfig()
    client_config.ClientConfig.CopyFrom(TClientConfig())
    if retry_timeout_ms:
        client_config.ClientConfig.RetryTimeout = retry_timeout_ms
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
