import contextlib
import logging
import os
import pathlib
import subprocess
import tempfile
import time
import json

from functools import wraps
from pathlib import Path

import yatest.common as common

import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.client_pb2 import TClientConfig, TClientAppConfig
from cloud.filestore.config.vhost_pb2 import TVhostAppConfig, TVhostServiceConfig, TServiceEndpoint
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count
from cloud.filestore.tests.python.lib.daemon_config import FilestoreVhostConfigGenerator
from cloud.filestore.tests.python.lib.vhost import FilestoreVhost, wait_for_filestore_vhost
from cloud.storage.core.protos.endpoints_pb2 import (
    EEndpointStorageType,
)

from google.protobuf.text_format import MessageToString


BINARY_PATH = common.binary_path("cloud/blockstore/apps/client/blockstore-client")


def retry(max_retries):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if attempt == max_retries - 1:
                        raise
        return wrapper
    return decorator


class CsiLoadTest:
    """Independent YDB/NBS backends connected to one CSI driver.

    ``runs[i]`` addresses storage i directly. The CSI controller addresses
    storage 0; VM staging balances endpoints across all backends, so tests
    must create volumes on the backend that will be selected.
    """

    def __init__(
            self,
            storage_count: int = 1,
            vm_mode: bool = False,
            external_fs_configs=None,
            retry_timeout_ms: int | None = None):
        if storage_count < 1:
            raise ValueError("storage_count must be positive")

        self.storages = []
        self.local_nfs_vhost = None
        self.runs = []
        self._resources = contextlib.ExitStack()
        try:
            subprocess.check_call(["modprobe", "nbd"], timeout=20)
            self.sockets_dir = Path(self._resources.enter_context(
                tempfile.TemporaryDirectory(dir="/tmp")))
            output_dir = Path(get_unique_path_for_current_test(
                output_path=common.output_path(), sub_folder="storages"))
            for index in range(storage_count):
                storage_dir = output_dir / str(index)
                server = _server_app_config(
                    self.sockets_dir, storage_dir / "endpoints", vm_mode)
                # Match the socket naming convention in CSI createClients().
                socket_name = "grpc.sock" if index == 0 else f"grpc{index}.sock"
                server.ServerConfig.UnixSocketPath = str(self.sockets_dir / socket_name)
                storage = LocalLoadTest(
                    endpoint="",
                    server_app_config=server,
                    use_in_memory_pdisks=True,
                    kikimr_output_path=str(storage_dir),
                    config_sub_folder=f"nbs_configs_{index}")
                self.storages.append(storage)
                self._resources.callback(storage.tear_down)
                self.runs.append(_client_runner(
                    storage.nbs_port, storage_dir / "client-config.txt", retry_timeout_ms))

            if external_fs_configs:
                self.local_nfs_vhost = LocalNfsVhostRunner(external_fs_configs)
                self._resources.callback(self.local_nfs_vhost.stop)
                self.local_nfs_vhost.start()

            self.csi = NbsCsiDriverRunner(
                str(self.sockets_dir),
                str(self.sockets_dir / "grpc.sock"),
                vm_mode,
                local_fs_config=(
                    self.local_nfs_vhost.get_config() if self.local_nfs_vhost else None),
                nbs_server_count=storage_count)
            self._resources.callback(self.csi.stop)
            self.csi.start()
        except BaseException:
            self.tear_down()
            raise

    @property
    def nbs_port(self):
        return self.storages[0].nbs_port

    def tear_down(self):
        self._resources.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.tear_down()


class NbsCsiDriverRunner:

    def __init__(
            self,
            sockets_dir: str,
            grpc_unix_socket_path: str,
            vm_mode: bool,
            local_fs_config,
            nbs_server_count: int = 1):
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
        self._local_fs_config = local_fs_config
        self._nbs_server_count = nbs_server_count

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
            f"--nbs-server-replica-count={self._nbs_server_count}",
            f"--nbs-server-replica-count-total={self._nbs_server_count}",
            "--nfs-vhost-port=0",
            "--nfs-server-port=0",
            "-v=4",
        ]

        if self._local_fs_config:
            fs_override_path = (
                Path(yatest_common.output_path()) / "local-filestore-override.txt"
            )

            fs_override_path.write_text(json.dumps(self._local_fs_config["external_fs_configs"]))

            args += [
                f"--local-filestore-override={str(fs_override_path)}",
                f'--nfs-local-filestore-port={self._local_fs_config["filestore_port"]}',
                f'--nfs-local-endpoint-port={self._local_fs_config["endpoint_port"]}',
            ]

        if self._vm_mode:
            args += ["--vm-mode=true"]

        logging.info("Exec: %s", " ".join(args))
        self._proc = subprocess.Popen(
            args,
            stdout=self._log_file,
            stderr=self._log_file,
        )
        self._wait_socket()

    def _client_run(self, *args):
        args = [
            str(self._client_binary_path),
            *args,
            "--endpoint",
            str(self._endpoint),
        ]
        logging.info("Exec: %s", " ".join(args))
        result = subprocess.run(
            args,
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

    @retry(max_retries=3)
    def create_volume(self, name: str, size: int, is_nfs: bool = False):
        args = ["createvolume", "--name", name, "--size", str(size)]
        if is_nfs:
            args += ["--backend", "nfs"]

        return self._controller_run(*args)

    @retry(max_retries=3)
    def delete_volume(self, name: str):
        return self._controller_run("deletevolume", "--id", name)

    @retry(max_retries=3)
    def stage_volume(
            self,
            volume_id: str,
            access_type: str,
            is_nfs: bool = False,
            vhost_request_queues_count: int = 8,
            instance_id: str = "example-instance-id",
            staging_target_path: str = "",
            access_mode: str = "single-node-writer"):
        args = [
            "stagevolume",
            "--volume-id",
            volume_id,
            "--access-type",
            access_type,
            "--vhost-request-queues-count",
            str(vhost_request_queues_count),
        ]
        args += ["--instance-id", instance_id, "--access-mode", access_mode]
        if staging_target_path:
            args += ["--staging-target-path", staging_target_path]
        if is_nfs:
            args += ["--backend", "nfs"]

        return self._node_run(*args)

    @retry(max_retries=3)
    def unstage_volume(self, volume_id: str, staging_target_path: str = ""):
        args = [
            "unstagevolume",
            "--volume-id",
            volume_id,
        ]
        if staging_target_path:
            args += ["--staging-target-path", staging_target_path]

        return self._node_run(*args)

    @retry(max_retries=3)
    def publish_volume(
            self,
            pod_id: str,
            volume_id: str,
            pod_name: str,
            access_type: str,
            fs_type: str = "",
            readonly: bool = False,
            volume_mount_group: str = "",
            is_nfs: bool = False,
            instance_id: str = "example-instance-id",
            staging_target_path: str = "",
            target_path: str = "",
            access_mode: str = "single-node-writer"):
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
            "--instance-id",
            instance_id,
            "--access-mode",
            access_mode,
        ]
        if staging_target_path:
            args += ["--staging-target-path", staging_target_path]
        if target_path:
            args += ["--target-path", target_path]
        if readonly:
            args += ["--readonly"]

        if len(volume_mount_group) != 0:
            args += ["--volume-mount-group", volume_mount_group]

        if is_nfs:
            args += ["--backend", "nfs"]

        return self._node_run(*args)

    @retry(max_retries=3)
    def unpublish_volume(
            self,
            pod_id: str,
            volume_id: str,
            access_type: str,
            target_path: str = ""):
        args = [
            "unpublishvolume",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
            "--access-type",
            access_type,
        ]
        if target_path:
            args += ["--target-path", target_path]

        return self._node_run(*args)

    def stop(self):
        if self._proc is not None:
            logging.info("Process exit code: %s", self._proc.returncode)
            logging.info("Process pid %d", self._proc.pid)
            self._proc.kill()
            self._proc.wait()
            self._proc = None
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    @retry(max_retries=3)
    def volumestats(self, pod_id: str, volume_id: str):
        ret = self._node_run(
            "volumestats",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
        )
        return json.loads(ret)

    @retry(max_retries=3)
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


def _server_app_config(sockets_dir: Path, endpoints_dir: Path, vm_mode: bool):
    endpoints_dir.mkdir(parents=True, exist_ok=True)
    server = TServerAppConfig()
    config = server.ServerConfig
    config.NbdEnabled = True
    config.NbdNetlink = True
    config.AutomaticNbdDeviceManagement = True
    config.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    config.EndpointStorageDir = str(endpoints_dir)
    config.AllowAllRequestsViaUDS = True
    config.UnixSocketPath = str(sockets_dir / "grpc.sock")
    config.VhostEnabled = vm_mode
    config.NbdDevicePrefix = "/dev/nbd"
    config.ThreadsCount = thread_count()
    config.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    return server


def init(vm_mode: bool = False, retry_timeout_ms: int | None = None, external_fs_configs=None):
    env = CsiLoadTest(
        vm_mode=vm_mode,
        external_fs_configs=external_fs_configs,
        retry_timeout_ms=retry_timeout_ms)
    return env, env.runs[0]


def _client_runner(nbs_port: int, client_config_path: Path, retry_timeout_ms=None):
    client_config = TClientAppConfig()
    client_config.ClientConfig.CopyFrom(TClientConfig())
    if retry_timeout_ms:
        client_config.ClientConfig.RetryTimeout = retry_timeout_ms
    client_config.ClientConfig.Host = "localhost"
    client_config.ClientConfig.InsecurePort = nbs_port
    client_config_path.write_text(MessageToString(client_config))

    def run(*args, **kwargs):
        config_path = kwargs.get("config_path", client_config_path)
        args = [BINARY_PATH, *args, "--config", str(config_path)]
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
    return run


class LocalNfsVhostRunner:
    def __init__(self, external_fs_configs):
        self.external_fs_configs = external_fs_configs
        self.daemon = None

    def start(self):
        if not self.external_fs_configs:
            return

        endpoint_storage_dir = common.work_path() + '/local_nfs_endpoints'
        pathlib.Path(endpoint_storage_dir).mkdir(parents=True, exist_ok=True)

        root_dir = common.work_path() + '/local_nfs_root'
        pathlib.Path(root_dir).mkdir(parents=True, exist_ok=True)

        config = TVhostAppConfig()
        config.VhostServiceConfig.CopyFrom(TVhostServiceConfig())
        config.VhostServiceConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
        config.VhostServiceConfig.EndpointStorageDir = endpoint_storage_dir

        service_endpoint = TServiceEndpoint()
        config.VhostServiceConfig.ServiceEndpoints.append(service_endpoint)
        config.VhostServiceConfig.LocalServiceConfig.RootPath = root_dir

        vhost_configurator = FilestoreVhostConfigGenerator(
            binary_path=common.binary_path(
                "cloud/filestore/apps/vhost/filestore-vhost"),
            app_config=config,
            service_type="local",
            verbose=True,
            kikimr_port=0,
            domain=None,
        )

        self.daemon = FilestoreVhost(vhost_configurator)
        self.daemon.start()

        self.endpoint_port = vhost_configurator.port
        wait_for_filestore_vhost(self.daemon, self.endpoint_port, port_type="endpoint")

        self.filestore_port = vhost_configurator.local_service_port
        wait_for_filestore_vhost(self.daemon, self.filestore_port, port_type="filestore")

    def get_config(self):
        if not self.daemon:
            return None

        return dict(external_fs_configs=self.external_fs_configs,
                    endpoint_port=self.endpoint_port,
                    filestore_port=self.filestore_port)

    def stop(self):
        if self.daemon:
            self.daemon.stop()
