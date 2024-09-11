import logging
import os
import pytest
import subprocess
import tempfile
import time
import json

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
            sockets_temporary_directory: tempfile.TemporaryDirectory,
            *args,
            **kwargs,
    ):
        super(CsiLoadTest, self).__init__(*args, **kwargs)
        self.sockets_temporary_directory = sockets_temporary_directory
        self.csi = NbsCsiDriverRunner(sockets_dir, grpc_unix_socket_path)
        self.csi.start()

    def tear_down(self):
        self.csi.stop()
        super(CsiLoadTest, self).tear_down()
        self.sockets_temporary_directory.cleanup()


def init():
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
        sockets_temporary_directory=temp_dir,
        endpoint="",
        server_app_config=server,
        storage_config_patches=None,
        use_in_memory_pdisks=True,
        with_endpoint_proxy=True,
        with_netlink=True)

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
        self._endpoint = Path(sockets_dir) / "csi.sock"
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
                f"--endpoint={str(self._endpoint)}",
                "--nfs-vhost-port=0",
                "--nfs-server-port=0",
            ],
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

    def expand_volume(self, pod_id: str, volume_id: str, size: int):
        return self._node_run(
            "expandvolume",
            "--pod-id",
            pod_id,
            "--volume-id",
            volume_id,
            "--size",
            str(size),
        )


def cleanup_after_test(env: CsiLoadTest):
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


def test_nbs_csi_driver_mounted_disk_protected_from_deletion():
    env, run = init()
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
        cleanup_after_test(env)


def test_nbs_csi_driver_volume_stat():
    # Scenario
    # 1. create volume and publish volume
    # 2. get volume stats and validate output
    # 3. create two files in the mounted directory
    # 4. get volume stats again and validate output
    # 5. check that the difference between used/available bytes is 2 block sizes
    # 6. check that the difference between used/available inodes is 2
    env, run = init()
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.publish_volume(pod_id, volume_name, pod_name)
        stats1 = env.csi.volumestats(pod_id, volume_name)

        assert "usage" in stats1
        usage_array1 = stats1["usage"]
        assert 2 == len(usage_array1)
        for usage in usage_array1:
            usage = usage_array1[0]
            assert {"unit", "total", "available", "used"} == usage.keys()
            assert 0 != usage["total"]
            assert usage["total"] == usage["available"] + usage["used"]

        mount_path = Path("/var/lib/kubelet/pods") / pod_id / "volumes/kubernetes.io~csi" / volume_name / "mount"
        (mount_path / "test1.file").write_bytes(b"\0")
        (mount_path / "test2.file").write_bytes(b"\0")

        stats2 = env.csi.volumestats(pod_id, volume_name)
        assert "usage" in stats2
        usage_array2 = stats2["usage"]
        assert 2 == len(usage_array2)
        for usage in usage_array2:
            usage = usage_array2[0]
            assert {"unit", "total", "available", "used"} == usage.keys()
            assert 0 != usage["total"]
            assert usage["total"] == usage["available"] + usage["used"]

        bytesUsage1 = usage_array1[0]
        bytesUsage2 = usage_array2[0]
        assert 4096 * 2 == bytesUsage1["available"] - bytesUsage2["available"]
        assert 4096 * 2 == bytesUsage2["used"] - bytesUsage1["used"]

        nodesUsage1 = usage_array1[1]
        nodesUsage2 = usage_array2[1]
        assert 2 == nodesUsage1["available"] - nodesUsage2["available"]
        assert 2 == nodesUsage2["used"] - nodesUsage1["used"]

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
        cleanup_after_test(env)


@pytest.mark.parametrize('mount_path,volume_access_type',
                         [("/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/456/mount", "mount"),
                          ("/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/123/456", "block")])
def test_csi_sanity_nbs_backend(mount_path, volume_access_type):
    env, run = init()
    backend = "nbs"

    try:
        CSI_SANITY_BINARY_PATH = common.binary_path("cloud/blockstore/tools/testing/csi-sanity/bin/csi-sanity")
        mount_dir = Path(mount_path)
        mount_dir.parent.mkdir(parents=True, exist_ok=True)

        params_file = Path(os.getcwd()) / "params.yaml"
        params_file.write_text(f"backend: {backend}")

        skipTests = ["should fail when the node does not exist"]

        args = [CSI_SANITY_BINARY_PATH,
                "-csi.endpoint",
                env.csi._endpoint,
                "--csi.mountdir",
                mount_dir,
                "-csi.testvolumeparameters",
                params_file,
                "-csi.testvolumeaccesstype",
                volume_access_type,
                "--ginkgo.skip",
                '|'.join(skipTests)]
        subprocess.run(
            args,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise
    finally:
        cleanup_after_test(env)


def test_node_volume_expand():
    env, run = init()
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.publish_volume(pod_id, volume_name, pod_name)

        new_volume_size = 2 * volume_size
        env.csi.expand_volume(pod_id, volume_name, new_volume_size)

        stats = env.csi.volumestats(pod_id, volume_name)
        assert "usage" in stats
        usage_array = stats["usage"]
        assert 2 == len(usage_array)
        bytes_usage = usage_array[0]
        assert "total" in bytes_usage
        # approximate check that total space is around 2GB
        assert bytes_usage["total"] // 1000 ** 3 == 2

        # check that expand_volume is idempotent method
        env.csi.expand_volume(pod_id, volume_name, new_volume_size)
    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise
    finally:
        try:
            env.csi.unpublish_volume(pod_id, volume_name)
        except subprocess.CalledProcessError as e:
            log_called_process_error(e)
        try:
            env.csi.delete_volume(volume_name)
        except subprocess.CalledProcessError as e:
            log_called_process_error(e)
        cleanup_after_test(env)
