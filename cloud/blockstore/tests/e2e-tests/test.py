import logging
import pytest
import os
import signal
import shutil
import subprocess
import tempfile
import time
import yaml

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


BLOCKSTORE_CLIENT_PATH = common.binary_path(
    "cloud/blockstore/apps/client/blockstore-client")
ENDPOINT_PROXY_PATH = common.binary_path(
    "cloud/blockstore/apps/endpoint_proxy/blockstore-endpoint-proxy")


def init(
        with_netlink=True,
        with_endpoint_proxy=True,
        stored_endpoints_path=None,
        nbd_request_timeout=None,
        nbd_reconnect_delay=None,
        proxy_restart_events=None,
        max_zero_blocks_sub_request_size=None
):
    server_config_patch = TServerConfig()
    server_config_patch.NbdEnabled = True
    if with_endpoint_proxy:
        ep_socket = "ep-%s.sock" % hash(common.context.test_name)
        server_config_patch.EndpointProxySocketPath = ep_socket
    elif with_netlink:
        server_config_patch.NbdNetlink = True
        if nbd_request_timeout:
            server_config_patch.NbdRequestTimeout = nbd_request_timeout * 1000
        else:
            server_config_patch.NbdRequestTimeout = 10 * 1000       # 10 seconds
        logging.info("nbd_request_timeout: %d" % server_config_patch.NbdRequestTimeout)
        server_config_patch.NbdConnectionTimeout = 10 * 60 * 1000   # 10 minutes
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
    if max_zero_blocks_sub_request_size:
        server.ServerConfig.MaxZeroBlocksSubRequestSize = max_zero_blocks_sub_request_size
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    subprocess.check_call(["modprobe", "nbd"], timeout=20)
    if stored_endpoints_path:
        stored_endpoints_path.mkdir(exist_ok=True)
    env = LocalLoadTest(
        endpoint="",
        server_app_config=server,
        storage_config_patches=None,
        use_in_memory_pdisks=True,
        with_endpoint_proxy=with_endpoint_proxy,
        with_netlink=with_netlink,
        stored_endpoints_path=stored_endpoints_path,
        nbd_request_timeout=nbd_request_timeout,
        nbd_reconnect_delay=nbd_reconnect_delay,
        proxy_restart_events=proxy_restart_events)

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


def test_nbd_reconnect():
    disk_id = "disk0"
    block_size = 4096
    blocks_count = 1024
    nbd_device = "/dev/nbd0"
    socket = "/tmp/nbd.sock"
    request_timeout = 2
    runtime = request_timeout * 2
    nbs_downtime = request_timeout + 2
    iodepth = 1024

    env, run = init(
        with_netlink=True,
        with_endpoint_proxy=False,
        nbd_request_timeout=request_timeout)

    try:
        result = run(
            "createvolume",
            "--disk-id",
            disk_id,
            "--blocks-count",
            str(blocks_count),
            "--block-size",
            str(block_size),
        )
        assert result.returncode == 0

        result = run(
            "startendpoint",
            "--disk-id",
            disk_id,
            "--socket",
            socket,
            "--ipc-type",
            "nbd",
            "--persistent",
            "--nbd-device",
            nbd_device
        )
        assert result.returncode == 0

        proc = subprocess.Popen(
            [
                "fio",
                "--name=fio",
                "--ioengine=libaio",
                "--direct=1",
                "--time_based=1",
                "--rw=randrw",
                "--rwmixread=50",
                "--filename=" + nbd_device,
                "--runtime=" + str(runtime),
                "--blocksize=" + str(block_size),
                "--iodepth=" + str(iodepth),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        os.kill(env.nbs.pid, signal.SIGSTOP)
        time.sleep(nbs_downtime)
        os.kill(env.nbs.pid, signal.SIGCONT)

        proc.communicate(timeout = runtime * 2)
        assert proc.returncode == 0

    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise

    finally:
        run(
            "stopendpoint",
            "--socket",
            socket,
        )

        result = run(
            "destroyvolume",
            "--disk-id",
            disk_id,
            input=disk_id,
        )

        cleanup_after_test(env)


def test_multiple_errors():
    volume_name = "test-disk"
    block_size = 4096
    blocks_count = 1024
    nbd_device = "/dev/nbd0"
    socket_path = "/tmp/nbd.sock"
    request_timeout = 2
    runtime = request_timeout * 2
    nbs_downtime = request_timeout + 1
    iodepth = 1024

    env, run = init(
        with_netlink=True,
        with_endpoint_proxy=True,
        nbd_request_timeout=request_timeout,
        proxy_restart_events=2)

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

        proc = subprocess.Popen(
            [
                "fio",
                "--name=fio",
                "--ioengine=libaio",
                "--direct=1",
                "--time_based=1",
                "--rw=randrw",
                "--rwmixread=50",
                "--filename=" + nbd_device,
                "--runtime=" + str(runtime),
                "--blocksize=" + str(block_size),
                "--iodepth=" + str(iodepth),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        os.kill(env.nbs.pid, signal.SIGSTOP)
        time.sleep(nbs_downtime)
        os.kill(env.nbs.pid, signal.SIGCONT)

        proc.communicate(timeout=60)

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


def test_stop_start():
    env, run = init(
        with_netlink=True,
        with_endpoint_proxy=True,
        nbd_reconnect_delay=1)

    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 1024
    nbd_device = "/dev/nbd0"
    socket_path = "/tmp/nbd.sock"
    data_file = "data"

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

        result = run(
            "stopendpoint",
            "--socket",
            socket_path,
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

        proc = subprocess.Popen(
            [
                "dd",
                "if=" + nbd_device,
                "iflag=direct",
                "of=" + data_file,
                "bs=" + str(block_size),
                "count=" + str(blocks_count)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        proc.communicate(timeout=60)
        assert proc.returncode == 0

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


@pytest.mark.parametrize('with_netlink,with_endpoint_proxy',
                         [(True, False), (True, True), (False, False), (False, True)])
def test_resize_device(with_netlink, with_endpoint_proxy):
    stored_endpoints_path = Path(common.output_path()) / "stored_endpoints"
    env, run = init(
        with_netlink,
        with_endpoint_proxy,
        stored_endpoints_path,
        max_zero_blocks_sub_request_size=512 * 1024 * 1024
    )

    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 1310720
    volume_size = blocks_count * block_size
    nbd_device = "/dev/nbd0"
    socket_path = "/tmp/nbd.sock"
    stored_endpoint_path = stored_endpoints_path / socket_path.replace("/", "_")
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
            nbd_device,
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
            ["mkfs", "-t", "ext4", nbd_device],
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

        if with_endpoint_proxy:
            with open(stored_endpoint_path) as stream:
                stored_endpoint = yaml.safe_load(stream)
            assert stored_endpoint["BlocksCount"] == volume_size / block_size

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

        if with_endpoint_proxy:
            with open(stored_endpoint_path) as stream:
                stored_endpoint = yaml.safe_load(stream)
            assert stored_endpoint["BlocksCount"] == new_volume_size / block_size

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

        if mount_dir is not None:
            result = common.execute(
                ["umount", str(mount_dir)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)

        cleanup_after_test(env)


def test_do_not_restore_endpoint_with_missing_volume():
    # Scenario:
    # 1. run nbs
    # 2. create volume and start endpoint
    # 4. copy endpoint to backup folder
    # 5. stop endpoint and remove volume
    # 6. stop nbs
    # 7. copy endpoint from backup folder to endpoints folder
    # 8. run nbs
    # 9. check that endpoint was removed from endpoints folder
    env, run = init()

    test_hash = hash(common.context.test_name)
    endpoints_dir = Path(common.output_path()) / f"endpoints-{test_hash}"
    backup_endpoints_dir = Path(common.output_path()) / f"backup-endpoints-{test_hash}"

    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 10000
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

        shutil.copytree(endpoints_dir, backup_endpoints_dir)

        result = run(
            "stopendpoint",
            "--socket",
            socket_path,
        )
        assert result.returncode == 0

        result = run(
            "destroyvolume",
            "--disk-id",
            volume_name,
            input=volume_name,
        )
        assert result.returncode == 0

    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise

    cleanup_after_test(env)

    shutil.rmtree(endpoints_dir)
    shutil.copytree(backup_endpoints_dir, endpoints_dir)
    env, run = init()
    result = run(
        "listendpoints",
        "--wait-for-restoring",
    )
    assert result.returncode == 0
    assert 0 == len(os.listdir(endpoints_dir))
    cleanup_after_test(env)


def test_restore_endpoint_when_socket_directory_does_not_exist():
    # Scenario:
    # 1. run nbs
    # 2. create volume and start endpoint
    # 4. copy endpoint to backup directory
    # 5. stop endpoint
    # 6. stop nbs
    # 7. copy endpoint from backup directory to endpoints directory
    # 8. remove socket directory
    # 8. run nbs
    # 9. check that endpoint was restored
    env, run = init()

    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 10000
    nbd_device = Path("/dev/nbd0")
    socket_dir = Path("/tmp") / volume_name
    socket_dir.mkdir()
    socket_path = socket_dir / "nbd.sock"
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

        shutil.rmtree(socket_dir)

        env.nbs.restart()

        result = run(
            "listendpoints",
            "--wait-for-restoring",
        )
        assert result.returncode == 0
        assert socket_path.exists()

    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise

    cleanup_after_test(env)


def test_discard_device():
    stored_endpoints_path = Path(common.output_path()) / "stored_endpoints"
    env, run = init(
        True,
        False,
        stored_endpoints_path,
        max_zero_blocks_sub_request_size=512 * 1024 * 1024
    )

    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 1310720
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
            nbd_device,
        )
        assert result.returncode == 0

        result = common.execute(
            ["blockdev", "--getsize64", nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        assert result.returncode == 0
        disk_size = int(result.stdout)
        assert volume_size == disk_size

        # discard all bytes
        result = common.execute(
            ["blkdiscard", nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        assert result.returncode == 0

        # discard 2 GiB with unaligned offset in the first and last block
        length = 2 * 1024 * 1024 * 1024
        sector_size = 512
        result = common.execute(
            ["blkdiscard", "-o", str(sector_size), "-l", str(length), nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        assert result.returncode == 0

        # discard ~2 GiB with unaligned offset in the last block
        result = common.execute(
            ["blkdiscard", "-l", str(length + sector_size), nbd_device],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        assert result.returncode == 0

        # discard ~2 GiB with unaligned offset in the first block
        result = common.execute(
            ["blkdiscard", "-o", str(sector_size), "-l", str(length - sector_size), nbd_device],
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

        result = run(
            "destroyvolume",
            "--disk-id",
            volume_name,
            input=volume_name,
        )

        cleanup_after_test(env)
