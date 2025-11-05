import logging
import pytest
import os
import signal
import shutil
import subprocess
import tempfile
import time

from pathlib import Path

import yatest.common as common
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from contrib.ydb.core.protos.config_pb2 import TLogConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.client_pb2 import TClientConfig, TClientAppConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType
from google.protobuf.text_format import MessageToString


BLOCKSTORE_CLIENT_PATH = common.binary_path(
    "cloud/blockstore/apps/client/blockstore-client")


def init(
        nbd_netlink=True,
        nbd_request_timeout=10,
        nbds_max=4,
        max_zero_blocks_sub_request_size=None,
):
    server_config_patch = TServerConfig()
    server_config_patch.NbdEnabled = True
    if nbd_netlink:
        server_config_patch.NbdNetlink = True
        server_config_patch.NbdRequestTimeout = nbd_request_timeout * 1000
        server_config_patch.NbdConnectionTimeout = 600 * 1000
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
    server_config_patch.AutomaticNbdDeviceManagement = True
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(server_config_patch)
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    if max_zero_blocks_sub_request_size:
        server.ServerConfig.MaxZeroBlocksSubRequestSize = max_zero_blocks_sub_request_size
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    subprocess.check_call(["modprobe", "nbd", f"nbds_max={nbds_max}"], timeout=20)
    log_config = TLogConfig()
    log_config.Entry.add(Component=b"BLOCKSTORE_NBD", Level=7)
    env = LocalLoadTest(
        endpoint="",
        server_app_config=server,
        use_in_memory_pdisks=True,
        log_config=log_config)

    client_config_path = Path(yatest_common.output_path()) / "client-config.txt"
    client_config = TClientAppConfig()
    client_config.ClientConfig.CopyFrom(TClientConfig())
    client_config.ClientConfig.Host = "localhost"
    client_config.ClientConfig.InsecurePort = env.nbs_port
    client_config_path.write_text(MessageToString(client_config))

    def run(*args, **kwargs):
        args = [BLOCKSTORE_CLIENT_PATH,
                *args,
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
    subprocess.call(["rmmod", "nbd"], timeout=20)
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


@pytest.mark.parametrize('nbd_netlink', [True, False])
def test_free_device_allocation(nbd_netlink):
    disk = "disk"
    block_size = 4096
    blocks_count = 1024
    socket = "/tmp/sock"
    nbds_max = 4

    env, run = init(
        nbd_netlink=nbd_netlink,
        nbds_max=nbds_max)

    def createvolume(i):
        return run(
            "createvolume",
            "--disk-id",
            disk + str(i),
            "--blocks-count",
            str(blocks_count),
            "--block-size",
            str(block_size)
        ).returncode

    def startendpoint(i):
        return run(
            "startendpoint",
            "--disk-id",
            disk + str(i),
            "--socket",
            socket + str(i),
            "--ipc-type",
            "nbd",
            "--persistent",
            "--nbd-device"
        ).returncode

    try:
        for i in range(0, nbds_max):
            assert createvolume(i) == 0
            assert startendpoint(i) == 0

        if nbd_netlink:
            # netlink implementation can allocate new devices on the fly,
            # so we should be able to go beyond configured limit
            assert createvolume(nbds_max) == 0
            assert startendpoint(nbds_max) == 0
        else:
            # ioctl implementation won't be able to find free device
            assert createvolume(nbds_max) == 0
            assert startendpoint(nbds_max) != 0

    except subprocess.CalledProcessError as e:
        log_called_process_error(e)
        raise

    finally:
        for i in range(0, nbds_max + 1):
            run(
                "stopendpoint",
                "--socket",
                socket + str(i),
            )

            run(
                "destroyvolume",
                "--disk-id",
                disk + str(i),
                input=disk+str(i),
            )

        cleanup_after_test(env)


def force_nbd_reconnect(
        env,
        filename,
        runtime,
        downtime,
        block_size=4096,
        iodepth=1024,
):
    proc = subprocess.Popen(
        [
            "fio",
            "--name=fio",
            "--ioengine=libaio",
            "--direct=1",
            "--time_based=1",
            "--rw=randrw",
            "--rwmixread=50",
            "--filename=" + filename,
            "--runtime=" + str(runtime),
            "--blocksize=" + str(block_size),
            "--iodepth=" + str(iodepth),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    os.kill(env.nbs.pid, signal.SIGSTOP)
    time.sleep(downtime)
    os.kill(env.nbs.pid, signal.SIGCONT)

    proc.communicate(timeout=60)
    assert proc.returncode == 0


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
    reconnects = 3
    nbs_runtime_between_reconnects = request_timeout

    env, run = init(
        nbd_netlink=True,
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

        for i in range(0, reconnects):
            force_nbd_reconnect(
                env,
                nbd_device,
                runtime,
                nbs_downtime,
                block_size,
                iodepth)

            time.sleep(nbs_runtime_between_reconnects)

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


@pytest.mark.parametrize('nbd_netlink', [True, False])
def test_resize_device(nbd_netlink):
    volume_name = "example-disk"
    block_size = 4096
    blocks_count = 1310720
    volume_size = blocks_count * block_size
    nbd_device = "/dev/nbd0"
    socket_path = "/tmp/nbd.sock"
    iodepth = 1024
    request_timeout = 2
    runtime = request_timeout * 2
    nbs_downtime = request_timeout + 2

    env, run = init(
        nbd_netlink=nbd_netlink,
        nbd_request_timeout=request_timeout,
        max_zero_blocks_sub_request_size=512 * 1024 * 1024
    )

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

        # check if reconnected device didn't shrink back
        if nbd_netlink:
            force_nbd_reconnect(
                env,
                nbd_device,
                runtime,
                nbs_downtime,
                block_size,
                iodepth)

            try:
                fd = os.open(nbd_device, os.O_RDWR | os.O_SYNC)
                os.lseek(fd, volume_size, os.SEEK_SET)
                os.write(fd, b"foobar")
            finally:
                os.close(fd)

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
    env, run = init(
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
