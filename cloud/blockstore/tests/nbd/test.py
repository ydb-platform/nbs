import pytest
import os
import mmap
import time
import logging
import signal
import subprocess
import tempfile
import fcntl
import struct

from concurrent.futures import ThreadPoolExecutor, wait
from itertools import count
from pathlib import Path

from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD, \
    IPC_NBD, VOLUME_ACCESS_READ_WRITE
from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType

import yatest.common as common


BLOCK_SIZE = 4096
NBD_REQUEST_TIMEOUT_SECONDS = 10
BLKGETSIZE64 = 0x80081272


@pytest.fixture(autouse=True)
def load_nbd_module():
    subprocess.check_call(["modprobe", "nbd", "nbds_max=4"], timeout=20)


@pytest.fixture
def sockets_dir():
    with tempfile.TemporaryDirectory(prefix='nbd-', dir="/tmp") as path:
        yield Path(path)


@pytest.fixture(name='ydb')
def start_ydb_cluster():
    daemon = start_ydb()
    yield daemon
    daemon.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb, tmp_path, sockets_dir):
    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    log_config = cfg.files["log"]
    log_config.Entry.add(Component=b"BLOCKSTORE_NBD", Level=7)
    log_config.Entry.add(Component=b"BLOCKSTORE_CLIENT", Level=7)

    server_config = cfg.files["server"].ServerConfig

    server_config.NbdEnabled = True
    server_config.NbdNetlink = True
    server_config.NbdRequestTimeout = NBD_REQUEST_TIMEOUT_SECONDS * 1000
    server_config.NbdConnectionTimeout = 86400 * 1000  # 24h
    server_config.NbdDevicePrefix = "/dev/nbd"

    server_config.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server_config.EndpointStorageDir = str(tmp_path)
    server_config.AllowAllRequestsViaUDS = True

    server_config.UnixSocketPath = str(sockets_dir / "grpc.sock")
    server_config.VhostEnabled = False
    server_config.AutomaticNbdDeviceManagement = True

    daemon = start_nbs(cfg)

    yield daemon

    daemon.stop()


@pytest.fixture(name='volume')
def create_volume(nbs, sockets_dir):
    disk_id = "vol0"
    nbd_device = "/dev/nbd0"
    client_id = common.context.test_name
    blocks_count = 1024 ** 3 // BLOCK_SIZE

    cli = CreateTestClient(f"localhost:{nbs.port}")

    try:
        cli.create_volume(
            disk_id=disk_id,
            block_size=BLOCK_SIZE,
            blocks_count=blocks_count,
            storage_media_kind=STORAGE_MEDIA_SSD)

        socket_path = str(sockets_dir / f"{disk_id}.nbd.sock")

        cli.start_endpoint(
            unix_socket_path=socket_path,
            disk_id=disk_id,
            ipc_type=IPC_NBD,
            access_mode=VOLUME_ACCESS_READ_WRITE,
            client_id=client_id,
            seq_number=0,
            persistent=True,
            nbdDeviceFile=nbd_device,
        )

        try:
            fd = os.open(nbd_device, os.O_RDWR | os.O_DIRECT)
            try:
                yield fd, cli, disk_id, socket_path, blocks_count
            finally:
                os.close(fd)
        finally:
            cli.stop_endpoint(
                unix_socket_path=socket_path,
                disk_id=disk_id,
                client_id=client_id,
            )
    finally:
        cli.close()


def test_ydb_outage(ydb, volume):
    fd, *_ = volume

    def make_block(byte):
        return byte * BLOCK_SIZE

    with mmap.mmap(-1, BLOCK_SIZE) as wbuf, \
         mmap.mmap(-1, BLOCK_SIZE) as rbuf, \
         ThreadPoolExecutor(max_workers=1) as executor:

        def write_block():
            written = os.pwrite(fd, wbuf, 0)
            assert written == BLOCK_SIZE

        def read_block():
            rbuf[:] = make_block(b"\xbe")
            received = os.preadv(fd, [rbuf], 0)
            assert received == BLOCK_SIZE

        # Verify that the initially empty disk reads as zeros.
        read_block()
        assert rbuf[:] == make_block(b"\x00")

        # Write a block.
        expected = make_block(b"\x10")
        wbuf[:] = expected
        write_block()

        # Read the block back and verify the data.
        read_block()
        assert rbuf[:] == expected

        # Suspend YDB; subsequent requests are expected to block.
        for node_id, node in ydb.nodes.items():
            logging.info("Suspending YDB node #%s: %s", node_id, node)
            os.kill(node.pid, signal.SIGSTOP)
        try:
            logging.info("Submitting a write request while YDB is suspended")
            # Submit a write that should remain blocked while YDB is suspended.
            expected = make_block(b"\x42")
            wbuf[:] = expected
            future = executor.submit(write_block)

            # Verify that the write does not complete within 1 minute.
            logging.info("Verifying that the write remains pending for 1 minute")
            done, _ = wait([future], timeout=60)
            if done:
                future.result()  # Propagate any worker exception.
                raise AssertionError(
                    "The write completed while YDB was suspended"
                )
        finally:
            # Resume YDB even if a check fails.
            for node_id, node in ydb.nodes.items():
                logging.info("Resuming YDB node #%s: %s", node_id, node)
                os.kill(node.pid, signal.SIGCONT)

        logging.info("Waiting for the pending write to complete after YDB resumes")
        # The pending write should now complete.
        future.result()
        logging.info("Pending write completed")

        # Immediately overwrite the block with new data.
        logging.info("Overwriting the block with the final data pattern")
        expected = make_block(b"\xef")
        wbuf[:] = expected
        write_block()

        # Verify that the new data is not overwritten by a delayed write.
        dt = 120
        t0 = time.monotonic()
        deadline = t0 + dt

        logging.info("Checking for delayed overwrites for {dt} seconds")
        for i in count():
            read_block()
            assert rbuf[:] == expected, (
                f"Data mismatch at iteration #{i}, "
                f"{time.monotonic() - t0:.3f}s after the final write"
            )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.5, remaining))

        logging.info(
            "Verification passed: %s reads over %.3f seconds, no data mismatches",
            i + 1,
            time.monotonic() - t0,
        )


def get_device_size(fd):
    return struct.unpack("=Q", fcntl.ioctl(fd, BLKGETSIZE64, bytes(8)))[0]


def test_restore_resized_endpoint_with_pending_io(nbs, volume):
    fd, cli, disk_id, socket_path, old_blocks_count = volume
    new_blocks_count = 2 * old_blocks_count
    old_size = old_blocks_count * BLOCK_SIZE
    new_size = new_blocks_count * BLOCK_SIZE

    with mmap.mmap(-1, BLOCK_SIZE) as wbuf, \
         mmap.mmap(-1, BLOCK_SIZE) as rbuf, \
         ThreadPoolExecutor(max_workers=1) as executor:

        def write_block(offset=0):
            assert os.pwrite(fd, wbuf, offset) == BLOCK_SIZE

        def read_block(offset=0):
            rbuf[:] = b"\xbe" * BLOCK_SIZE
            assert os.preadv(fd, [rbuf], offset) == BLOCK_SIZE

        expected = b"\x42" * BLOCK_SIZE
        assert get_device_size(fd) == old_size
        wbuf[:] = expected
        write_block()
        read_block()
        assert rbuf[:] == expected

        # Do not refresh endpoint, just resize the volume. Restart should
        # install new socket and only then update geometry.
        cli.resize_volume(
            disk_id=disk_id,
            blocks_count=new_blocks_count,
            channels_count=0,
            config_version=0)
        common.wait_for(
            lambda: (cli.describe_volume(disk_id).BlocksCount == new_blocks_count),
            timeout=30,
            fail_message="volume resize")
        assert get_device_size(fd) == old_size

        logging.info("Suspending NBS and submitting a direct read")
        os.kill(nbs.pid, signal.SIGSTOP)

        try:
            # Keep issuing requests until NBS gets fully suspended.
            freeze_deadline = time.monotonic() + 10
            while True:
                assert time.monotonic() < freeze_deadline, "I/O did not freeze"
                future = executor.submit(read_block)
                done, _ = wait([future], timeout=1)
                if not done:
                    break
                future.result()
                assert rbuf[:] == expected

            # Wait for requests to time out.
            done, _ = wait([future], timeout=2*NBD_REQUEST_TIMEOUT_SECONDS)
            if done:
                future.result()
                raise AssertionError("The read completed before NBS recovery")

            logging.info("Killing NBS and restoring persistent endpoints")
            nbs.kill()
            nbs.start()

        finally:
            if nbs.is_alive():
                os.kill(nbs.pid, signal.SIGCONT)

        # Wait for endpoint restoration.
        future.result(timeout=120)
        assert rbuf[:] == expected
        common.wait_for(
            lambda: any(x.UnixSocketPath == socket_path for x in cli.list_endpoints()),
            timeout=120,
            fail_message="automatic endpoint restoration")

        # Verify new capacity and I/O beyond the old size boundary.
        assert get_device_size(fd) == new_size
        expected = b"\xa5" * BLOCK_SIZE
        wbuf[:] = expected
        write_block(offset=old_size)
        read_block(offset=old_size)
        assert rbuf[:] == expected
