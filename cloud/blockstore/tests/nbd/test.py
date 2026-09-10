import pytest
import os
import mmap
import time
import logging
import signal
import subprocess

from concurrent.futures import ThreadPoolExecutor, wait
from itertools import count

from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD, \
    IPC_NBD, VOLUME_ACCESS_READ_WRITE
from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient
from cloud.blockstore.config.client_pb2 import TClientConfig, TClientAppConfig
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType

import yatest.common as common


BLOCK_SIZE=4096


@pytest.fixture(autouse=True)
def load_nbd_module():
    subprocess.check_call(["modprobe", "nbd", "nbds_max=4"], timeout=20)


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    daemon = start_ydb()
    yield daemon
    daemon.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb, tmp_path):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    log_config = cfg.files["log"]
    log_config.Entry.add(Component=b"BLOCKSTORE_NBD", Level=7)
    log_config.Entry.add(Component=b"BLOCKSTORE_CLIENT", Level=7)

    server_config = cfg.files["server"].ServerConfig

    server_config.NbdEnabled = True
    server_config.NbdNetlink = True
    server_config.NbdRequestTimeout = 10000            # 10s
    server_config.NbdConnectionTimeout = 86400 * 1000  # 24h
    server_config.NbdDevicePrefix = "/dev/nbd"

    server_config.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server_config.EndpointStorageDir = str(tmp_path)
    server_config.AllowAllRequestsViaUDS = True

    server_config.UnixSocketPath = str(tmp_path / "grpc.sock")
    server_config.VhostEnabled = False
    server_config.AutomaticNbdDeviceManagement = True

    daemon = start_nbs(cfg)

    yield daemon

    daemon.stop()


@pytest.fixture(name='bdev')
def mount_nbd_device(nbs, tmp_path):
    test_disk_id = "vol0"
    nbd_device = "/dev/nbd0"
    client_id = common.context.test_name

    cli = CreateTestClient(f"localhost:{nbs.port}")

    cli.create_volume(
        disk_id=test_disk_id,
        block_size=BLOCK_SIZE,
        blocks_count=1024 ** 3 // BLOCK_SIZE,
        storage_media_kind=STORAGE_MEDIA_SSD)

    socket_path = str(tmp_path / f"{test_disk_id}.nbd.sock")

    cli.start_endpoint(
        unix_socket_path=socket_path,
        disk_id=test_disk_id,
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
            yield fd
        finally:
            os.close(fd)
    finally:
        cli.stop_endpoint(
            unix_socket_path=socket_path,
            disk_id=test_disk_id,
            client_id=client_id,
        )


def test_ydb_outage(ydb, bdev):

    def make_block(byte):
        return byte * BLOCK_SIZE

    with mmap.mmap(-1, BLOCK_SIZE) as wbuf, \
         mmap.mmap(-1, BLOCK_SIZE) as rbuf, \
         ThreadPoolExecutor(max_workers=1) as executor:

        def write_block():
            written = os.pwrite(bdev, wbuf, 0)
            assert written == BLOCK_SIZE

        def read_block():
            rbuf[:] = make_block(b"\xbe")
            received = os.preadv(bdev, [rbuf], 0)
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
