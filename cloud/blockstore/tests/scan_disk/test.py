import json
import time

import pytest

import cloud.blockstore.public.sdk.python.protos as protos
import cloud.blockstore.tests.python.lib.daemon as daemon

from cloud.blockstore.public.sdk.python.client import ClientError, Session
from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from library.python.retry import retry


BLOCK_SIZE = 4096
BLOCKS_PER_RANGE = 1024
# SSD writes of at least 128 KiB go to merged blobs, one blob per write.
BLOCKS_PER_BLOB = 32
SCAN_TIMEOUT_SEC = 120


@pytest.fixture(name='ydb')
def start_ydb_cluster():
    ydb_cluster = daemon.start_ydb()
    yield ydb_cluster
    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):
    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    nbs = daemon.start_nbs(cfg)
    yield nbs
    nbs.kill()


@pytest.fixture(name='client')
def create_client(nbs):
    return CreateTestClient(f"localhost:{nbs.port}")


def scan_disk(client, disk_id, **settings):
    client.execute_ScanDisk(DiskId=disk_id, **settings)


def get_scan_disk_status(client, disk_id):
    output = client.execute_GetScanDiskStatus(DiskId=disk_id)
    return json.loads(output)["Progress"]


def wait_for_scan_disk(client, disk_id):
    deadline = time.monotonic() + SCAN_TIMEOUT_SEC
    while True:
        progress = get_scan_disk_status(client, disk_id)
        if progress.get("IsCompleted"):
            return progress
        assert time.monotonic() < deadline, f"scan hangs: {progress}"
        time.sleep(0.5)


def counter(progress, name):
    # JSON keeps uint64 values as strings and drops zero values.
    return int(progress.get(name, 0))


@retry(max_times=10, exception=ClientError)
def create_volume(client, disk_id, blocks_count):
    client.create_volume(
        disk_id=disk_id,
        block_size=BLOCK_SIZE,
        blocks_count=blocks_count,
        storage_media_kind=protos.STORAGE_MEDIA_SSD)


def write_blob(session, start_index, fill):
    session.write_blocks(
        start_index,
        [bytes([fill]) * BLOCK_SIZE] * BLOCKS_PER_BLOB)


def make_volume_with_dead_blobs(client, disk_id):
    create_volume(client, disk_id, 4 * BLOCKS_PER_RANGE)

    session = Session(client, disk_id, "")
    session.mount_volume()

    overwritten = 0
    untouched = BLOCKS_PER_RANGE
    zeroed = 2 * BLOCKS_PER_RANGE

    write_blob(session, overwritten, 1)
    write_blob(session, overwritten, 2)
    write_blob(session, untouched, 3)
    write_blob(session, zeroed, 4)
    session.zero_blocks(zeroed, BLOCKS_PER_BLOB)

    session.unmount_volume()


def test_check_integrity_scan_checks_live_blobs(client):
    make_volume_with_dead_blobs(client, "vol0")

    scan_disk(
        client,
        "vol0",
        BatchSize=100,
        CheckIntegrity=True,
        MaxInFlight=8,
        CheckTimeoutMs=30000,
        MaxRetries=3,
        RetryTimeoutMs=100,
        MaxReportedBlobs=100,
        MaxDataInfoSize=256)

    progress = wait_for_scan_disk(client, "vol0")
    assert progress.get("CheckIntegrity")
    assert counter(progress, "ScanCommitId") > 0
    assert counter(progress, "Processed") == counter(progress, "Total")
    assert counter(progress, "DeadBlobs") == 2
    assert counter(progress, "CheckedBlobs") == 2
    assert counter(progress, "UncheckedBlobs") == 0
    assert counter(progress, "DegradedBlobs") == 0
    assert not progress.get("Cancelled")
    assert not progress.get("BrokenBlobs")
    assert not progress.get("BlobResults")


def test_read_mode_scan_keeps_working(client):
    make_volume_with_dead_blobs(client, "vol0")

    scan_disk(client, "vol0", BatchSize=100)

    progress = wait_for_scan_disk(client, "vol0")
    assert not progress.get("CheckIntegrity")
    assert counter(progress, "Processed") == counter(progress, "Total")
    assert counter(progress, "CheckedBlobs") == 0
    assert counter(progress, "DeadBlobs") == 0
    assert not progress.get("BrokenBlobs")


def test_cancel_scan_disk(client):
    blob_count = 100
    create_volume(client, "vol0", blob_count * BLOCKS_PER_RANGE)

    session = Session(client, "vol0", "")
    session.mount_volume()
    for i in range(blob_count):
        write_blob(session, i * BLOCKS_PER_RANGE, 1)
    session.unmount_volume()

    # Nothing to cancel yet.
    scan_disk(client, "vol0", Cancel=True)

    scan_disk(
        client,
        "vol0",
        BatchSize=1,
        CheckIntegrity=True,
        MaxInFlight=1)
    scan_disk(client, "vol0", Cancel=True)

    progress = wait_for_scan_disk(client, "vol0")
    assert progress.get("CheckIntegrity")
    if progress.get("Cancelled"):
        assert counter(progress, "CheckedBlobs") < blob_count
    else:
        assert counter(progress, "CheckedBlobs") == blob_count

    # A cancelled scan can be started again with other settings.
    scan_disk(
        client,
        "vol0",
        BatchSize=100,
        CheckIntegrity=True,
        MaxInFlight=8)

    progress = wait_for_scan_disk(client, "vol0")
    assert not progress.get("Cancelled")
    assert counter(progress, "CheckedBlobs") == blob_count
    assert counter(progress, "DeadBlobs") == 0
    assert not progress.get("BlobResults")
