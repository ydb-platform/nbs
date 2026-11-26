import collections
import json
import os
import subprocess
import time

import pytest

import cloud.blockstore.public.sdk.python.protos as protos
import cloud.blockstore.tests.python.lib.daemon as daemon
import yatest.common as yatest_common

from cloud.blockstore.public.sdk.python.client import ClientError, Session
from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from library.python.retry import retry


CLIENT_PATH = yatest_common.binary_path(
    "cloud/blockstore/apps/client/blockstore-client")

BLOCK_SIZE = 4096
# SSD writes of at least 128 KiB go to merged blobs, one blob per write.
BLOCKS_PER_BLOB = 32
BLOBS_PER_RANGE = 1024 // BLOCKS_PER_BLOB
CLIENT_TIMEOUT_SEC = 300
SCAN_TIMEOUT_SEC = 300
POLL_INTERVAL_SEC = 0.5
# Enough batches of one blob that a restart lands in the middle of the scan.
RESTART_BLOB_COUNT = 1000


@pytest.fixture(name='ydb')
def start_ydb_cluster():
    ydb_cluster = daemon.start_ydb()
    yield ydb_cluster
    ydb_cluster.stop()


def make_nbs_config(ydb, checksums=False):
    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()
    if checksums:
        cfg.files["storage"].DiskPrefixLengthWithBlockChecksumsInBlobs = \
            1024 ** 3
    return cfg


@pytest.fixture(name='nbs_config')
def create_nbs_config(request, ydb):
    return make_nbs_config(ydb, checksums=getattr(request, 'param', False))


@pytest.fixture(name='nbs')
def start_nbs_daemon(nbs_config):
    nbs = daemon.start_nbs(nbs_config)
    yield nbs
    nbs.kill()


@pytest.fixture(name='client')
def create_client(nbs):
    return CreateTestClient(f"localhost:{nbs.port}")


@retry(max_times=10, exception=ClientError)
def create_volume(client, disk_id, blocks_count):
    client.create_volume(
        disk_id=disk_id,
        block_size=BLOCK_SIZE,
        blocks_count=blocks_count,
        storage_media_kind=protos.STORAGE_MEDIA_SSD)


def write_blobs(client, disk_id, blob_count, overwritten_blob_count=0):
    session = Session(client, disk_id, "")
    session.mount_volume()

    def write_blob(blob_index, fill):
        session.write_blocks(
            blob_index * BLOCKS_PER_BLOB,
            [bytes([fill]) * BLOCK_SIZE] * BLOCKS_PER_BLOB)

    for i in range(blob_count):
        write_blob(i, 1)
    # Each overwrite leaves one dead blob behind.
    for i in range(overwritten_blob_count):
        write_blob(i, 2)

    session.unmount_volume()


def make_volume(client, disk_id, blob_count, overwritten_blob_count=0):
    range_count = (blob_count + BLOBS_PER_RANGE - 1) // BLOBS_PER_RANGE
    create_volume(client, disk_id, range_count * 1024)
    write_blobs(client, disk_id, blob_count, overwritten_blob_count)


def execute_action(nbs, action, action_input):
    command = [
        CLIENT_PATH, "executeaction",
        "--action", action,
        "--input-bytes", json.dumps(action_input),
        "--host", "localhost",
        "--port", str(nbs.port),
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=CLIENT_TIMEOUT_SEC)
    return result.returncode, result.stdout


def start_scan_disk(nbs, scan_input):
    returncode, output = execute_action(nbs, "scandisk", scan_input)
    assert returncode == 0, output
    return json.loads(output)["OperationId"]


# Returns None when this nbsd has no such scan.
def get_scan_disk_status(nbs, operation_id, acked_record_id):
    returncode, output = execute_action(
        nbs,
        "getscandiskstatus",
        {"OperationId": operation_id, "AckedRecordId": acked_record_id})
    if returncode != 0 and "E_NOT_FOUND" in output:
        return None
    assert returncode == 0, output
    return json.loads(output)


def save_record(records_file, record):
    records_file.write(json.dumps(record) + "\n")
    records_file.flush()
    os.fsync(records_file.fileno())


def make_resume_fields(record):
    if "Error" in record:
        cursor = record.get("Cursor", "")
    else:
        cursor = record["NextCursor"]
    return {"Cursor": cursor, "MaxCommitId": record["ScanCommitId"]}


class TPartitionScan:
    """Scans one partition the way a sweep script does: saves every record
    before acknowledging it, and starts the scan again from the last saved
    record when the scan is gone."""

    def __init__(self, nbs, disk_id, records_path, **fields):
        self.nbs = nbs
        self.scan_input = {"DiskId": disk_id, "PartitionIndex": 0, **fields}
        self.records_path = records_path
        self.records = []
        self.starts = 0

    def is_done(self):
        return bool(self.records) and (
            self.records[-1].get("IsCompleted") or
            "Error" in self.records[-1])

    def run(self, on_poll=lambda records: None):
        deadline = time.monotonic() + SCAN_TIMEOUT_SEC
        with open(self.records_path, "a") as records_file:
            while not self.is_done():
                self._scan_once(records_file, on_poll, deadline)
        return self.records

    def _scan_once(self, records_file, on_poll, deadline):
        scan_input = dict(self.scan_input)
        if self.records:
            scan_input.update(make_resume_fields(self.records[-1]))
        operation_id = start_scan_disk(self.nbs, scan_input)
        self.starts += 1

        acked_record_id = 0
        while time.monotonic() < deadline:
            status = get_scan_disk_status(
                self.nbs, operation_id, acked_record_id)
            if status is None:
                return

            for record in status.get("Records", []):
                save_record(records_file, record)
                self.records.append(record)
                acked_record_id = int(record["RecordId"])

            if status.get("IsCompleted"):
                # The acknowledgement of the last record stops the scan.
                get_scan_disk_status(self.nbs, operation_id, acked_record_id)
                assert get_scan_disk_status(
                    self.nbs, operation_id, acked_record_id) is None
                return

            on_poll(self.records)
            time.sleep(POLL_INTERVAL_SEC)

        assert False, "the scan didn't finish in time"


def scan_partition(nbs, disk_id, name, on_poll=lambda records: None, **fields):
    records_path = yatest_common.output_path(f"scan-{name}.jsonl")
    scan = TPartitionScan(nbs, disk_id, records_path, **fields)
    records = scan.run(on_poll)

    # The saved file alone is enough to resume from.
    with open(records_path) as f:
        assert [json.loads(line) for line in f] == records

    return scan, records


def sum_counters(records):
    total = collections.Counter()
    for record in records:
        for name, value in record.get("Counters", {}).items():
            total[name] += int(value)
    return total


def check_full_counts(records, live_blob_count, dead_blob_count):
    assert records[-1].get("IsCompleted"), records[-1]
    for record in records:
        assert "Error" not in record, record
        assert record.get("BlobResults", []) == [], record

    total = sum_counters(records)
    assert total["CheckedBlobs"] == live_blob_count
    assert total["DeadBlobs"] == dead_blob_count
    assert total["BrokenBlobs"] == 0
    assert total["UncheckedBlobs"] == 0
    assert total["UnconfirmedBlobs"] == 0
    assert total["DeletedDuringScan"] == 0
    return total


def get_partition_tablet_id(client, disk_id):
    output = client.execute_DescribeVolume(DiskId=disk_id)
    return json.loads(output)["Partitions"][0]["TabletId"]


# Calls restart once, as soon as the scan has saved a record but isn't done.
def make_restart_on_first_record(restart):
    restarted = []

    def on_poll(records):
        if records and not restarted:
            restarted.append(True)
            restart()

    return on_poll, restarted


def test_scan_disk(client, nbs):
    make_volume(client, "vol0", blob_count=10, overwritten_blob_count=3)

    scan, records = scan_partition(nbs, "vol0", "full", BatchSize=4)

    check_full_counts(records, live_blob_count=10, dead_blob_count=3)
    assert len(records) >= 3
    assert scan.starts == 1
    for record in records:
        # JSON leaves out the fields with default values.
        assert record.get("PartitionIndex", 0) == 0
        assert int(record["ScanCommitId"]) > 0


def test_scan_disk_survives_tablet_restart(client, nbs):
    make_volume(client, "vol0", RESTART_BLOB_COUNT)
    tablet_id = get_partition_tablet_id(client, "vol0")

    on_poll, restarted = make_restart_on_first_record(
        lambda: client.execute_KillTablet(TabletId=tablet_id))
    scan, records = scan_partition(
        nbs, "vol0", "tablet-restart", on_poll, BatchSize=1, MaxInFlight=1)

    assert restarted, "the scan finished before the restart"
    # The scan runs in nbsd, so a partition restart doesn't end it.
    assert scan.starts == 1
    check_full_counts(
        records,
        live_blob_count=RESTART_BLOB_COUNT,
        dead_blob_count=0)


def test_scan_disk_survives_nbs_restart(client, nbs):
    make_volume(client, "vol0", RESTART_BLOB_COUNT)

    def restart_nbs():
        nbs.kill()
        nbs.start()

    on_poll, restarted = make_restart_on_first_record(restart_nbs)
    scan, records = scan_partition(
        nbs, "vol0", "nbs-restart", on_poll, BatchSize=1, MaxInFlight=1)

    assert restarted, "the scan finished before the restart"
    # The restarted nbsd has no such scan, so the caller starts it again from
    # its last saved record.
    assert scan.starts == 2
    check_full_counts(
        records,
        live_blob_count=RESTART_BLOB_COUNT,
        dead_blob_count=0)


@pytest.mark.parametrize('nbs_config', [True], indirect=True, ids=['checksums'])
def test_scan_disk_verifies_checksums(client, nbs):
    blob_count = 10
    make_volume(client, "vol0", blob_count, overwritten_blob_count=2)

    _, records = scan_partition(nbs, "vol0", "checksums", VerifyChecksums=True)

    total = check_full_counts(
        records,
        live_blob_count=blob_count,
        dead_blob_count=2)
    assert total["ChecksumVerifiedBlobs"] == blob_count
    assert total["ChecksumMismatchBlobs"] == 0
    assert total["ReadFailedBlobs"] == 0
    assert total["ChecksumNotVerifiedBlobs"] == 0
