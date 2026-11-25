import json
import logging
import os
import random
import time

import pytest
import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient

BLOCK_SIZE = 4 * 1024
BLOCKS_COUNT = 1024 * 1024 * 1024 // BLOCK_SIZE
FILE_SIZE = 1024 * 1024
FILE_COUNT = 4

# RangeIdHasherType -> blocks per range (and per blob for aligned writes)
BLOCK_GROUP_SIZES = {1: 64, 2: 128, 3: 256}


def _init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())

    seed = time.time()
    random.seed(seed)
    logging.info(f"Run test with seed: {seed}")

    return client


def _get_tablet_ids(client, fs_id):
    topology = json.loads(client.execute_action(
        "getfilesystemtopology",
        {"FileSystemId": fs_id}))
    return [fs_id] + topology.get("ShardFileSystemIds", [])


def _set_range_id_hasher_type(client, fs_id, hasher_type):
    for tablet_id in _get_tablet_ids(client, fs_id):
        client.execute_action("unsafechangetabletstate", {
            "FileSystemId": tablet_id,
            "RangeIdHasherType": hasher_type,
        })


def _restart_tablets(client, fs_id):
    for tablet_id in _get_tablet_ids(client, fs_id):
        client.execute_action("restarttablet", {"FileSystemId": tablet_id})


def _get_mixed_blobs_count(client, fs_id):
    # the main tablet sums its own stats and the stats of all shards
    stats = json.loads(client.execute_action(
        "getstoragestats",
        {"FileSystemId": fs_id}))
    return int(stats.get("Stats", {}).get("MixedBlobsCount", 0))


def _write(client, fs_id, path, data, offset=0):
    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "wb") as f:
        f.write(data)

    client.write(fs_id, path, "--data", data_file, "--offset", f"{offset}")


def _check_files(client, fs_id, files):
    for path, expected in files.items():
        result = client.read(fs_id, path, "--length", f"{len(expected)}")
        assert expected == result, path


@pytest.mark.parametrize("shard_count", [0, 2])
@pytest.mark.parametrize("hasher_type", [2, 3])
def test_set_range_id_hasher_type(hasher_type, shard_count):
    client = _init_test()
    fs_id = f"fs_{hasher_type}_{shard_count}"
    blobs_per_file = FILE_SIZE // (BLOCK_GROUP_SIZES[hasher_type] * BLOCK_SIZE)

    client.create(fs_id, "test_cloud", "test_folder", BLOCK_SIZE, BLOCKS_COUNT)
    if shard_count:
        client.resize(fs_id, BLOCKS_COUNT, shard_count=shard_count)
    assert len(_get_tablet_ids(client, fs_id)) == shard_count + 1

    _set_range_id_hasher_type(client, fs_id, hasher_type)

    files = {}
    for i in range(FILE_COUNT):
        path = f"/file{i}"
        files[path] = random.randbytes(FILE_SIZE)
        _write(client, fs_id, path, files[path])

    assert FILE_COUNT * blobs_per_file == _get_mixed_blobs_count(client, fs_id)
    _check_files(client, fs_id, files)

    # the filesystem is not empty anymore
    with pytest.raises(common.ExecutionError):
        _set_range_id_hasher_type(client, fs_id, 1)

    # unaligned writes go through fresh blocks / bytes
    for path, data in files.items():
        write_size = random.randint(1, 10000)
        offset = random.randint(0, FILE_SIZE - write_size)
        logging.info(f"Write {path} with size {write_size} and offset {offset}")
        patch = random.randbytes(write_size)
        _write(client, fs_id, path, patch, offset)
        files[path] = data[:offset] + patch + data[offset + write_size:]

    for tablet_id in _get_tablet_ids(client, fs_id):
        client.forced_compaction(tablet_id)
    _check_files(client, fs_id, files)

    # resize keeps the type
    client.resize(fs_id, BLOCKS_COUNT * 2)
    blobs_count = _get_mixed_blobs_count(client, fs_id)
    files["/file_after_resize"] = random.randbytes(FILE_SIZE)
    _write(client, fs_id, "/file_after_resize", files["/file_after_resize"])
    assert blobs_count + blobs_per_file == _get_mixed_blobs_count(client, fs_id)

    _restart_tablets(client, fs_id)
    _check_files(client, fs_id, files)

    client.destroy(fs_id)
