import os
import math
import pytest
import json

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient

BLOCK_SIZE = 4 * 1024
SHARD_ALLOCATION_UNIT = 8 * 1024 * 1024
FILE_SYSTEM_SIZE = SHARD_ALLOCATION_UNIT * 4


def __init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())

    results_path = common.output_path() + "/results.txt"
    return client, results_path


def __create_file(file_name, size):
    data = "a" * size
    with open(file_name, "w") as f:
        f.write(data)


def __check_storage_stats(src, nodes_count, used_blocks_count, total_blocks_count):
    json_string = str(src, 'utf-8')
    json_data = json.loads(json_string)
    assert int(json_data["Stats"]["UsedNodesCount"]) == nodes_count
    assert int(json_data["Stats"]["UsedBlocksCount"]) == used_blocks_count
    assert int(json_data["Stats"]["TotalBlocksCount"]) == total_blocks_count

    return (str(json_data["Stats"]["ShardStats"]) + "\n").encode()


def test_strict_size():
    client, results_path = __init_test()
    total_blocks = int(FILE_SYSTEM_SIZE / BLOCK_SIZE)
    out = client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        total_blocks)

    files_count = 13
    file_size = math.floor(FILE_SYSTEM_SIZE / files_count)
    data_file = os.path.join(common.output_path(), "data.txt")
    __create_file(data_file, file_size)

    out += client.mkdir("fs0", "/aaa")
    for i in range(files_count):
        client.write("fs0", "/aaa/file" + str(i), "--data", data_file)

    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0"})
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0_s1"})
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0_s2"})
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0_s3"})
    out += client.execute_action(
        "getfilesystemtopology", {"FileSystemId": "fs0_s4"})

    # call 'getstoragestats' in order to update AggregateUsedBytesCount in the
    # main filesystem and shards
    used_blocks = files_count * math.ceil(file_size / BLOCK_SIZE)
    out += __check_storage_stats(client.execute_action(
        "getstoragestats", {"FileSystemId": "fs0"}),
        files_count + 1, used_blocks, total_blocks)
    out += __check_storage_stats(client.execute_action(
        "getstoragestats", {"FileSystemId": "fs0_s1", "Mode": 1}),
        files_count + 1, used_blocks, total_blocks)
    out += __check_storage_stats(client.execute_action(
        "getstoragestats", {"FileSystemId": "fs0_s2", "Mode": 1}),
        files_count + 1, used_blocks, total_blocks)
    out += __check_storage_stats(client.execute_action(
        "getstoragestats", {"FileSystemId": "fs0_s3", "Mode": 1}),
        files_count + 1, used_blocks, total_blocks)
    out += __check_storage_stats(client.execute_action(
        "getstoragestats", {"FileSystemId": "fs0_s4", "Mode": 1}),
        files_count + 1, used_blocks, total_blocks)

    # one more file should overflow the filesystem
    with pytest.raises(common.process.ExecutionError) as e:
        client.write("fs0", "/aaa/file" + str(files_count), "--data", data_file)

    err_str = str(e)
    assert err_str.find("(NCloud::TServiceError) no space left") != -1

    os.remove(data_file)

    with open(results_path, "wb") as results_file:
        results_file.write(out)

    ret = common.canonical_file(results_path, local=True)
    return ret
