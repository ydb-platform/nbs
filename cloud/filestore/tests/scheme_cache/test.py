import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient


BLOCK_SIZE = 4 * 1024
SHARD_SIZE = 1024 * 1024 * 1024


def verify_filesystem_topology(
    client: FilestoreCliClient,
    filesystem_id: str,
    expected_shard_count: int,
):
    result = client.execute_action(
        "getfilesystemtopology", {"FileSystemId": filesystem_id}
    )
    topology = json.loads(result)
    shards = topology.get("ShardFileSystemIds", [])
    assert len(shards) == expected_shard_count
    for idx, shard_id in enumerate(shards):
        assert shard_id == f"{filesystem_id}_s{idx + 1}"
        shard_topology = json.loads(
            client.execute_action(
                "getfilesystemtopology", {"FileSystemId": shard_id}
            )
        )
        assert shard_topology.get("ShardNo", 0) == idx + 1
        assert shard_topology.get("ShardFileSystemIds", []) == shards


def verify_io(client: FilestoreCliClient, filesystem_id: str):
    path = "/test-file"

    data = "some data"
    data_file = os.path.join(common.output_path(), "data.txt")
    with open(data_file, "w") as f:
        f.write(data)

    client.touch(filesystem_id, path)
    client.write(filesystem_id, path, "--offset", "0", "--data", data_file)

    result = client.read(
        filesystem_id,
        path,
        "--offset",
        "0",
        "--length",
        str(len(data)),
    )

    assert result.decode("utf-8") == data


def test_should_resize_and_alter():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client"
    )
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())

    client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1,
    )

    verify_filesystem_topology(client, "fs0", 0)

    shard_count = 10

    client.resize("fs0", shard_count * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs0", shard_count)

    client.execute_action(
        "alterfilesystem",
        {
            "FileSystemId": "fs0",
            "Config": {
                "CloudId": "new_test_cloud",
                "FolderId": "new_test_folder",
            },
        },
    )

    verify_io(client, "fs0")

    client.destroy("fs0")
