import json
import logging
import os
from time import sleep

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.test_helpers import (
    get_restart_interval,
    get_storage_config,
    write_storage_config,
)


BLOCK_SIZE = 4 * 1024
SHARD_SIZE = 1024 * 1024 * 1024


def enable_directory_sharding():
    logging.info("Setting DirectoryCreationInShardsEnabled to true")
    storage_config = get_storage_config()
    storage_config.DirectoryCreationInShardsEnabled = True
    write_storage_config(storage_config)
    sleep(get_restart_interval() * 1.5)


def verify_filesystem_topology(
    client: FilestoreCliClient,
    filesystem_id: str,
    expected_shard_count: int,
    directory_sharding_enabled: bool,
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
        # ShardFileSystemIds should be empty for shards with directory
        # sharding feature disabled and non-empty otherwise
        if directory_sharding_enabled:
            assert shard_topology.get("ShardFileSystemIds", []) == shards
        else:
            assert len(shard_topology.get("ShardFileSystemIds", [])) == 0
        assert (
            shard_topology.get("DirectoryCreationInShardsEnabled", False)
            == directory_sharding_enabled
        )
    assert (
        topology.get("DirectoryCreationInShardsEnabled", False)
        == directory_sharding_enabled
    )


def test_should_correctly_maintain_sharding_types():
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

    client.create(
        "fs1",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1,
    )

    verify_filesystem_topology(client, "fs0", 0, False)
    verify_filesystem_topology(client, "fs1", 0, False)
    client.resize("fs1", 2 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs1", 2, False)

    enable_directory_sharding()
    # From now on all new filesystems should have directory sharding enabled. Old ones should not be affected.

    client.create(
        "fs2",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1,
    )

    client.create(
        "fs3",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        3 * int(SHARD_SIZE / BLOCK_SIZE),
    )

    verify_filesystem_topology(client, "fs2", 0, True)
    verify_filesystem_topology(client, "fs3", 3, True)

    client.resize("fs0", 2 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs0", 2, False)

    client.resize("fs1", 3 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs1", 3, False)

    client.resize("fs2", 2 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs2", 2, True)

    client.resize("fs3", 4 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs3", 4, True)
