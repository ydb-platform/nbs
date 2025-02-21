import json
import logging
import os
from time import sleep

import yatest.common as common

from google.protobuf import text_format
from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.config.storage_pb2 import TStorageConfig


BLOCK_SIZE = 4 * 1024
SHARD_SIZE = 1024 * 1024 * 1024


def __init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())

    results_path = common.output_path() + "/results.txt"
    return client, results_path


def get_nfs_config(name: str) -> str:
    configs_dir = os.getenv("NFS_CONFIG_DIR")
    assert configs_dir is not None, "NFS_CONFIG_DIR is not set"
    return os.path.join(configs_dir, name)


def get_nfs_storage_config_path() -> str:
    return get_nfs_config("storage.txt")


def get_storage_config() -> TStorageConfig:
    nfs_storage_config_path = get_nfs_storage_config_path()
    storage_config = TStorageConfig()
    with open(nfs_storage_config_path) as p:
        storage_config = text_format.Parse(p.read(), TStorageConfig())
    return storage_config


def set_storage_config(storage_config: TStorageConfig):
    nfs_storage_config_path = get_nfs_storage_config_path()
    with open(nfs_storage_config_path, "w") as f:
        f.write(text_format.MessageToString(storage_config))


def set_directory_sharding_enabled(directory_sharding_enabled: bool):
    logging.info(
        f"Setting DirectoryCreationInShardsEnabled to {directory_sharding_enabled}"
    )
    storage_config = get_storage_config()
    storage_config.DirectoryCreationInShardsEnabled = directory_sharding_enabled
    set_storage_config(storage_config)
    sleep(RESTART_INTERVAL * 1.5)


RESTART_INTERVAL = 20


def verify_filesystem_topology(
        client: FilestoreCliClient,
        filesystem_id: str,
        expected_shard_count: int,
        directory_sharding_enabled: bool):
    result = client.execute_action("getfilesystemtopology", {"FileSystemId": filesystem_id})
    topology = json.loads(result)
    shards = topology.get("ShardFileSystemIds", [])
    assert len(shards) == expected_shard_count
    for idx, shard_id in enumerate(shards):
        assert shard_id == f"{filesystem_id}_s{idx + 1}"
        shard_topology = json.loads(client.execute_action("getfilesystemtopology", {"FileSystemId": shard_id}))
        assert shard_topology.get("ShardNo", 0) == idx + 1
        # assert shard_topology.get("DirectoryCreationInShardsEnabled", False) == directory_sharding_enabled
    assert topology.get("DirectoryCreationInShardsEnabled", False) == directory_sharding_enabled


def test_should_correctly_maintain_sharding_types():
    client, _ = __init_test()

    client.create(
        "fs0",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1)

    client.create(
        "fs1",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1)

    verify_filesystem_topology(client, "fs0", 0, False)
    verify_filesystem_topology(client, "fs1", 0, False)
    client.resize("fs1", 2 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs1", 2, False)

    set_directory_sharding_enabled(True)
    # From now on all new filesystems should have directory sharding enabled. Old ones should not be affected.

    client.create(
        "fs2",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1)


    client.create(
        "fs3",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        3 * int(SHARD_SIZE / BLOCK_SIZE))

    verify_filesystem_topology(client, "fs2", 0, True)
    verify_filesystem_topology(client, "fs3", 3, True)

    sleep(10)
    client.resize("fs0", 2 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs0", 2, False)

    client.resize("fs1", 3 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs1", 3, False)

    client.resize("fs2", 2 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs2", 2, True)

    client.resize("fs3", 4 * int(SHARD_SIZE / BLOCK_SIZE))
    verify_filesystem_topology(client, "fs3", 4, True)
