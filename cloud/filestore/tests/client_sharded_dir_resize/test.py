from concurrent import futures
import json
import logging
import os
from time import monotonic, sleep

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


def get_filesystem_topology(
    client: FilestoreCliClient,
    filesystem_id: str,
):
    return json.loads(
        client.execute_action(
            "getfilesystemtopology",
            {"FileSystemId": filesystem_id},
        )
    )


def verify_filesystem_topology(
    client: FilestoreCliClient,
    filesystem_id: str,
    expected_shard_count: int,
    directory_sharding_enabled: bool,
):
    topology = get_filesystem_topology(client, filesystem_id)
    shards = topology.get("ShardFileSystemIds", [])
    assert len(shards) == expected_shard_count
    for idx, shard_id in enumerate(shards):
        assert shard_id == f"{filesystem_id}_s{idx + 1}"
        shard_topology = get_filesystem_topology(client, shard_id)
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


def get_shard_creation_state(
    client: FilestoreCliClient,
    filesystem_id: str,
):
    result = client.execute_action(
        "unsafechangetabletstate",
        {
            "FileSystemId": filesystem_id,
            "ShardCreationState": {},
        },
    )
    return json.loads(result).get("ShardCreationState", {})


def get_shard_creation_state_version(
    client: FilestoreCliClient,
    filesystem_id: str,
):
    return get_shard_creation_state(client, filesystem_id).get("Version", 0)


def verify_shard_creation_state_range(
    client: FilestoreCliClient,
    filesystem_id: str,
    expected_base_shard_count: int,
    expected_target_shard_count: int,
):
    state = get_shard_creation_state(client, filesystem_id)
    assert state.get("BaseShardCount", 0) == expected_base_shard_count, state
    assert state.get("TargetShardCount", 0) == expected_target_shard_count, state


def wait_for_shard_creation_state_update(
    client: FilestoreCliClient,
    filesystem_id: str,
    initial_version: int,
    expected_base_shard_count: int,
    expected_target_shard_count: int,
    resize_future,
):
    deadline = monotonic() + 60
    while monotonic() < deadline:
        if resize_future.done():
            resize_error = resize_future.exception()
            if resize_error:
                raise AssertionError(
                    "resize failed before partial shard creation progress "
                    "was observed"
                ) from resize_error

            raise AssertionError(
                "resize finished before partial shard creation progress "
                "was observed"
            )

        try:
            state = get_shard_creation_state(client, filesystem_id)
        except Exception as error:
            raise AssertionError(
                "failed to read shard creation state while waiting for "
                "partial resize progress"
            ) from error

        version = state.get("Version", 0)
        if (
            version > initial_version
            and state.get("BaseShardCount", 0) == expected_base_shard_count
            and state.get("TargetShardCount", 0) == expected_target_shard_count
        ):
            return

        sleep(0.1)

    raise AssertionError(
        "timed out waiting for partial shard creation progress: "
        f"version did not advance past {initial_version} for range "
        f"[{expected_base_shard_count}, {expected_target_shard_count})"
    )


def resize_with_retry_and_restart(
    client: FilestoreCliClient,
    async_client: FilestoreCliClient,
    filesystem_id: str,
    blocks_count: int,
    previous_shard_count: int,
    shard_count: int,
):
    initial_version = get_shard_creation_state_version(client, filesystem_id)

    with futures.ThreadPoolExecutor(max_workers=2) as executor:
        resize_future = executor.submit(
            async_client.resize,
            filesystem_id,
            blocks_count,
        )
        wait_for_shard_creation_state_update(
            client,
            filesystem_id,
            initial_version,
            previous_shard_count,
            shard_count,
            resize_future,
        )

        retry_future = executor.submit(
            async_client.resize,
            filesystem_id,
            blocks_count,
        )
        # The retry and restart are intentionally close together. The test
        # treats them as chaos and does not require strict request overlap.
        client.execute_action("restarttablet", {"FileSystemId": filesystem_id})

        resize_future.result()
        retry_future.result()

    client.resize(filesystem_id, blocks_count)


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


def test_should_resize_with_retries_and_tablet_restarts():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client"
    )
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())
    async_client = FilestoreCliClient(
        binary_path,
        port,
        cwd=common.output_path(),
        check_exit_code=False,
    )

    client.create(
        "fs_resize",
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        int(SHARD_SIZE / BLOCK_SIZE) - 1,
    )

    topology = get_filesystem_topology(client, "fs_resize")
    # Read the flag from this filesystem to keep this test order-independent
    # from the test that toggles directory sharding. The main filesystem flag
    # check below is therefore tautological here; shard consistency is still
    # verified.
    directory_sharding_enabled = topology.get(
        "DirectoryCreationInShardsEnabled",
        False,
    )

    verify_filesystem_topology(
        client,
        "fs_resize",
        0,
        directory_sharding_enabled,
    )
    previous_shard_count = 0
    for shard_count in (8, 24):
        resize_with_retry_and_restart(
            client,
            async_client,
            "fs_resize",
            shard_count * int(SHARD_SIZE / BLOCK_SIZE),
            previous_shard_count,
            shard_count,
        )
        verify_filesystem_topology(
            client,
            "fs_resize",
            shard_count,
            directory_sharding_enabled,
        )
        verify_shard_creation_state_range(
            client,
            "fs_resize",
            previous_shard_count,
            shard_count,
        )
        previous_shard_count = shard_count
