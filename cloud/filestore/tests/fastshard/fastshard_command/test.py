import json
import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.fs import FsItem, fill_fs, DIR, FILE

BLOCK_SIZE = 4 * 1024
SHARD_SIZE = 1024 * 1024 * 1024
SHARD_COUNT = 3
FILE_SHARD_COUNT = 1

FS_ID = "fs0"


def __init_test():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(binary_path, port, cwd=common.output_path())
    client_nocheck = FilestoreCliClient(
        binary_path,
        port,
        cwd=common.output_path(),
        check_exit_code=False)

    return client, client_nocheck


def __configure_fastshards(client):
    shard_ids = []
    file_shard_ids = []
    for i in range(SHARD_COUNT):
        shard_id = "%s_s%s" % (FS_ID, i + 1)
        if i >= SHARD_COUNT - FILE_SHARD_COUNT:
            file_shard_ids.append(shard_id)
        shard_ids.append(shard_id)

    for i, shard_id in enumerate(shard_ids):
        client.execute_action(
            "configureasshard",
            {
                "FileSystemId": shard_id,
                "ShardNo": i + 1,
                "MainFileSystemId": FS_ID,
                "ShardFileSystemIds": shard_ids,
                "FileShardFileSystemIds": file_shard_ids,
                "IsFastShard": shard_id in file_shard_ids,
                "FastShardConfig": {"MemConfig": {}},
                "DirectoryCreationInShardsEnabled": True,
            },
        )

    client.execute_action(
        "configureshards",
        {
            "FileSystemId": FS_ID,
            "ShardFileSystemIds": shard_ids,
            "FileShardFileSystemIds": file_shard_ids,
            "DirectoryCreationInShardsEnabled": True,
        },
    )

    return file_shard_ids


def __fast_shard_command(client, shard_id, command):
    request = {"FileSystemId": shard_id}
    request.update(command)
    output = client.execute_action("fastshardcommand", request)
    return json.loads(output)


def __collect_stats(client, shard_id):
    response = __fast_shard_command(client, shard_id, {"CollectStats": {}})
    return response.get("Stats", {})


def test_fastshard_command():
    client, client_nocheck, = __init_test()

    client.create(
        FS_ID,
        "test_cloud",
        "test_folder",
        BLOCK_SIZE,
        SHARD_COUNT * int(SHARD_SIZE / BLOCK_SIZE))

    file_shard_ids = __configure_fastshards(client)
    assert len(file_shard_ids) == FILE_SHARD_COUNT

    #
    # A non-fast-shard filesystem must reject the command.
    #

    res = client_nocheck.execute_action(
        "fastshardcommand", {"FileSystemId": FS_ID, "CollectStats": {}})
    assert b"not a fast shard" in res

    #
    # An empty fast shard: stats are collectable and zero, the layout dump
    # is a json document (empty for the mem shard).
    #

    for shard_id in file_shard_ids:
        stats = __collect_stats(client, shard_id)
        assert int(stats.get("UsedNodeCount", 0)) == 0

        layout = __fast_shard_command(
            client, shard_id, {"DumpLayoutJson": {}})
        json.loads(layout["LayoutJson"])

    #
    # Populate the filesystem - the files land in the fast shards.
    #

    items = [
        FsItem("/d0", DIR, None),
        FsItem("/d0/f0.txt", FILE, "some data 0"),
        FsItem("/d0/f1.txt", FILE, "some data 1"),
        FsItem("/f2.txt", FILE, "some data 2"),
    ]
    fill_fs(client, FS_ID, items)

    used_nodes = sum(
        int(__collect_stats(client, shard_id).get("UsedNodeCount", 0))
        for shard_id in file_shard_ids)
    assert used_nodes == 3

    #
    # Format must wipe the fast shards.
    #

    for shard_id in file_shard_ids:
        __fast_shard_command(client, shard_id, {"Format": {}})

    for shard_id in file_shard_ids:
        stats = __collect_stats(client, shard_id)
        assert int(stats.get("UsedNodeCount", 0)) == 0
        assert int(stats.get("UsedPageCount", 0)) == 0

    #
    # The file nodes are gone: stat through the main filesystem fails
    # because the fast shard no longer knows the node behind the ref.
    #

    res = client_nocheck.stat(FS_ID, "/d0/f0.txt")
    assert b'"Id"' not in res

    #
    # The shards stay usable: the same paths can be filled again.
    #

    fill_fs(
        client,
        FS_ID,
        [FsItem("/d0/f3.txt", FILE, "some data 3")])

    used_nodes = sum(
        int(__collect_stats(client, shard_id).get("UsedNodeCount", 0))
        for shard_id in file_shard_ids)
    assert used_nodes == 1
