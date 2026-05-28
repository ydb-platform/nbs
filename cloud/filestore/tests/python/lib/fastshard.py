import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient


def configure_fastshard(shard_count, file_shard_count, fast_shard_config):
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(
        binary_path,
        port,
        cwd=common.output_path())

    filesystem = os.getenv("NFS_FILESYSTEM")
    shard_ids = []
    file_shard_ids = []
    for i in range(shard_count):
        shard_no = i + 1
        shard_id = "%s_s%s" % (filesystem, shard_no)
        if i >= shard_count - file_shard_count:
            file_shard_ids.append(shard_id)
        shard_ids.append(shard_id)

    for i, shard_id in enumerate(shard_ids):
        shard_no = i + 1
        client.execute_action(
            "configureasshard",
            {
                "FileSystemId": shard_id,
                "ShardNo": shard_no,
                "ShardFileSystemIds": shard_ids,
                "FileShardFileSystemIds": file_shard_ids,
                "IsFastShard": shard_id in file_shard_ids,
                "FastShardConfig": fast_shard_config,
                "DirectoryCreationInShardsEnabled": True,
            },
        )

    client.execute_action(
        "configureshards",
        {
            "FileSystemId": filesystem,
            "ShardFileSystemIds": shard_ids,
            "FileShardFileSystemIds": file_shard_ids,
            "DirectoryCreationInShardsEnabled": True,
        },
    )
