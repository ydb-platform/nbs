import os

import yatest.common as common

from cloud.filestore.private.api.protos.tablet_pb2 import TShardTopology
from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.fs import request_tablet


def make_shard_topology(shard_ids, file_shard_ids):
    shards = []
    for shard_id in shard_ids:
        shard = {
            "ShardId": shard_id,
            "Type": TShardTopology.E_ORDINARY,
            "Role": 1 << (TShardTopology.E_DIRECTORY - 1),
        }
        if shard_id in file_shard_ids:
            shard["Type"] = TShardTopology.E_FASTSHARD
            shard["Role"] = 1 << (TShardTopology.E_FILE - 1)
        shards.append(shard)
    return {"Shards": shards}


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
    topology = make_shard_topology(shard_ids, file_shard_ids)

    for i, shard_id in enumerate(shard_ids):
        shard_no = i + 1
        request = {
            "FileSystemId": shard_id,
            "ShardNo": shard_no,
            "MainFileSystemId": filesystem,
            "ShardTopology": topology,
            "IsFastShard": shard_id in file_shard_ids,
            "DirectoryCreationInShardsEnabled": True,
        }
        if shard_id in file_shard_ids:
            request["FastShardConfig"] = fast_shard_config

        client.execute_action("configureasshard", request)

    client.execute_action(
        "configureshards",
        {
            "FileSystemId": filesystem,
            "ShardTopology": topology,
            "DirectoryCreationInShardsEnabled": True,
        },
    )

    return file_shard_ids


def fetch_layout(tablet_id):
    response = request_tablet(tablet_id, "action=fastShardLayoutJson")
    return response.json()
