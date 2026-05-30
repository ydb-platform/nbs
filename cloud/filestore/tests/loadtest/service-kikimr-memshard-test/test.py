import os
import tempfile

import yatest.common as common

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.loadtest import run_load_test


SHARD_ID = "memshard-test_s1"


def configure_memshard():
    port = os.getenv("NFS_SERVER_PORT")
    binary_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")
    client = FilestoreCliClient(
        binary_path,
        port,
        cwd=common.output_path())

    client.execute_action(
        "configureasshard",
        {
            "FileSystemId": SHARD_ID,
            "ShardNo": 1,
            "ShardFileSystemIds": [SHARD_ID],
            "FileShardFileSystemIds": [SHARD_ID],
            "IsFastShard": True,
            "FastShardConfig": {"MemConfig": {}},
            "DirectoryCreationInShardsEnabled": False,
        },
    )


def make_config(fast_shard_port):
    template_path = common.source_path(
        "cloud/filestore/tests/loadtest/service-kikimr-memshard-test"
        "/memshard-read-write.txt"
    )
    with open(template_path) as f:
        content = f.read().replace("{fast_shard_port}", str(fast_shard_port))

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False, dir=common.output_path())
    tmp.write(content)
    tmp.close()
    return tmp.name


def test_memshard_read_write():
    configure_memshard()

    fast_shard_port = int(os.getenv("NFS_FAST_SHARD_PORT"))
    run_load_test(
        "memshard-read-write",
        make_config(fast_shard_port),
        os.getenv("NFS_SERVER_PORT"),
    )
