import logging
import os
import pytest

from cloud.filestore.public.sdk.python.client import Client, CreateClient
from cloud.filestore.public.sdk.python.client.error import ClientError

BLOCK_SIZE = 4096
BLOCKS_COUNT = 1000
BLOCKS_COUNT_LARGE = int(1 * 1024 * 1024 * 1024 / 4096)

ROOT_NODE_ID = 1
INVALID_HANDLE_ID = 0


@pytest.mark.parametrize("shard_count", [None, 10])
def test_parentless_create_unlink_node(shard_count):
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        nfs_client.create_filestore(
            "fs",
            "project",
            "folder",
            "cloud",
            BLOCK_SIZE,
            BLOCKS_COUNT,
            shard_count=shard_count,
        )

        session_id = nfs_client.create_session("fs").Session.SessionId.encode(
            "utf-8"
        )

        node = nfs_client.create_node(
            "fs",
            session_id,
            Client.File(0o644),
            ROOT_NODE_ID,
            "file1",
        ).Node

        assert node.Mode == 0o644
        assert node.Id >= (ROOT_NODE_ID if shard_count is None else (1 << 56))
        assert node.MTime == node.CTime
        assert node.Size == 0

        list_nodes_result = nfs_client.list_nodes(
            "fs",
            session_id,
            ROOT_NODE_ID,
        )
        # In a parentless mode, the node should not be listed under the parent
        assert len(list_nodes_result.Nodes) == 0

        # In a parentless mode, the node should be removed by its ID, not by name
        nfs_client.unlink_node(
            "fs",
            session_id,
            node.Id,
            "",
        )

        files_count = 10
        files = [
            nfs_client.create_node(
                "fs",
                session_id,
                Client.File(0o644),
                ROOT_NODE_ID,
                "file%d" % i,
            ).Node
            for i in range(files_count)
        ]
        assert len(files) == files_count

        for file_node in files:
            nfs_client.unlink_node(
                "fs",
                session_id,
                file_node.Id,
                "",
            )


@pytest.mark.parametrize("shard_count", [None, 10])
def test_handleless_read_write_data(shard_count):
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        nfs_client.create_filestore(
            "fs",
            "project",
            "folder",
            "cloud",
            BLOCK_SIZE,
            BLOCKS_COUNT,
            shard_count=shard_count,
        )

        session_id = nfs_client.create_session("fs").Session.SessionId.encode(
            "utf-8"
        )

        node = nfs_client.create_node(
            "fs",
            session_id,
            Client.File(0o644),
            ROOT_NODE_ID,
            "file1",
        ).Node

        logger.info("Created node: %s", node)

        data = b"Hello, world!"
        nfs_client.write_data(
            "fs",
            session_id,
            INVALID_HANDLE_ID,
            0,  # offset
            data,
            node_id=node.Id,
        )

        logger.info("Wrote data: %s", data)

        read_data = nfs_client.read_data(
            "fs",
            session_id,
            INVALID_HANDLE_ID,
            0,  # offset
            len(data),
            node_id=node.Id,
        ).Buffer

        assert read_data == data, f"Expected {data}, got {read_data}"

        invalid_node_id = node.Id + 1
        with pytest.raises(ClientError):
            nfs_client.read_data(
                "fs",
                session_id,
                INVALID_HANDLE_ID,
                0,  # offset
                len(data),
                node_id=invalid_node_id,
            )
