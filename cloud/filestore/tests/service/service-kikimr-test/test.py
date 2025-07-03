import os
import logging

from cloud.filestore.public.sdk.python.client import Client, CreateClient
from cloud.filestore.public.sdk.python.client.error import ClientError
import cloud.storage.core.protos.media_pb2 as media
import cloud.filestore.public.sdk.python.protos as protos

BLOCK_SIZE = 4096
BLOCKS_COUNT = 1000
BLOCKS_COUNT_LARGE = int(1*1024*1024*1024 / 4096)


def test_create_destroy():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        nfs_client.create_filestore(
            "fs",
            "project",
            "folder",
            "cloud",
            BLOCK_SIZE,
            BLOCKS_COUNT)

        info = nfs_client.get_filestore_info("fs")

        assert info.FileStore.FileSystemId == "fs"
        assert info.FileStore.ProjectId == "project"
        assert info.FileStore.FolderId == "folder"
        assert info.FileStore.CloudId == "cloud"
        assert info.FileStore.BlockSize == BLOCK_SIZE
        assert info.FileStore.BlocksCount == BLOCKS_COUNT

        nfs_client.destroy_filestore("fs")


def test_alter():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        nfs_client.create_filestore(
            "fs",
            "project",
            "folder",
            "cloud",
            BLOCK_SIZE,
            BLOCKS_COUNT)

        nfs_client.alter_filestore("fs", "xxx", "yyy", "zzz")

        info = nfs_client.get_filestore_info("fs")
        assert info.FileStore.FileSystemId == "fs"
        assert info.FileStore.ProjectId == "xxx"
        assert info.FileStore.FolderId == "yyy"
        assert info.FileStore.CloudId == "zzz"
        assert info.FileStore.BlockSize == BLOCK_SIZE
        assert info.FileStore.BlocksCount == BLOCKS_COUNT

        nfs_client.destroy_filestore("fs")


def test_resize():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        nfs_client.create_filestore(
            "fs",
            "project",
            "folder",
            "cloud",
            BLOCK_SIZE,
            BLOCKS_COUNT)

        nfs_client.resize_filestore("fs", BLOCKS_COUNT * 100)

        info = nfs_client.get_filestore_info("fs")
        assert info.FileStore.FileSystemId == "fs"
        assert info.FileStore.ProjectId == "project"
        assert info.FileStore.FolderId == "folder"
        assert info.FileStore.CloudId == "cloud"
        assert info.FileStore.BlockSize == BLOCK_SIZE
        assert info.FileStore.BlocksCount == BLOCKS_COUNT * 100

        nfs_client.destroy_filestore("fs")


def test_describe_model():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        model = nfs_client.describe_filestore_model(
            BLOCK_SIZE,
            BLOCKS_COUNT_LARGE,
            media.STORAGE_MEDIA_SSD)

        assert model.FileStoreModel.BlockSize == BLOCK_SIZE
        assert model.FileStoreModel.BlocksCount == BLOCKS_COUNT_LARGE
        assert model.FileStoreModel.StorageMediaKind == media.STORAGE_MEDIA_SSD


ROOT_NODE_ID = 1


def test_create_unlink_node():
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
        )

        session_id = nfs_client.create_session(
            "fs"
        ).Session.SessionId.encode("utf-8")

        node = nfs_client.create_node(
            "fs",
            session_id,
            Client.File(0o644),
            ROOT_NODE_ID,
            "file1",
        ).Node

        assert node.Mode == 0o644
        assert node.Id == ROOT_NODE_ID + 1
        assert node.MTime == node.CTime
        assert node.Size == 0

        list_nodes_result = nfs_client.list_nodes(
            "fs",
            session_id,
            ROOT_NODE_ID,
        )

        assert len(list_nodes_result.Nodes) == 1

        directory_id = nfs_client.create_node(
            "fs",
            session_id,
            Client.Directory(0o755),
            ROOT_NODE_ID,
            "dir1",
        ).Node.Id

        files_count = 10
        for i in range(files_count):
            nfs_client.create_node(
                "fs",
                session_id,
                Client.File(0o644),
                directory_id,
                "file%d" % i,
            )
        list_nodes_result = nfs_client.list_nodes(
            "fs",
            session_id,
            directory_id,
        )
        assert len(list_nodes_result.Nodes) == files_count
        assert set(list_nodes_result.Names) == set(
            [b"file%d" % i for i in range(files_count)]
        )

        # unlink half of files
        for i in range(files_count // 2):
            nfs_client.unlink_node(
                "fs",
                session_id,
                directory_id,
                "file%d" % i,
            )

        # check that the remaining half of files are still there
        list_nodes_result = nfs_client.list_nodes(
            "fs",
            session_id,
            directory_id,
        )
        assert len(list_nodes_result.Nodes) == files_count // 2
        assert set(list_nodes_result.Names) == set(
            [b"file%d" % i for i in range(files_count // 2, files_count)]
        )


def test_read_write_data():
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

        handle = nfs_client.create_handle(
            "fs",
            session_id,
            node.Id,
            flags=Client.CreateHandleFlags(
                [
                    protos.TCreateHandleRequest.EFlags.E_READ,
                    protos.TCreateHandleRequest.EFlags.E_WRITE,
                ]
            ),
        ).Handle

        logger.info("Created handle: %s", handle)

        data = b"Hello, world!"
        nfs_client.write_data(
            "fs",
            session_id,
            handle,
            0,  # offset
            data,
        )

        logger.info("Wrote data: %s", data)

        read_data = nfs_client.read_data(
            "fs",
            session_id,
            handle,
            0,  # offset
            len(data),
        ).Buffer

        assert read_data == data, f"Expected {data}, got {read_data}"

        read_only_handle = nfs_client.create_handle(
            "fs",
            session_id,
            node.Id,
            flags=Client.CreateHandleFlags(
                [protos.TCreateHandleRequest.EFlags.E_READ]
            ),
        ).Handle

        # Reading data with read-only handle should succeed
        read_only_data = nfs_client.read_data(
            "fs",
            session_id,
            read_only_handle,
            0,  # offset
            len(data),
        ).Buffer
        assert read_only_data == data, f"Expected {data}, got {read_only_data}"

        # Writing data with read-only handle should raise an error
        try:
            nfs_client.write_data(
                "fs",
                session_id,
                read_only_handle,
                0,  # offset
                b"New data",
            )
        except ClientError as e:
            logger.info("Expected error on write with read-only handle: %s", e)
        else:
            raise AssertionError(
                "Write with read-only handle did not raise an error"
            )
