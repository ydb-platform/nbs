import os
import logging

import cloud.filestore.public.sdk.python.client as client


BLOCK_SIZE = 4096
BLOCKS_COUNT = 1000
BLOCKS_COUNT_LARGE = int(1*1024*1024*1024 / 4096)


def test_create_destroy():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with client.CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
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

    with client.CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
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
