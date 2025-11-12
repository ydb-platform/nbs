import logging
import os

import pytest

from cloud.filestore.public.sdk.python.client import CreateClient
from cloud.filestore.public.sdk.python.client.error import ClientError


def test_shared_memory_not_implemented():
    # TODO(4649): remove this stub test and implement actual ones
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        with pytest.raises(ClientError, match="not implemented"):
            nfs_client.mmap(
                file_path="/test/buffer",
                size=4096
            )

        with pytest.raises(ClientError, match="not implemented"):
            nfs_client.munmap(mmap_id="0xdeadbeef")

        with pytest.raises(ClientError, match="not implemented"):
            nfs_client.list_mmap_regions()
