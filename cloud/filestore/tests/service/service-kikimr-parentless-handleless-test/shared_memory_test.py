import logging
import os

import pytest
import yatest.common as common

from cloud.filestore.public.sdk.python.client import CreateClient
from cloud.filestore.public.sdk.python.client.error import ClientError


def test_mmap_unmmap():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    base_path = os.path.join(common.output_path(), "shm")
    filename = "testfile"

    with CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        file_path = os.path.join(base_path, filename)
        # for non-existent file mmap should fail
        with pytest.raises(ClientError, match="No such file or directory"):
            nfs_client.mmap(file_path=filename, size=4096)

        # Create a file to mmap
        with open(file_path, "wb") as f:
            f.write(b"\0" * 8192)  # 8KB file
        response = nfs_client.mmap(file_path=filename, size=4096)
        logger.info("Successfully mmaped file, response: %s", response)
        assert response.Id > 0

        # Ensure that this id is listed in mmap regions
        regions = nfs_client.list_mmap_regions()
        logger.info("Mmap regions: %s", regions)
        assert len(regions.Regions) == 1
        assert regions.Regions[0].Id == response.Id
        assert regions.Regions[0].Size == 4096
        assert regions.Regions[0].FilePath == os.path.join(base_path, filename)

        # Unmap the non-existent id
        with pytest.raises(ClientError, match="Mmap region not found"):
            nfs_client.munmap(mmap_id=response.Id + 1)
        # Unmap the valid id
        nfs_client.munmap(mmap_id=response.Id)
        logger.info("Successfully unmapped mmap id: %d", response.Id)

        # Ensure that no regions are listed now
        regions = nfs_client.list_mmap_regions()
        logger.info("Mmap regions after unmap: %s", regions)
        assert len(regions.Regions) == 0
