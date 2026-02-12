import logging
import os
import time

import yatest.common as common
from cloud.filestore.public.sdk.python.client import CreateClient


def create_mmap_region(nfs_client, filename, size) -> (int, str):
    base_path = os.path.join(common.output_path(), "shm")
    file_path = os.path.join(base_path, filename)

    with open(file_path, "wb") as f:
        f.truncate(size)

    response = nfs_client.mmap(file_path=filename, size=size)
    return response.Id, file_path


def test_invalidate_timed_out_regions():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(f"localhost:{port}", log=logger) as nfs_client:
        region_id, _ = create_mmap_region(
            nfs_client, "testfile_timeout", 4096
        )
        logger.info(f"Created region: {region_id}")

        regions = nfs_client.list_mmap_regions()
        assert len(regions.Regions) == 1

        # Wait for the timeout (configured to 5s in server patch) to expire.
        # Refresh cycle is 15s, so we wait a bit longer to ensure it runs.
        time.sleep(40)

        regions = nfs_client.list_mmap_regions()
        assert len(regions.Regions) == 0
