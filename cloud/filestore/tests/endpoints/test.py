import os
import logging

import cloud.filestore.public.sdk.python.client as client

from pathlib import Path


BLOCK_SIZE = 4096
BLOCKS_COUNT = 1000


def test_remove_endpoint_for_non_existing_fs():
    logger = logging.getLogger("test")
    vhost_port = os.getenv("NFS_VHOST_PORT")

    with client.CreateEndpointClient(str("localhost:%s" % vhost_port), log=logger) as vhost_client:
        info = vhost_client.list_endpoints()
        assert len(info.Endpoints) == 0
        endpoint_storage_dir = Path(os.getenv("NFS_VHOST_ENDPOINT_STORAGE_DIR", None))
        assert endpoint_storage_dir is not None
        assert len(os.listdir(endpoint_storage_dir)) == 0
