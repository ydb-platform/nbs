import os
import logging

import cloud.filestore.public.sdk.python.client as client


BLOCK_SIZE = 4096
BLOCKS_COUNT = 1000


def test_create_destroy():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")
    vhost_port = os.getenv("NFS_VHOST_PORT")

    with client.CreateClient(str("localhost:%s" % port), log=logger) as nfs_client:
        nfs_client.create_filestore(
            "fs",
            "project",
            "folder",
            "cloud",
            BLOCK_SIZE,
            BLOCKS_COUNT)

    with client.CreateEndpointClient(str("localhost:%s" % vhost_port), log=logger) as vhost_client:
        vhost_client.start_endpoint("fs", "client", "tmp-xxxx")

        info = vhost_client.list_endpoints()

        assert len(info.Endpoints) == 1
        assert info.Endpoints[0].FileSystemId == "fs"
        assert info.Endpoints[0].ClientId == "client"
        assert info.Endpoints[0].SocketPath == "tmp-xxxx"

        vhost_client.stop_endpoint("tmp-xxxx")

        info = vhost_client.list_endpoints()

        assert len(info.Endpoints) == 0
