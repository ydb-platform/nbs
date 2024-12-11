import base64
import logging
import uuid

import yatest.common as common

import cloud.filestore.public.sdk.python.protos as protos

from library.python.testing.recipe import declare_recipe, set_env

from pathlib import Path

logger = logging.getLogger(__name__)


def start(argv):
    endpoint_storage_dir = Path(common.work_path() + '/endpoints-' + str(uuid.uuid4()))
    endpoint_storage_dir.mkdir()
    set_env("NFS_VHOST_ENDPOINT_STORAGE_DIR", str(endpoint_storage_dir))
    socket_path = "tmp-xxxx"

    start_endpoint_request = protos.TStartEndpointRequest()
    start_endpoint_request.Endpoint.FileSystemId = "fs"
    start_endpoint_request.Endpoint.ClientId = "client"
    start_endpoint_request.Endpoint.SocketPath = socket_path
    start_endpoint_request.Endpoint.Persistent = True
    # base64 trailing symbol for encoded endpoint ids is ','
    endpoint_id = base64.b64encode(socket_path.encode()).decode().replace("=", ",")
    endpoint = endpoint_storage_dir / endpoint_id
    logger.error("MYAGKOV: test endpoint path: %s", endpoint)
    endpoint.write_bytes(start_endpoint_request.SerializeToString())


def stop(argv):
    pass


if __name__ == "__main__":
    declare_recipe(start, stop)
