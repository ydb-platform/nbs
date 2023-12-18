import logging
import pytest

from concurrent import futures

import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.public.sdk.python.client.error import ClientError
from cloud.filestore.public.sdk.python.client.client import Client

from cloud.filestore.public.sdk.python.client.ut.client_methods \
    import client_methods


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


class FakeClientImpl(object):

    def __init__(self):
        pass


def _test_every_method_impl(sync):
    client_impl = FakeClientImpl()
    client = Client(client_impl)

    args_map = {
        "ping": {},
        "create_filestore": {
            "filesystem_id": "test",
            "project_id": "project",
            "folder_id": "folder",
            "cloud_id": "cloud",
            "block_size": 100500,
            "blocks_count": 500100,
        },
        "alter_filestore": {
            "filesystem_id": "test",
            "project_id": "xxx",
            "folder_id": "yyy",
            "cloud_id": "zzz",
        },
        "resize_filestore": {
            "filesystem_id": "test",
            "blocks_count": 100500,
        },
        "destroy_filestore": {
            "filesystem_id": "test",
        },
        "get_filestore_info": {
            "filesystem_id": "test",
        },
        "create_checkpoint": {
            "filesystem_id": "test",
            "checkpoint_id": "aaaa-bbbb-cccc-dddd",
        },
        "destroy_checkpoint": {
            "filesystem_id": "test",
            "checkpoint_id": "aaaa-bbbb-cccc-dddd",
        },
        "describe_filestore_model": {
            "block_size": 100500,
            "blocks_count": 500100,
        },
    }

    for client_method in client_methods:
        method_name = client_method[0]

        # Add method to client_impl
        type_root_name = client_method[1]
        response_name = "T%sResponse" % type_root_name

        def request_handler(
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout):

            response_class = getattr(protos, response_name)
            response = response_class()
            if sync:
                return response
            future = futures.Future()
            future.set_result(response)
            return future

        request_dict = args_map[method_name]

        if not sync:
            method_name += "_async"

        setattr(client_impl, method_name, request_handler)

        # test client's wrapper method
        client_method = getattr(client, method_name)
        try:
            if sync:
                client_method(**request_dict)
            else:
                client_method(**request_dict).result()
        except ClientError as e:
            logger.exception(e)
            pytest.fail(str(e))


def test_every_method_sync():
    _test_every_method_impl(sync=True)


def test_every_method_async():
    _test_every_method_impl(sync=False)
