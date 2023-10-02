import grpc
import logging
import os
import pytest
import socket

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client.error_codes import make_grpc_error
from cloud.blockstore.public.sdk.python.client.grpc_client import GrpcClient
from cloud.blockstore.public.sdk.python.client.credentials import ClientCredentials

from cloud.blockstore.public.sdk.python.client.ut.client_methods \
    import client_methods

from yatest.common.network import PortManager


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


def _create_test_grpc_client(port, credentials=None):
    return GrpcClient(str("localhost:%d" % port), credentials, log=logger)


def _create_test_credentials():
    cert_files_dir = os.getenv("TEST_CERT_FILES_DIR")
    root_certs_file = os.path.join(cert_files_dir, "server.crt")
    cert_file = os.path.join(cert_files_dir, "server.crt")
    cert_private_key_file = os.path.join(cert_files_dir, "server.key")
    return ClientCredentials(root_certs_file, cert_file, cert_private_key_file)


def _test_every_method_impl(secure_channel, sync):
    port = 0
    if secure_channel:
        port = int(os.getenv("LOCAL_NULL_SECURE_NBS_SERVER_PORT"))
    else:
        port = int(os.getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT"))

    credentials = None
    if secure_channel:
        credentials = _create_test_credentials()

    grpc_client = _create_test_grpc_client(port, credentials)

    args_map = {
        "start_endpoint": {
            "UnixSocketPath": "./test.socket",
            "ClientId": "TestClientId"
        },
    }

    for client_method in client_methods:
        method_name = client_method[0]
        if not sync:
            method_name += "_async"
        request_name = "T%sRequest" % client_method[1]
        method = getattr(grpc_client, method_name)
        request_class = getattr(protos, request_name)
        request = request_class()

        request_dict = args_map.get(client_method[0])
        if request_dict:
            for key in request_dict:
                setattr(request, key, request_dict[key])

        try:
            if sync:
                method(request)
            else:
                method(request).result()
        except Exception as e:
            logger.exception(e)
            pytest.fail(str(e))


def test_every_method_with_insecure_channel_sync():
    _test_every_method_impl(secure_channel=False, sync=True)


def test_every_method_with_insecure_channel_async():
    _test_every_method_impl(secure_channel=False, sync=False)


def test_every_method_with_secure_channel_sync():
    _test_every_method_impl(secure_channel=True, sync=True)


def test_every_method_with_secure_channel_async():
    _test_every_method_impl(secure_channel=True, sync=False)


def test_deadline_exceeded():
    pm = PortManager()
    port = pm.get_port()

    s = socket.socket()
    s.bind(('', port))
    s.listen(1)

    grpc_client = GrpcClient("localhost:%d" % port, timeout=0, log=logger)

    request = protos.TExecuteActionRequest(
        Action='DescribeVolume',
        Input=None,
    )

    idempotence_id = None
    timestamp = None
    trace_id = None
    request_timeout = None

    future = grpc_client.execute_action_async(
        request,
        idempotence_id,
        timestamp,
        trace_id,
        request_timeout)

    error = None

    try:
        future.result()
    except ClientError as e:
        error = e.code

    assert(error == make_grpc_error(grpc.StatusCode.DEADLINE_EXCEEDED.value[0]))
