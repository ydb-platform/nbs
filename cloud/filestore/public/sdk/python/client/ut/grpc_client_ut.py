import grpc
import logging
import os
import pytest
import socket

import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.public.sdk.python.client.grpc_client import CreateGrpcClient

from cloud.filestore.public.sdk.python.client.ut.client_methods import client_methods

from yatest.common.network import PortManager


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


def _create_test_grpc_client(port, credentials=None):
    return CreateGrpcClient(str("localhost:%d" % port), credentials, log=logger)


def _test_every_method_impl(sync):
    port = int(os.getenv("NFS_SERVER_PORT"))
    credentials = None

    with _create_test_grpc_client(port, credentials) as grpc_client:
        for client_method in client_methods:
            method_name = client_method[0]
            if not sync:
                method_name += "_async"
            request_name = "T%sRequest" % client_method[1]
            method = getattr(grpc_client, method_name)
            request_class = getattr(protos, request_name)
            request = request_class()

            try:
                if sync:
                    method(request)
                else:
                    method(request).result()
            except Exception as e:
                logger.exception(e)
                pytest.fail(str(e))


def test_every_method_sync():
    _test_every_method_impl(sync=True)


def test_every_method_async():
    _test_every_method_impl(sync=False)


def test_deadline_exceeded():
    pm = PortManager()
    port = pm.get_port()

    s = socket.socket()
    s.bind(('', port))
    s.listen(1)

    with CreateGrpcClient("localhost:%d" % port, timeout=0, log=logger) as grpc_client:
        request = protos.TPingRequest()

        idempotence_id = None
        timestamp = None
        trace_id = None
        request_timeout = None

        future = grpc_client.ping_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        error = None
        try:
            future.result()
        except Exception as e:
            error = e.code()

        assert (error == grpc.StatusCode.DEADLINE_EXCEEDED)
