import logging
import pytest

from concurrent import futures

import cloud.filestore.public.sdk.python.protos as protos

from cloud.filestore.public.sdk.python.client.error_codes import EResult
from cloud.filestore.public.sdk.python.client.error import ClientError, \
    client_error_from_response
from cloud.filestore.public.sdk.python.client.durable import DurableClient

from cloud.filestore.public.sdk.python.client.ut.client_methods \
    import client_methods

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


class FakeGrpcClient(object):
    def __init__(self):
        pass


def _test_retry_stopped_after_timeout(sync):
    grpc_client = FakeGrpcClient()

    timeout = 0.1
    timeout_increment = 0
    durable_client = DurableClient(
        grpc_client, timeout, timeout_increment, logger)

    for client_method in client_methods:
        method_name = client_method[0]

        # Add method to grpc_client
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
            response.Error.Code = EResult.E_REJECTED.value

            future = futures.Future()
            error = client_error_from_response(response)
            if error.failed:
                future.set_exception(error)
            else:
                future.set_result(response)
            return future

        setattr(grpc_client, method_name + "_async", request_handler)

        # test durable client's method
        request_name = "T%sRequest" % type_root_name
        request_class = getattr(protos, request_name)
        request = request_class()

        if not sync:
            method_name += "_async"
        durable_method = getattr(durable_client, method_name)
        succ = 0
        try:
            if sync:
                response = durable_method(request)
            else:
                response = durable_method(request).result()
            if response.Error.Code == EResult.S_OK.value:
                succ = succ + 1
        except ClientError:
            pass

        assert succ == 0


def _test_retry_undelivered_request_every_method_impl(sync):
    grpc_client = FakeGrpcClient()

    timeout = 1
    timeout_increment = 0
    durable_client = DurableClient(
        grpc_client, timeout, timeout_increment, logger)

    for client_method in client_methods:
        method_name = client_method[0]

        # Add method to grpc_client
        type_root_name = client_method[1]
        response_name = "T%sResponse" % type_root_name

        class ctx:
            max_requests_count = 3
            request_count = 1

        def request_handler(
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout):

            response_class = getattr(protos, response_name)
            response = response_class()
            if ctx.request_count < ctx.max_requests_count:
                ctx.request_count += 1
                response.Error.Code = EResult.E_REJECTED.value

            future = futures.Future()
            error = client_error_from_response(response)
            if error.failed:
                future.set_exception(error)
            else:
                future.set_result(response)
            return future

        setattr(grpc_client, method_name + "_async", request_handler)

        # test durable client's method
        request_name = "T%sRequest" % type_root_name
        request_class = getattr(protos, request_name)
        request = request_class()

        if not sync:
            method_name += "_async"
        durable_method = getattr(durable_client, method_name)
        try:
            if sync:
                durable_method(request)
            else:
                durable_method(request).result()
        except ClientError as e:
            logger.exception(e)
            pytest.fail(str(e))

        assert ctx.request_count == ctx.max_requests_count


def _test_no_retry_unretriable_requests_every_method_impl(sync):
    grpc_client = FakeGrpcClient()

    timeout = 1
    timeout_increment = 0
    durable_client = DurableClient(
        grpc_client, timeout, timeout_increment, logger)

    for client_method in client_methods:
        method_name = client_method[0]

        # Add method to grpc_client
        class ctx:
            request_count = 0

        def request_handler(
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout):

            ctx.request_count += 1
            future = futures.Future()
            error = ClientError(EResult.E_FAIL.value, "")
            future.set_exception(error)
            return future

        setattr(grpc_client, method_name + "_async", request_handler)

        # test durable client's method
        request_name = "T%sRequest" % client_method[1]
        request_class = getattr(protos, request_name)
        request = request_class()

        if not sync:
            method_name += "_async"
        durable_method = getattr(durable_client, method_name)

        request_failed = False
        try:
            if sync:
                durable_method(request)
            else:
                durable_method(request).result()
        except ClientError:
            request_failed = True

        assert request_failed is True
        assert ctx.request_count == 1


def test_retry_undelivered_request_sync():
    _test_retry_undelivered_request_every_method_impl(sync=True)


def test_retry_undelivered_request_async():
    _test_retry_undelivered_request_every_method_impl(sync=False)


def test_no_retry_unretriable_requests_sync():
    _test_no_retry_unretriable_requests_every_method_impl(sync=True)


def test_no_retry_unretriable_requests_async():
    _test_no_retry_unretriable_requests_every_method_impl(sync=False)
