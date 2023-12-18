import datetime
import grpc
import logging

from concurrent import futures

from .base_client import _BaseClient, _BaseEndpointClient

from .error_codes import EResult

from .error import ClientError, _handle_errors

from .request import _next_request_id, request_name, _AsyncResponse

from cloud.filestore.public.api.grpc.service_pb2_grpc import TFileStoreServiceStub, TEndpointManagerServiceStub


MAX_MESSAGE_SIZE = 8 * 1024 * 1024


def _to_camel_case(method_name):
    message_name = method_name[0].upper()

    i = 1
    while i < len(method_name):
        c = method_name[i]
        if c == '_':
            i += 1
            message_name += method_name[i].upper()
        else:
            message_name += c
        i += 1

    return message_name.replace("Filestore", "FileStore")


class _GrpcClient(_BaseClient):

    def __init__(
            self,
            endpoint,
            stub,
            credentials=None,
            timeout=None,
            log=None):

        try:
            super(_GrpcClient, self).__init__(endpoint, timeout)

            opts = [
                ("grpc.max_message_length", MAX_MESSAGE_SIZE),
                ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
                ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE)
            ]

            if credentials is not None:
                self.channel = grpc.secure_channel(
                    self.endpoint,
                    credentials.get_ssl_channel_credentials(),
                    options=opts)
            else:
                self.channel = grpc.insecure_channel(
                    self.endpoint,
                    options=opts)

            if log is not None:
                self.log = log
            else:
                self.log = logging.getLogger("grpc_client")

            self.stub = stub(self.channel)

        except Exception as e:
            if log is not None:
                log.exception("Can't create client")
            raise ClientError(EResult.E_FAIL.value, str(e))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.channel.close()

    def _resolve(self, method_name):
        return getattr(self.stub, _to_camel_case(method_name)).future

    @_handle_errors
    def _send_request(
            self,
            request,
            call,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request_id = _next_request_id()
        self._setup_headers(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        self.log.debug(
            "%s #%d sending request",
            request_name(request),
            request_id)
        started = datetime.datetime.now()
        future = call(request, self.timeout)
        return _AsyncResponse(future, request, request_id, started)

    @_handle_errors
    def _execute_request_async(
            self,
            path,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        call = self._resolve(path)

        async_response = self._send_request(
            request,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        future = futures.Future()

        def set_result(f):
            try:
                response = self._process_response(
                    f.result(),
                    async_response.request,
                    async_response.request_id,
                    async_response.started)
                future.set_result(response)
            except Exception as e:
                future.set_exception(e)

        async_response.future.add_done_callback(set_result)
        return future

    @_handle_errors
    def _execute_request(
            self,
            path,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        call = self._resolve(path)

        async_response = self._send_request(
            request,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return self._process_response(
            async_response.future.result(),
            async_response.request,
            async_response.request_id,
            async_response.started)


# FIXME: code dup
class _GrpcEndpointClient(_BaseEndpointClient):

    def __init__(
            self,
            endpoint,
            stub,
            credentials=None,
            timeout=None,
            log=None):

        try:
            super(_GrpcEndpointClient, self).__init__(endpoint, timeout)

            opts = [
                ("grpc.max_message_length", MAX_MESSAGE_SIZE),
                ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
                ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE)
            ]

            if credentials is not None:
                self.channel = grpc.secure_channel(
                    self.endpoint,
                    credentials.get_ssl_channel_credentials(),
                    options=opts)
            else:
                self.channel = grpc.insecure_channel(
                    self.endpoint,
                    options=opts)

            if log is not None:
                self.log = log
            else:
                self.log = logging.getLogger("grpc_client")

            self.stub = stub(self.channel)

        except Exception as e:
            if log is not None:
                log.exception("Can't create client")
            raise ClientError(EResult.E_FAIL.value, str(e))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.channel.close()

    def _resolve(self, method_name):
        return getattr(self.stub, _to_camel_case(method_name)).future

    @_handle_errors
    def _send_request(
            self,
            request,
            call,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request_id = _next_request_id()
        self._setup_headers(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        self.log.debug(
            "%s #%d sending request",
            request_name(request),
            request_id)
        started = datetime.datetime.now()
        future = call(request, self.timeout)
        return _AsyncResponse(future, request, request_id, started)

    @_handle_errors
    def _execute_request_async(
            self,
            path,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        call = self._resolve(path)

        async_response = self._send_request(
            request,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        future = futures.Future()

        def set_result(f):
            try:
                response = self._process_response(
                    f.result(),
                    async_response.request,
                    async_response.request_id,
                    async_response.started)
                future.set_result(response)
            except Exception as e:
                future.set_exception(e)

        async_response.future.add_done_callback(set_result)
        return future

    @_handle_errors
    def _execute_request(
            self,
            path,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        call = self._resolve(path)

        async_response = self._send_request(
            request,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return self._process_response(
            async_response.future.result(),
            async_response.request,
            async_response.request_id,
            async_response.started)


def CreateGrpcClient(endpoint, credentials=None, timeout=None, log=None):
    return _GrpcClient(endpoint, TFileStoreServiceStub, credentials, timeout, log)


def CreateGrpcEndpointClient(endpoint, credentials=None, timeout=None, log=None):
    return _GrpcEndpointClient(endpoint, TEndpointManagerServiceStub, credentials, timeout, log)
