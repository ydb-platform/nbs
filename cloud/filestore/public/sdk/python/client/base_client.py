import datetime
import uuid
import time

import cloud.filestore.public.sdk.python.protos as protos

from .request import request_name, request_size
from .error import _handle_errors, client_error_from_response


DEFAULT_REQUEST_TIMEOUT = 30
REQUEST_TIME_WARN_THRESHOLD = 10

CLIENT_METHODS = [
    "ping",
    "create_filestore",
    "alter_filestore",
    "resize_filestore",
    "destroy_filestore",
    "get_filestore_info",
    "create_checkpoint",
    "destroy_checkpoint",
    "describe_filestore_model",
    "execute_action",

    # session methods
    "create_session",

    # node methods
    "create_node",
    "list_nodes",
    "unlink_node",

    # read/write methods
    "create_handle",
    "read_data",
    "write_data",
]

ENDPOINT_METHODS = [
    "start_endpoint",
    "stop_endpoint",
    "list_endpoints",
    "kick_endpoint",
    "ping",
]


def __create_sync_method_impl(method_name):
    def method_impl(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        if hasattr(self, '_execute_request'):
            return self._execute_request(
                method_name,
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)

        return self._execute_request_async(
            method_name,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).result()

    return method_impl


def __create_async_method_impl(method_name):
    def method_impl(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        return self._execute_request_async(
            method_name,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
    return method_impl


def dispatch_client_methods(Cls):
    for method_name in CLIENT_METHODS:
        if not hasattr(Cls, method_name):
            setattr(Cls, method_name, __create_sync_method_impl(method_name))
        if not hasattr(Cls, method_name + '_async'):
            setattr(Cls, method_name + '_async',
                    __create_async_method_impl(method_name))
    return Cls


def dispatch_endpoint_methods(Cls):
    for method_name in ENDPOINT_METHODS:
        if not hasattr(Cls, method_name):
            setattr(Cls, method_name, __create_sync_method_impl(method_name))
        if not hasattr(Cls, method_name + '_async'):
            setattr(Cls, method_name + '_async',
                    __create_async_method_impl(method_name))
    return Cls


class _Base(object):

    def __init__(self, endpoint, timeout):

        self.__endpoint = endpoint
        self.__client_id = uuid.uuid4().bytes

        self.__timeout = DEFAULT_REQUEST_TIMEOUT
        if timeout is not None:
            self.__timeout = timeout

    def _setup_headers(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        if request.Headers is None:
            request.Headers = protos.THeaders()

        request.Headers.ClientId = self.__client_id

        if idempotence_id is not None:
            request.Headers.IdempotenceId = idempotence_id

        now = datetime.datetime.now()
        if timestamp is None or \
                timestamp > now or \
                now - timestamp > datetime.timedelta(seconds=1):
            # fix request timestamp
            timestamp = now

        # convert datetime.datetime to timestamp in microseconds
        request.Headers.Timestamp = int(
            time.mktime(timestamp.timetuple())) * 1000000

        if trace_id is not None:
            request.Headers.TraceId = trace_id

        if request_timeout is None:
            request_timeout = self.__timeout

        # convert seconds to milliseconds
        request.Headers.RequestTimeout = request_timeout * 1000000

    @_handle_errors
    def _process_response(self, response, request, request_id, started):
        request_time = datetime.datetime.now() - started
        error = client_error_from_response(response)

        if request_time < datetime.timedelta(seconds=REQUEST_TIME_WARN_THRESHOLD):
            self.log.debug(
                "%s #%d request completed (time: %s, size: %d, error: %s)",
                request_name(request),
                request_id,
                request_time,
                request_size(request),
                error)
        else:
            self.log.warning(
                "%s #%d request too slow (time: %s, size: %d, error: %s)",
                request_name(request),
                request_id,
                request_time,
                request_size(request),
                error)

        if error.failed:
            raise error

        return response

    @property
    def client_id(self):
        return self.__client_id

    @property
    def timeout(self):
        return self.__timeout

    @property
    def endpoint(self):
        return self.__endpoint


@dispatch_client_methods
class _BaseClient(_Base):

    def __init__(self, endpoint, timeout):
        super(_BaseClient, self).__init__(endpoint, timeout)


@dispatch_endpoint_methods
class _BaseEndpointClient(_Base):

    def __init__(self, endpoint, timeout):
        super(_BaseEndpointClient, self).__init__(endpoint, timeout)
