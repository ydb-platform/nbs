import datetime
import typing
import uuid
import time

from google.protobuf.message import Message

import cloud.blockstore.public.sdk.python.protos as protos

from .request import request_name, request_size
from .error import _handle_errors, client_error_from_response


DEFAULT_REQUEST_TIMEOUT = 30
REQUEST_TIME_WARN_THRESHOLD = 10

NBS_CLIENT_METHODS = [
    "ping",
    "create_volume",
    "destroy_volume",
    "resize_volume",
    "alter_volume",
    "assign_volume",
    "stat_volume",
    "mount_volume",
    "unmount_volume",
    "read_blocks",
    "write_blocks",
    "zero_blocks",
    "create_checkpoint",
    "delete_checkpoint",
    "get_changed_blocks",
    "describe_volume",
    "describe_volume_model",
    "list_endpoints",
    "list_volumes",
    "discover_instances",
    "query_available_storage",
    "create_volume_from_device",
    "resume_device",
    "start_endpoint",
    "stop_endpoint",
    "refresh_endpoint",
    "execute_action",
    "kick_endpoint",
    "cms_action",
    "update_disk_registry_config",
    "describe_disk_registry_config",
    "query_agents_info",
    "list_disks_states",
]


def __create_sync_method_impl(method_name: str) -> typing.Callable:
    def method_impl(
            self,
            request: type[Message],
            idempotence_id: str | None = None,
            timestamp: datetime.datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

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


def __create_async_method_impl(method_name: str) -> typing.Callable:
    def method_impl(
            self,
            request: type[Message],
            idempotence_id: str | None = None,
            timestamp: datetime.datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> typing.Callable:

        return self._execute_request_async(
            method_name,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
    return method_impl


def dispatch_nbs_client_methods(Cls):
    for method_name in NBS_CLIENT_METHODS:
        if not hasattr(Cls, method_name):
            setattr(Cls, method_name, __create_sync_method_impl(method_name))
        if not hasattr(Cls, method_name + '_async'):
            setattr(Cls, method_name + '_async', __create_async_method_impl(method_name))
    return Cls


@dispatch_nbs_client_methods
class _BaseClient(object):

    def __init__(self, endpoint: str, timeout: int | None):

        self.__endpoint = endpoint
        self.__client_id = str(uuid.uuid4())

        self.__timeout = DEFAULT_REQUEST_TIMEOUT
        if timeout is not None:
            self.__timeout = timeout

    def _setup_headers(
            self,
            request: type[Message],
            idempotence_id: str | None = None,
            timestamp: datetime.datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        if request.Headers is None:
            request.Headers = protos.THeaders()

        if request.Headers.ClientId == "":
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
        request.Headers.Timestamp = int(time.mktime(timestamp.timetuple())) * 1000000

        if trace_id is not None:
            request.Headers.TraceId = trace_id

        if request_timeout is None:
            request_timeout = self.__timeout

        # convert seconds to milliseconds
        request.Headers.RequestTimeout = request_timeout * 1000000

    @_handle_errors
    def _process_response(
            self,
            response: type[Message],
            request: type[Message],
            request_id: int,
            started: datetime.datetime) -> type[Message]:

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
    def client_id(self) -> str:
        return self.__client_id

    @property
    def timeout(self) -> int:
        return self.__timeout

    @property
    def endpoint(self) -> str:
        return self.__endpoint
