import datetime
import logging

from concurrent import futures
from threading import Timer

from .error import ClientError, _handle_errors, client_error_from_response
from .request import request_name, request_details, _RetryState
from .base_client import dispatch_nbs_client_methods


DEFAULT_RETRY_TIMEOUT = 300             # 300 sec = 5 min
DEFAULT_RETRY_TIMEOUT_INCREMENT = 0.5   # 0.5 sec = 500 msec


@dispatch_nbs_client_methods
class DurableClient(object):

    def __init__(self, grpc_client, timeout=None, timeout_increment=None, log=None):
        self.__grpc_client = grpc_client

        self.__timeout = DEFAULT_RETRY_TIMEOUT
        if timeout is not None:
            self.__timeout = timeout

        self.__timeout_increment = DEFAULT_RETRY_TIMEOUT_INCREMENT
        if timeout_increment is not None:
            self.__timeout_increment = timeout_increment

        if log is not None:
            self.log = log
        else:
            self.log = logging.getLogger("durable_client")

    def close(self):
        self.__grpc_client.close()

    @property
    def timeout(self):
        return self.__timeout

    @property
    def timeout_increment(self):
        return self.__timeout_increment

    @_handle_errors
    def _handle_request_async(
            self,
            retry_state,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        future = futures.Future()
        try:
            future = call(
                retry_state.request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)
        except ClientError as e:
            retry_state.set_exception(e)
            return

        def process_response(f):
            try:
                self._process_response(
                    f,
                    retry_state,
                    call,
                    idempotence_id,
                    timestamp,
                    trace_id,
                    request_timeout)
            except ClientError as e:
                retry_state.set_exception(e)

        future.add_done_callback(process_response)

    @_handle_errors
    def _execute_request_async_impl(
            self,
            request,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        retry_state = _RetryState(
            request,
            self.__timeout,
            self.__timeout_increment)

        self._handle_request_async(
            retry_state,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return retry_state.future

    def _process_response(
            self,
            future,
            retry_state,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        error = ClientError()
        exception = future.exception()
        response = None
        if exception:
            try:
                raise exception
            except ClientError as e:
                error = e
            except Exception as e:
                retry_state.set_exception(e)
                return
        else:
            response = future.result()
            error = client_error_from_response(response)

        duration = datetime.datetime.now() - retry_state.started

        if error.succeeded:
            if retry_state.retries > 1:
                self.log.info(
                    "%s%s completed (retries: %d, duration: %s)",
                    request_name(retry_state.request),
                    request_details(retry_state.request),
                    retry_state.retries,
                    duration)
            retry_state.set_result(response)
            return

        if not error.is_retriable:
            retry_state.set_exception(error)
            return

        if duration.total_seconds() > retry_state.timeout:
            retry_state.set_exception(error)
            return

        self.log.warning(
            "%s%s retry request (retries: %d, timeout: %s, error: %s)",
            request_name(retry_state.request),
            request_details(retry_state.request),
            retry_state.retries,
            retry_state.timeout,
            error)

        retry_state.increment_retries()

        def retry():
            try:
                self._handle_request_async(
                    retry_state,
                    call,
                    idempotence_id,
                    timestamp,
                    trace_id,
                    request_timeout)
            except ClientError as e:
                retry_state.set_exception(e)

        t = Timer(retry_state.delay, retry)
        t.start()

        retry_state.increment_delay(self.__timeout_increment)

    @_handle_errors
    def _execute_request_async(
            self,
            method_name,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        call = getattr(self.__grpc_client, method_name + '_async')
        return self._execute_request_async_impl(
            request,
            call,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
