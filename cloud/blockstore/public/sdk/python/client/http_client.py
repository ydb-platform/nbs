import datetime
import logging
import requests
import threading

from concurrent import futures

from .base_client import _BaseClient
from .request import _next_request_id, request_name

from google.protobuf.json_format import MessageToJson, Parse

from .error import ClientError, _handle_errors
from .error_codes import EResult

import cloud.blockstore.public.sdk.python.protos as protos


TRANSPORT_TIMEOUT = 1
CONNECT_TIMEOUT = 5
MAX_MESSAGE_SIZE = 8 * 1024 * 1024


def _response_name(request):
    name = type(request).__name__
    return name[:len(name) - 7] + 'Response'


class HttpClient(_BaseClient):

    def __init__(
            self,
            endpoint,
            credentials=None,
            timeout=None,
            log=None,
            executor=None,
            connect_timeout=CONNECT_TIMEOUT):
        try:
            super(HttpClient, self).__init__(endpoint, timeout)

            if executor is None:
                self.__executor = futures.ThreadPoolExecutor(max_workers=1)
            else:
                self.__executor = executor

            self.__context = threading.local()
            self.__credentials = credentials

            if log is not None:
                self.log = log
            else:
                self.log = logging.getLogger("http_client")

            self.__connect_timeout = connect_timeout

        except Exception as e:
            if log is not None:
                log.exception("Can't create client")
            raise ClientError(EResult.E_FAIL.value, str(e))

    def _session(self):
        session = getattr(self.__context, 'session', None)
        if session is None:
            thread = threading.current_thread()
            self.log.debug('creating session for thread %s' % thread.ident)
            session = requests.Session()
            session.verify = False

            if self.__credentials is not None:
                session.cert = (
                    self.__credentials.cert_file,
                    self.__credentials.cert_private_key_file)

            self.__context.session = session

        return session

    def close(self):
        pass

    @_handle_errors
    def _post(
            self,
            path,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        self._setup_headers(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        json = MessageToJson(request)

        if len(json) > MAX_MESSAGE_SIZE:
            raise ValueError("request too large")

        wait_timeout = request_timeout
        if wait_timeout is None:
            wait_timeout = self.timeout
        wait_timeout += TRANSPORT_TIMEOUT

        try:
            r = self._session().post(
                self.endpoint + '/' + path,
                data=json,
                stream=True,
                timeout=(self.__connect_timeout, wait_timeout)
            )
            r.raise_for_status()
        except (requests.exceptions.ConnectTimeout, requests.exceptions.Timeout) as e:
            raise ClientError(EResult.E_TIMEOUT.value, str(e))

        if int(r.headers.get('Content-Length', 0)) > MAX_MESSAGE_SIZE:
            raise ValueError("response too large")

        response_class = getattr(protos, _response_name(request))
        return Parse(r.content, response_class())

    def _execute_request(
            self,
            path,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        request_id = _next_request_id()

        self.log.debug(
            "%s #%d sending request",
            request_name(request),
            request_id)
        started = datetime.datetime.now()

        r = self._post(
            path,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        return self._process_response(r, request, request_id, started)

    def _execute_request_async(
            self,
            path,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        request_id = _next_request_id()

        self.log.debug(
            "%s #%d sending request",
            request_name(request),
            request_id)
        started = datetime.datetime.now()

        future = futures.Future()

        def set_result(f):
            try:
                response = self._process_response(
                    f.result(),
                    request,
                    request_id,
                    started)
                future.set_result(response)
            except ClientError as e:
                future.set_exception(e)

        f = self.__executor.submit(
            self._post,
            path,
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        f.add_done_callback(set_result)

        return future
