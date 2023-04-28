import datetime
import random
import sys

from concurrent import futures

import cloud.blockstore.public.sdk.python.protos as protos


def _next_request_id():
    return random.randint(1, sys.maxsize)


def request_name(request):
    name = type(request).__name__
    if name.startswith("T") and name.endswith("Request"):
        # strip "T" prefix and "Request" suffix from type name
        return name[1:len(name) - 7]

    return name


def request_size(request):
    if isinstance(request, protos.TReadBlocksRequest):
        return request.BlocksCount

    if isinstance(request, protos.TWriteBlocksRequest):
        return len(request.Blocks.Buffers)

    if isinstance(request, protos.TZeroBlocksRequest):
        return request.BlocksCount

    return 0


def request_details(request):
    if isinstance(request, protos.TReadBlocksRequest):
        return " (offset: {}, count: {})".format(
            request.StartIndex,
            request.BlocksCount)

    if isinstance(request, protos.TWriteBlocksRequest):
        return " (offset: {}, count: {})".format(
            request.StartIndex,
            len(request.Blocks.Buffers))

    if isinstance(request, protos.TZeroBlocksRequest):
        return " (offset: {}, count: {})".format(
            request.StartIndex,
            request.BlocksCount)

    return ""


class _AsyncResponse(object):

    def __init__(self, future, request, request_id, started):
        self.__future = future
        self.__request = request
        self.__request_id = request_id
        self.__started = started

    @property
    def future(self):
        return self.__future

    @property
    def request(self):
        return self.__request

    @property
    def request_id(self):
        return self.__request_id

    @property
    def started(self):
        return self.__started


class _RetryState(object):

    def __init__(self, request, timeout, delay):
        self.__started = datetime.datetime.now()
        self.__request = request
        self.__timeout = timeout

        self.__delay = delay
        self.__retries = 0

        self.__response = futures.Future()

    @property
    def started(self):
        return self.__started

    @property
    def request(self):
        return self.__request

    @property
    def timeout(self):
        return self.__timeout

    @property
    def delay(self):
        return self.__delay

    @property
    def retries(self):
        return self.__retries

    @property
    def future(self):
        return self.__response

    def increment_delay(self, increment):
        self.__delay += increment

    def increment_retries(self):
        self.__retries += 1

    def set_result(self, result):
        self.__response.set_result(result)

    def set_exception(self, exception):
        self.__response.set_exception(exception)
