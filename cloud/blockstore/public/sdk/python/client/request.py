import datetime
import random
import sys

from concurrent import futures
from google.protobuf.message import Message

import cloud.blockstore.public.sdk.python.protos as protos


def _next_request_id() -> int:
    return random.randint(1, sys.maxsize)


def request_name(request: type[Message]) -> str:
    name = type(request).__name__
    if name.startswith("T") and name.endswith("Request"):
        # strip "T" prefix and "Request" suffix from type name
        return name[1:len(name) - 7]

    return name


def request_size(request: type[Message]) -> int:
    if isinstance(request, protos.TReadBlocksRequest):
        return request.BlocksCount

    if isinstance(request, protos.TWriteBlocksRequest):
        return len(request.Blocks.Buffers)

    if isinstance(request, protos.TZeroBlocksRequest):
        return request.BlocksCount

    return 0


def request_details(request: type[Message]) -> str:
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

    def __init__(
            self,
            future: futures.Future,
            request: type[Message],
            request_id: int,
            started: datetime.datetime):
        self.__future = future
        self.__request = request
        self.__request_id = request_id
        self.__started = started

    @property
    def future(self) -> futures.Future:
        return self.__future

    @property
    def request(self) -> type[Message]:
        return self.__request

    @property
    def request_id(self) -> int:
        return self.__request_id

    @property
    def started(self) -> datetime.datetime:
        return self.__started


class _RetryState(object):

    def __init__(
            self,
            request: type[Message],
            timeout: int,
            delay: float):
        self.__started = datetime.datetime.now()
        self.__request = request
        self.__timeout = timeout

        self.__delay = delay
        self.__retries = 0

        self.__response = futures.Future()

    @property
    def started(self) -> datetime.datetime:
        return self.__started

    @property
    def request(self) -> type[Message]:
        return self.__request

    @property
    def timeout(self) -> int:
        return self.__timeout

    @property
    def delay(self) -> float:
        return self.__delay

    @property
    def retries(self) -> int:
        return self.__retries

    @property
    def future(self) -> futures.Future:
        return self.__response

    def increment_delay(self, increment: float):
        self.__delay += increment

    def increment_retries(self):
        self.__retries += 1

    def set_result(self, result: type[Message]):
        self.__response.set_result(result)

    def set_exception(self, exception: type[BaseException]):
        self.__response.set_exception(exception)
