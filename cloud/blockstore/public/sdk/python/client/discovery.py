import logging
import typing

from concurrent import futures
from datetime import datetime
from threading import Timer
from functools import partial

from google.protobuf.message import Message

import cloud.blockstore.public.sdk.python.protos as protos

from .credentials import ClientCredentials
from .error_codes import EResult
from .error import ClientError, _handle_errors, client_error_from_response
from .grpc_client import GrpcClient
from .http_client import HttpClient
from .durable import DurableClient
from .base_client import _BaseClient, dispatch_nbs_client_methods
from .safe_client import _SafeClient
from .future import unit, bind


DEFAULT_HARD_TIMEOUT = 8*60  # 8 min
DEFAULT_DISCOVERY_LIMIT = 3


class _Executor(object):
    def __init__(
            self,
            method: str,
            balancer: futures.Future,
            factory: typing.Callable,
            limit: int,
            secure: bool,
            log: type[logging.Logger]):
        self.__response = futures.Future()
        self.__method = method
        self.__balancer = balancer
        self.__factory = factory
        self.__limit = limit
        self.__secure = secure
        self.__visited = None
        self.__log = log
        self.__hedged_timer = None
        self.__pending = {}
        self.__done = False
        self.__instances = None
        self.__instances_future = None
        self.__idx = 0
        self.__main_timer = None
        self.__hedged_timer = None

    def run(
            self,
            impl,
            addr,
            timeout: int,
            soft_timeout: int | None,
            create_timer: typing.Callable) -> futures.Future:
        self.__main_timer = create_timer(timeout, self._on_main_timer)
        self.__main_timer.start()

        if soft_timeout:
            self.__hedged_timer = create_timer(soft_timeout, self._on_hedged_timer)
            self.__hedged_timer.start()

        self.__visited = addr

        if impl is not None:
            self._shoot(impl, addr)
        else:
            self._try_shoot()

        return self.__response

    def _cancel_all(self):
        self.__done = True
        self.__main_timer.cancel()
        if self.__hedged_timer is not None:
            self.__hedged_timer.cancel()
        for f in self.__pending.values():
            f.cancel()

    def _set_result(self, result, impl, addr):
        if self.__done:
            return
        self._cancel_all()
        self.__response.set_result((impl, addr, result))

    def _set_exception(self, e):
        if self.__done:
            return
        self._cancel_all()
        self.__response.set_exception(e)

    def _on_main_timer(self):
        if self.__done:
            return

        self._set_exception(
            ClientError(EResult.E_TIMEOUT.value, "deadline exceeded"))

    def _on_hedged_timer(self):
        if self.__done:
            return

        self._try_shoot()

    def _shoot(self, impl, addr):
        r = self.__method(impl)
        self.__pending[(addr.Host, addr.Port)] = r

        def cb(f):
            self._handle_response(f, impl, addr)

        r.add_done_callback(cb)

    def _on_discover_instances(self, f):
        if self.__done:
            return

        e = f.exception()

        if e is None:
            self.__instances = f.result().Instances
            self.__log.debug("success discovery: {}".format(
                map(lambda x: x.Host + ":" + str(x.Port), self.__instances)))
            self._try_shoot()
            return

        self.__log.error("error on discovery: {}".format(e))

        if len(self.__pending) == 0:
            self.set_exception(e)

    def _try_shoot(self):
        if self.__instances is None:
            if self.__instances_future is None:
                request = protos.TDiscoverInstancesRequest()
                request.Limit = self.__limit
                if self.__secure:
                    request.InstanceFilter = protos.EDiscoveryPortFilter.Value(
                        "DISCOVERY_SECURE_PORT")
                self.__instances_future = self.__balancer.discover_instances_async(request)
            self.__instances_future.add_done_callback(self._on_discover_instances)
            return

        while self.__idx < len(self.__instances):
            inst = self.__instances[self.__idx]
            self.__idx += 1

            if self.__visited and \
                    inst.Host == self.__visited.Host and \
                    inst.Port == self.__visited.Port:
                continue

            try:
                impl = self.__factory(inst.Host, inst.Port)
            except Exception as e:
                self.__log.warning("error on create client: {}".format(e))
                continue

            if impl is None:
                continue

            self._shoot(impl, inst)
            return

        if len(self.__pending) == 0:
            self._set_exception(
                ClientError(EResult.E_FAIL.value, "can't create client"))

    def _handle_response(self, f, impl, addr):
        if f.cancelled():
            return

        self.__log.debug("handle response from {}:{}".format(
            addr.Host,
            addr.Port))

        del self.__pending[(addr.Host, addr.Port)]

        if self.__done:
            return

        is_retriable = False
        error = None

        try:
            response = f.result()
            e = client_error_from_response(response)
            if not e.succeeded:
                raise e
        except ClientError as e:
            error = e
            is_retriable = e.is_retriable
        except Exception as e:
            error = e

        if not error:
            self._set_result(response, impl, addr)
            return

        self.__log.error("{}:{} request error: {}".format(addr.Host, addr.Port, error))

        if not is_retriable:
            self._set_exception(error)
            return

        if len(self.__pending) == 0:
            self._try_shoot()


@dispatch_nbs_client_methods
class _DiscoveryClient(object):

    def __init__(
            self,
            balancer: futures.Future,
            factory: typing.Callable,
            discovery_limit: int | None = None,
            hard_timeout: int | None = None,
            soft_timeout: int | None = None,
            log: type[logging.Logger] | None = None,
            secure: bool = False):

        self.__impl = None
        self.__addr = None
        self.__balancer = balancer
        self.__factory = factory
        self.__secure = secure

        self.__limit = DEFAULT_DISCOVERY_LIMIT
        if discovery_limit is not None:
            self.__limit = discovery_limit

        self.__timeout = DEFAULT_HARD_TIMEOUT
        if hard_timeout is not None:
            self.__timeout = hard_timeout

        self.__soft_timeout = soft_timeout

        if log is not None:
            self.log = log
        else:
            self.log = logging.getLogger("discovery_client")

        self.__create_timer = Timer

    def close(self):
        if self.__impl is not None:
            self.__impl.close()

        if self.__balancer.done() and not self.__balancer.cancelled():
            self.__balancer.result().close()

    def set_timer_factory(self, create_timer: Timer):
        self.__create_timer = create_timer

    @property
    def timeout(self) -> int:
        return self.__timeout

    @property
    def soft_timeout(self) -> int | None:
        return self.__soft_timeout

    @property
    def limit(self) -> int:
        return self.__limit

    @_handle_errors
    def _execute_request_async(
            self,
            method_name: str,
            request: type[Message],
            idempotence_id: str,
            timestamp: datetime,
            trace_id: str,
            request_timeout: int) -> futures.Future:

        def method(impl):
            m = getattr(impl, method_name + '_async')
            return m(
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)

        def run(client):
            e = _Executor(
                method,
                client,
                self.__factory,
                self.__limit,
                self.__secure,
                self.log)

            return e.run(
                self.__impl,
                self.__addr,
                self.__timeout,
                self.__soft_timeout,
                self.__create_timer)

        def update(client):
            self.__impl, self.__addr, r = client
            return unit(r)

        return bind(bind(self.__balancer, run), update)

    def ping_async(
            self,
            request: type[Message],
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        def cb(client):
            return client.ping_async(
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)

        return bind(self.__balancer, cb)

    def ping(
            self,
            request: type[Message],
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        return self.ping_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).result()

    def discover_instances_async(
            self,
            request: type[Message],
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        def cb(client):
            return client.discover_instances_async(
                request,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)

        return bind(self.__balancer, cb)

    def discover_instances(
            self,
            request: type[Message],
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        return self.discover_instances_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).result()

    def discover_instance_async(self) -> futures.Future:
        future = futures.Future()

        def ping_cb(f, impl, instances, i):
            try:
                f.result()
                future.set_result(impl)
            except Exception:
                loop(instances, i)

        def loop(instances, i):
            while i < len(instances):
                inst = instances[i]
                i += 1

                try:
                    impl = self.__factory(inst.Host, inst.Port)
                except Exception as e:
                    self.__log.warning("error on create client: {}".format(e))
                    continue

                if impl is None:
                    continue

                f = impl.ping_async(protos.TPingRequest())

                def cb(f):
                    ping_cb(f, impl, instances, i)

                f.add_done_callback(cb)

                return

            future.set_exception(
                ClientError(EResult.E_FAIL.value, "can't create client"))

        def discover_instances_cb(f):
            try:
                instances = f.result().Instances
                loop(instances, 0)
            except Exception as e:
                future.set_exception(e)

        request = protos.TDiscoverInstancesRequest()
        request.Limit = self.__limit
        if self.__secure:
            request.InstanceFilter = protos.EDiscoveryPortFilter.Value(
                "DISCOVERY_SECURE_PORT")

        f = self.discover_instances_async(request)

        f.add_done_callback(discover_instances_cb)

        return future


class DiscoveryClient(_SafeClient):
    def __init__(self, impl):
        super(DiscoveryClient, self).__init__(impl)

    def discover_instance(self):
        return self.discover_instance_async().result()

    def discover_instance_async(self):
        return self._impl.discover_instance_async()


def find_closest(
        clients: map,
        request_timeout: int | None = None) -> futures.Future:
    result = futures.Future()
    requests = dict()

    def done(c: DurableClient, f: futures.Future):
        if result.done():
            return

        del requests[c]

        if f.exception():
            if not requests:
                result.set_exception(f.exception())
            c.close()
        else:
            result.set_result(c)

            while requests:
                x, f = requests.popitem()
                f.cancel()
                x.close()

    requests = {c: c.ping_async(
        protos.TPingRequest(),
        request_timeout=request_timeout) for c in clients}

    for c, f in requests.copy().items():
        f.add_done_callback(partial(done, c))

    return result


def CreateDiscoveryClient(
        endpoints: list[str] | str,
        credentials: ClientCredentials | None = None,
        request_timeout: int | None = None,
        retry_timeout: int | None = None,
        retry_timeout_increment: int | None = None,
        log: type[logging.Logger] | None = None,
        executor: futures.ThreadPoolExecutor | None = None,
        hard_timeout: int | None = None,
        soft_timeout: int | None = None,
        discovery_limit: int | None = None) -> DiscoveryClient:

    def make_http_backend(endpoint: str) -> HttpClient:
        return HttpClient(
            endpoint,
            credentials,
            request_timeout,
            log,
            executor)

    def make_grpc_backend(endpoint: str) -> GrpcClient:
        return GrpcClient(
            endpoint,
            credentials,
            request_timeout,
            log)

    def make_backend(endpoint: str) -> type[_BaseClient]:
        if endpoint.startswith('https://') or endpoint.startswith('http://'):
            return make_http_backend(endpoint)
        else:
            return make_grpc_backend(endpoint)

    def make_client(endpoint: str) -> DurableClient:
        return DurableClient(
            make_backend(endpoint),
            retry_timeout,
            retry_timeout_increment,
            log)

    def factory(host: str, port: str) -> DurableClient:
        return make_client(host + ':' + str(port))

    if not isinstance(endpoints, list):
        endpoints = [endpoints]

    balancer = find_closest(map(make_client, endpoints))

    discovery_client = _DiscoveryClient(
        balancer,
        factory,
        discovery_limit,
        hard_timeout,
        soft_timeout,
        log,
        credentials is not None)

    return DiscoveryClient(discovery_client)
