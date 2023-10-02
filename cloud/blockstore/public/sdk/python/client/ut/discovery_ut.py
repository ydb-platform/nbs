import heapq
import itertools
import logging

from concurrent import futures

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client.discovery import _DiscoveryClient, find_closest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")

# In python3 xrange is subsumed by range.
try:
    xrange
except NameError:
    xrange = range


class _TestTimer(object):
    def __init__(self, deadline, cb):
        self.deadline = deadline
        self.cb = cb

    def start(self):
        pass

    def cancel(self):
        self.cb = None


class _TestClock(object):
    def __init__(self):
        self.__timers = []
        self.__current_time = 0
        self.__counter = itertools.count()

    def create_timer(self, timeout, cb):
        t = _TestTimer(self.__current_time + timeout, cb)

        count = next(self.__counter)
        heapq.heappush(self.__timers, (t.deadline, count, t))

        return t

    def invoke_timers(self):
        while len(self.__timers) != 0:
            deadline, _, t = heapq.heappop(self.__timers)
            self.__current_time = deadline
            if t.cb is None:
                continue
            t.cb()


class _GoodClient(object):
    def __init__(self, ctx, delay=None):
        self.__ctx = ctx
        self.__delay = delay
        self.__ctx.client_count += 1

    def close(self):
        self.__ctx.client_count -= 1

    def _execute(self, result):
        self.__ctx.request_count += 1

        future = futures.Future()

        def cb():
            if not future.cancelled():
                future.set_result(result)

        if self.__delay is None:
            cb()
        else:
            t = self.__ctx.clock.create_timer(self.__delay, cb)
            t.start()

        return future

    def describe_volume_async(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        return self._execute(protos.TDescribeVolumeResponse())

    def ping_async(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        return self._execute(protos.TPingResponse())


class _BadClient(object):
    def __init__(self, ctx, error=ClientError(EResult.E_REJECTED.value, ""), delay=None):
        self.__ctx = ctx
        self.__error = error
        self.__delay = delay
        self.__ctx.client_count += 1

    def close(self):
        self.__ctx.client_count -= 1

    def _execute(self):
        self.__ctx.request_count += 1

        future = futures.Future()

        def cb():
            if not future.cancelled():
                future.set_exception(self.__error)

        if self.__delay is None:
            cb()
        else:
            t = self.__ctx.clock.create_timer(self.__delay, cb)
            t.start()

        return future

    def describe_volume_async(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        return self._execute()

    def ping_async(
            self,
            request,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        return self._execute()


def _create_balancer(hosts):

    class TestClient(object):
        def __init__(self, hosts):
            self.__hosts = hosts if hosts and isinstance(hosts[0], list) else [hosts]
            self.__idx = 0

        def discover_instances_async(
                self,
                request,
                idempotence_id=None,
                timestamp=None,
                trace_id=None,
                request_timeout=None):

            future = futures.Future()

            response = protos.TDiscoverInstancesResponse()

            instances = []

            if self.__idx < len(self.__hosts):
                instances = self.__hosts[self.__idx]
                self.__idx += 1

            if request.Limit < len(instances):
                instances = instances[:request.Limit]

            for host in instances:
                i = response.Instances.add()
                i.Host = host

            future.set_result(response)

            return future

    future = futures.Future()
    future.set_result(TestClient(hosts))

    return future


def _describe_volume(client):
    request = protos.TDescribeVolumeRequest()

    r = client.describe_volume_async(request)
    return r.exception()


def _describe_volume_async(client, clock):
    r = client.describe_volume_async(protos.TDescribeVolumeRequest())

    clock.invoke_timers()

    return r.exception()


def _test_limit(fail_expected):
    limit = 2 if fail_expected else 3

    balancer = _create_balancer(["bad-1", "bad-2", "good"])

    class ctx:
        request_count = 0
        client_count = 0

    def factory(host, port):
        if host.startswith("good"):
            return _GoodClient(ctx)

        return _BadClient(ctx)

    discovery = _DiscoveryClient(
        balancer,
        factory,
        discovery_limit=limit,
        log=logger)

    error = _describe_volume(discovery)

    assert ctx.request_count == limit

    if fail_expected:
        assert error.message == "can't create client"

    if not fail_expected:
        assert error is None


def _test_soft_timeout(
        instances,
        expected_requests_count,
        hard_timeout,
        soft_timeout,
        fail_expected):

    balancer = _create_balancer(instances)

    class ctx:
        request_count = 0
        clock = _TestClock()
        client_count = 0

    def factory(host, port):
        if host == "broken":
            raise Exception("broken client")
        if host == "good":
            return _GoodClient(ctx)
        if host.startswith("bad"):
            return _BadClient(ctx)
        return _BadClient(ctx, delay=0.8)

    discovery = _DiscoveryClient(
        balancer,
        factory,
        hard_timeout=hard_timeout,
        soft_timeout=soft_timeout,
        discovery_limit=len(instances),
        log=logger)

    discovery.set_timer_factory(ctx.clock.create_timer)

    error = _describe_volume_async(discovery, ctx.clock)

    if fail_expected:
        assert error is not None

    if not fail_expected:
        assert not error

    assert ctx.request_count == expected_requests_count


def _test_discovery(fail_expected):
    balancer = _create_balancer(["bad-1", "bad-2", "good"])

    class ctx:
        request_count = 0
        client_count = 0

    def factory(host, port):
        if host.startswith("bad"):
            return _BadClient(ctx)
        return _GoodClient(ctx)

    discovery = _DiscoveryClient(
        balancer,
        factory,
        hard_timeout=1.5,
        soft_timeout=0.1,
        discovery_limit=3,
        log=logger)

    r = discovery.discover_instance_async()

    client = r.result()

    assert ctx.request_count == 3

    error = _describe_volume(client)
    assert ctx.request_count == 4
    assert error is None


def test_limit_success():
    _test_limit(fail_expected=False)


def test_limit_failure():
    _test_limit(fail_expected=True)


def test_no_retriable_error():
    balancer = _create_balancer(["bad", "good"])

    class ctx:
        request_count = 0
        client_count = 0

    def factory(host, port):
        if host == "good":
            return _GoodClient(ctx)
        return _BadClient(ctx, ClientError(EResult.E_INVALID_STATE.value, ""))

    discovery = _DiscoveryClient(
        balancer,
        factory,
        log=logger)

    error = _describe_volume(discovery)

    assert ctx.request_count == 1
    assert error.code == EResult.E_INVALID_STATE.value


def test_volatile_network():
    balancer = _create_balancer([
        ["host-1", "host-2", "host-3"],
        ["host-1", "host-3", "host-4"]])

    class ctx:
        route = []
        alive = set({'host-3', 'host-4'})
        client_count = 0

    class TestClient(object):
        def __init__(self, ctx, host):
            self.__ctx = ctx
            self.__host = host

        def describe_volume_async(
                self,
                request,
                idempotence_id=None,
                timestamp=None,
                trace_id=None,
                request_timeout=None):

            self.__ctx.route.append(self.__host)

            future = futures.Future()

            if self.__host in self.__ctx.alive:
                future.set_result(protos.TDescribeVolumeResponse())
            else:
                future.set_exception(ClientError(EResult.E_REJECTED.value, ""))

            return future

    def factory(host, port):
        return TestClient(ctx, host)

    discovery = _DiscoveryClient(
        balancer,
        factory,
        log=logger)

    assert _describe_volume(discovery) is None

    ctx.alive.remove("host-3")

    assert _describe_volume(discovery) is None

    expected_route = ["host-1", "host-2", "host-3", "host-3", "host-1", "host-4"]

    assert ctx.route == expected_route


def test_empty_instances():
    balancer = _create_balancer([])

    def factory(host, port):
        return None

    discovery = _DiscoveryClient(
        balancer,
        factory,
        log=logger)

    error = _describe_volume(discovery)

    assert error.message == "can't create client"


def test_good_one():
    balancer = _create_balancer(["good"])

    class ctx:
        request_count = 0
        client_count = 0

    def factory(host, port):
        return _GoodClient(ctx)

    discovery = _DiscoveryClient(
        balancer,
        factory,
        log=logger)

    n = 10

    for _ in xrange(n):
        assert _describe_volume(discovery) is None

    assert ctx.request_count == n


def test_hard_timeout():
    balancer = _create_balancer(["slow-1", "slow-2", "good"])

    class ctx:
        request_count = 0
        clock = _TestClock()
        client_count = 0

    def factory(host, port):
        if host.startswith("slow"):
            return _BadClient(ctx, delay=0.9)
        return _GoodClient(ctx)

    discovery = _DiscoveryClient(
        balancer,
        factory,
        hard_timeout=1.5,
        log=logger)

    discovery.set_timer_factory(ctx.clock.create_timer)

    assert _describe_volume_async(discovery, ctx.clock).code == EResult.E_TIMEOUT.value
    assert ctx.request_count == 2


def test_soft_timeout1():
    _test_soft_timeout(["ugly", "good"], 2, 0.5, 0.01, False)


def test_soft_timeout2():
    _test_soft_timeout(["bad", "ugly", "good"], 3, 0.5, 0.01, False)


def test_soft_timeout3():
    _test_soft_timeout(["ugly-1", "ugly-2", "good"], 3, 1.5, 0.01, False)


def test_soft_timeout4():
    _test_soft_timeout(["ugly", "bad", "good"], 2, 0.5, 0.01, True)


def test_soft_timeout5():
    _test_soft_timeout(["ugly", "broken", "good"], 2, 0.8, 0.01, False)


def test_discovery_success():
    _test_discovery(False)


def test_discovery_failure():
    _test_discovery(True)


def test_ping_contest():
    class ctx:
        request_count = 0
        clock = _TestClock()
        client_count = 0

    def run(clients):
        f = find_closest(clients)
        ctx.clock.invoke_timers()
        return f

    bad_fast = _BadClient(ctx, delay=1)
    bad_slow = _BadClient(ctx, delay=2)
    good_fast = _GoodClient(ctx, delay=1)
    good_slow = _GoodClient(ctx, delay=2)
    good_instant = _GoodClient(ctx)

    assert run([bad_slow, bad_fast]).exception()
    assert run([good_slow, bad_fast]).result() == good_slow
    assert run([good_slow, good_fast]).result() == good_fast
    assert run([good_instant, good_fast]).result() == good_instant


def test_ping_close():
    class ctx:
        request_count = 0
        clock = _TestClock()
        client_count = 0

    def run(clients):
        f = find_closest(clients)
        ctx.clock.invoke_timers()
        return f

    bad_fast = _BadClient(ctx, delay=1)
    bad_slow = _BadClient(ctx, delay=2)
    good_fast = _GoodClient(ctx, delay=1)
    good_slow = _GoodClient(ctx, delay=2)

    c = run([bad_slow, bad_fast, good_slow, good_fast]).result()

    assert c == good_fast
    c.close()

    assert ctx.client_count == 0
