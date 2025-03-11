from cloud.storage.core.tools.common.python.daemon import Daemon
import contrib.ydb.tests.library.common.yatest_common as yatest_common


class EndpointProxy(Daemon):

    def __init__(
            self,
            working_dir,
            unix_socket_path,
            with_netlink,
            stored_endpoints_path,
            nbd_request_timeout,
            nbd_reconnect_delay,
            restart_events,
    ):
        command = [yatest_common.binary_path(
            "cloud/blockstore/apps/endpoint_proxy/blockstore-endpoint-proxy")]
        command += [
            "--unix-socket-path", unix_socket_path, "--verbose", "--without-libnl",
        ]
        if stored_endpoints_path:
            command += ["--stored-endpoints-path", stored_endpoints_path]

        if with_netlink:
            command += ["--netlink"]

        if nbd_request_timeout:
            command += ["--nbd-request-timeout", nbd_request_timeout]

        if nbd_reconnect_delay:
            command += ["--nbd-reconnect-delay", nbd_reconnect_delay]

        if restart_events:
            command += ["--restart-events", str(restart_events)]

        super(EndpointProxy, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name="blockstore-endpoint-proxy")

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, tb):
        self.stop()

    @property
    def pid(self):
        return super(EndpointProxy, self).daemon.process.pid
