from contrib.ydb.tests.library.harness.daemon import Daemon
import contrib.ydb.tests.library.common.yatest_common as yatest_common


class EndpointProxy(Daemon):

    def __init__(self, working_dir, unix_socket_path, with_netlink):
        command = [yatest_common.binary_path(
            "cloud/blockstore/apps/endpoint_proxy/blockstore-endpoint-proxy")]
        command += [
            "--unix-socket-path", unix_socket_path
        ]

        if with_netlink:
            command += "--netlink"

        super(EndpointProxy, self).__init__(
            command=command,
            cwd=working_dir,
            timeout=180)

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, tb):
        self.stop()

    @property
    def pid(self):
        return super(EndpointProxy, self).daemon.process.pid
