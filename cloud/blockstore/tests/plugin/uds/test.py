import os
import pytest

from cloud.blockstore.tests.python.lib.test_with_plugin import run_plugin_test


class TestCase(object):

    def __init__(
            self,
            name,
            disk_id,
            test_config,
            endpoint,
            client_ipc_type,
            nbd_socket_suffix="",
            restart_interval=None,
            run_count=1):

        self.name = name
        self.disk_id = disk_id
        self.test_config = test_config
        self.endpoint = endpoint
        self.client_ipc_type = client_ipc_type
        self.nbd_socket_suffix = nbd_socket_suffix
        self.restart_interval = restart_interval
        self.run_count = run_count


TESTS = [
    TestCase(
        "endpoint",
        "vol0",
        "cloud/blockstore/tests/plugin/uds/endpoint.txt",
        "vol0.socket",
        "grpc",
    ),
    TestCase(
        "endpoint2",
        "vol0",
        "cloud/blockstore/tests/plugin/uds/endpoint2.txt",
        "vol0.socket",
        "grpc",
    ),
    TestCase(
        "endpoint_nbd",
        "vol0",
        "cloud/blockstore/tests/plugin/uds/endpoint.txt",
        "vol0.socket",
        "nbd",
        "_nbd_suffix",
    ),
    TestCase(
        "restarts",
        "vol0",
        "cloud/blockstore/tests/plugin/uds/endpoint.txt",
        "vol0.socket",
        "grpc",
        restart_interval=15,
        run_count=200,
    ),
]


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
@pytest.mark.parametrize("plugin_version", ["trunk", "stable"])
def test_load(test_case, plugin_version):
    if plugin_version == "stable" and os.environ.get("SANITIZER_TYPE") == "thread":
        pytest.skip("skip stable plugin test with tsan")

    return run_plugin_test(
        test_case.name,
        test_case.disk_id,
        test_case.test_config,
        endpoint=test_case.endpoint,
        client_ipc_type=test_case.client_ipc_type,
        nbd_socket_suffix=test_case.nbd_socket_suffix,
        plugin_version=plugin_version,
        restart_interval=test_case.restart_interval,
        run_count=test_case.run_count)
