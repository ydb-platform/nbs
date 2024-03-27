import pytest
import uuid

import yatest.common as common

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test, \
    get_restart_interval
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType


class TestCase(object):

    def __init__(
            self,
            name,
            config_path,
            restart_interval=get_restart_interval(),
            nbd_structured_reply=False,
    ):
        self.name = name
        self.config_path = config_path
        self.restart_interval = restart_interval
        self.nbd_structured_reply = nbd_structured_reply


TESTS = [
    TestCase(
        "version1",
        "cloud/blockstore/tests/loadtest/local-nemesis/local-tablet-version-1.txt",
    ),
    TestCase(
        "version2",
        "cloud/blockstore/tests/loadtest/local-nemesis/local-tablet-version-2.txt",
    ),
    TestCase(
        "version1-grpc-endpoint",
        "cloud/blockstore/tests/loadtest/local-nemesis/local-tablet-version-1-grpc-endpoint.txt",
    ),
    TestCase(
        "version1-nbd-endpoint-simple",
        "cloud/blockstore/tests/loadtest/local-nemesis/local-tablet-version-1-nbd-endpoint.txt",
        nbd_structured_reply=False,
    ),
    TestCase(
        "version1-nbd-endpoint-structured",
        "cloud/blockstore/tests/loadtest/local-nemesis/local-tablet-version-1-nbd-endpoint.txt",
        nbd_structured_reply=True,
    ),
]


def __run_test(test_case):
    endpoint_storage_dir = common.output_path() + '/endpoints-' + str(uuid.uuid4())
    nbd_socket_suffix = "_nbd"

    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    # server.ServerConfig.StrictContractValidation = True
    server.ServerConfig.NbdEnabled = True
    server.ServerConfig.NbdSocketSuffix = nbd_socket_suffix
    server.ServerConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server.ServerConfig.EndpointStorageDir = endpoint_storage_dir
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        storage_config_patches=[storage_config_with_default_limits()],
        use_in_memory_pdisks=True,
        restart_interval=test_case.restart_interval,
    )

    client = TClientConfig()
    client.NbdSocketSuffix = nbd_socket_suffix
    client.NbdStructuredReply = test_case.nbd_structured_reply

    try:
        ret = run_test(
            test_case.name,
            test_case.config_path,
            env.nbs_port,
            env.mon_port,
            nbs_log_path=env.nbs_log_path,
            client_config=client,
            endpoint_storage_dir=endpoint_storage_dir,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    test_case.config_path = common.source_path(test_case.config_path)
    return __run_test(test_case)
