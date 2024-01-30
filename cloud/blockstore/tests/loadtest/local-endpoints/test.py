import pytest

import yatest.common as common

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.server_pb2 import \
    TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test


CONFIG_DIR = "cloud/blockstore/tests/loadtest/local-endpoints/"


class TestCase(object):

    def __init__(
        self,
        name,
        config_path,
        nbd_enabled=False,
        nbd_structured_reply=False,
    ):
        self.name = name
        self.config_path = config_path
        self.nbd_enabled = nbd_enabled
        self.nbd_structured_reply = nbd_structured_reply


def default_storage_config():
    bw = 1 << 5     # 32 MB/s
    iops = 1 << 12  # 4096 iops

    storage = TStorageServiceConfig()
    storage.ThrottlingEnabled = True

    storage.SSDUnitReadBandwidth = bw
    storage.SSDUnitWriteBandwidth = bw
    storage.SSDMaxReadBandwidth = bw
    storage.SSDMaxWriteBandwidth = bw
    storage.SSDUnitReadIops = iops
    storage.SSDUnitWriteIops = iops
    storage.SSDMaxReadIops = iops
    storage.SSDMaxWriteIops = iops

    storage.HDDUnitReadBandwidth = bw
    storage.HDDUnitWriteBandwidth = bw
    storage.HDDMaxReadBandwidth = bw
    storage.HDDMaxWriteBandwidth = bw
    storage.HDDUnitReadIops = iops
    storage.HDDUnitWriteIops = iops
    storage.HDDMaxReadIops = iops
    storage.HDDMaxWriteIops = iops

    storage.InactiveClientsTimeout = 10000
    return storage


TESTS = [
    TestCase(
        "version1-grpc-endpoint",
        "local-tablet-version-1-grpc-endpoint.txt",
        nbd_enabled=True,   # TODO: remove this
    ),
    TestCase(
        "version1-grpc-endpoint-bs8k",
        "local-tablet-version-1-grpc-endpoint-bs8k.txt",
        nbd_enabled=True,   # TODO: remove this
    ),
    TestCase(
        "version1-nbd-endpoint-simple",
        "local-tablet-version-1-nbd-endpoint.txt",
        nbd_enabled=True,
    ),
    TestCase(
        "version1-nbd-endpoint-structured",
        "local-tablet-version-1-nbd-endpoint.txt",
        nbd_enabled=True,
        nbd_structured_reply=True,
    ),
    TestCase(
        "version1-nbd-endpoint-bs8k-simple",
        "local-tablet-version-1-nbd-endpoint-bs8k.txt",
        nbd_enabled=True,
    ),
    TestCase(
        "version1-nbd-endpoint-bs8k-structured",
        "local-tablet-version-1-nbd-endpoint-bs8k.txt",
        nbd_enabled=True,
        nbd_structured_reply=True,
    ),
]


def __run_test(test_case):
    nbd_socket_suffix = "_nbd"

    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.ServerConfig.NbdEnabled = test_case.nbd_enabled
    server.ServerConfig.NbdSocketSuffix = nbd_socket_suffix
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        use_in_memory_pdisks=True,
        storage_config_patches=[
            default_storage_config()
        ],
    )

    client_config = TClientConfig()
    client_config.NbdSocketSuffix = nbd_socket_suffix
    client_config.NbdStructuredReply = test_case.nbd_structured_reply

    try:
        ret = run_test(
            test_case.name,
            test_case.config_path,
            env.nbs_port,
            env.mon_port,
            nbs_log_path=env.nbs_log_path,
            client_config=client_config,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    config = common.get_param("config")
    if config is None:
        test_case.config_path = common.source_path(
            CONFIG_DIR + test_case.config_path)
        return __run_test(test_case)
