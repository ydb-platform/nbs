import pytest

import yatest.common as common

from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test


def default_storage_config_patch():
    storage = storage_config_with_default_limits()

    storage.InactiveClientsTimeout = 10000

    storage.ZoneBlockCount = 2048
    storage.HotZoneRequestCountFactor = 2
    storage.ColdZoneRequestCountFactor = 1
    storage.DontEnqueueCollectGarbageUponPartitionStartup = True
    storage.OptimizeForShortRanges = True

    storage.BlockDigestsEnabled = True

    return storage


class TestCase(object):

    def __init__(self, name, config_path):
        self.name = name
        self.config_path = config_path


TESTS = [
    TestCase(
        "partition-reboots",
        "cloud/blockstore/tests/loadtest/local-v2/partition-reboots.txt",
    ),
]


def __run_test(test_case):
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        storage_config_patches=[default_storage_config_patch()],
        use_in_memory_pdisks=True,
    )

    try:
        ret = run_test(
            test_case.name,
            test_case.config_path,
            env.nbs_port,
            env.mon_port,
            nbs_log_path=env.nbs_log_path,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    test_case.config_path = common.source_path(test_case.config_path)
    return __run_test(test_case)
