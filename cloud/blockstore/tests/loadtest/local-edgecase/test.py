import pytest

import yatest.common as common

from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import CT_LOAD
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test


def default_storage_config_patch(tablet_version=1):
    storage = storage_config_with_default_limits()

    if tablet_version == 2:
        storage.BlockDigestsEnabled = True
        storage.UseTestBlockDigestGenerator = True
        storage.DigestedBlocksPercentage = 100
        storage.DumpBlockCommitIdsIntoProfileLog = True
        storage.DumpBlobUpdatesIntoProfileLog = True

    return storage


def storage_config_with_batching(tablet_version=1):
    storage = storage_config_with_default_limits()
    storage.WriteRequestBatchingEnabled = True
    storage.ThrottlingEnabled = False

    if tablet_version == 2:
        storage.BlockDigestsEnabled = True
        storage.DigestedBlocksPercentage = 100
        storage.DumpBlockCommitIdsIntoProfileLog = True

    return storage


def storage_config_with_new_compaction():
    storage = storage_config_with_default_limits()
    storage.SSDCompactionType = CT_LOAD
    storage.HDDCompactionType = CT_LOAD
    storage.V1GarbageCompactionEnabled = True

    return storage


class TestCase(object):

    def __init__(
            self,
            name,
            config_path,
            storage_config_patch,
    ):
        self.name = name
        self.config_path = config_path
        self.storage_config_patch = storage_config_patch


TESTS = [
    TestCase(
        "version1-large-requests",
        "cloud/blockstore/tests/loadtest/local-edgecase/local-tablet-version-1-large-requests.txt",
        default_storage_config_patch(),
    ),
    TestCase(
        "version1-large-disk",
        "cloud/blockstore/tests/loadtest/local-edgecase/local-tablet-version-1-large-disk.txt",
        default_storage_config_patch(),
    ),
    TestCase(
        "version2-large-disk",
        "cloud/blockstore/tests/loadtest/local-edgecase/local-tablet-version-2-large-disk.txt",
        default_storage_config_patch(tablet_version=2),
    ),
    TestCase(
        "version1-new-compaction",
        "cloud/blockstore/tests/loadtest/local-edgecase/local-tablet-version-1-new-compaction.txt",
        storage_config_with_new_compaction(),
    ),
    TestCase(
        "version1-bs8k",
        "cloud/blockstore/tests/loadtest/local-edgecase/local-tablet-version-1-bs8k.txt",
        default_storage_config_patch(),
    ),
    TestCase(
        "version1-batching",
        "cloud/blockstore/tests/loadtest/local-edgecase/local-tablet-version-1-batching.txt",
        storage_config_with_batching(),
    ),
    TestCase(
        "version2-batching",
        "cloud/blockstore/tests/loadtest/local-edgecase/local-tablet-version-2-batching.txt",
        storage_config_with_batching(tablet_version=2),
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
        storage_config_patches=[test_case.storage_config_patch],
        use_in_memory_pdisks=True,
    )

    try:
        ret = run_test(
            test_case.name,
            test_case.config_path,
            env.nbs_port,
            env.mon_port,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    config = common.get_param("config")
    if config is None:
        test_case.config_path = common.source_path(test_case.config_path)
        return __run_test(test_case)
