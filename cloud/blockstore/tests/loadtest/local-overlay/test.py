import pytest

import yatest.common as common

from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test

import google.protobuf.json_format as protojson


def parse_storage_config(param):
    if param is None:
        return None

    return protojson.Parse(param, TStorageServiceConfig())


def default_storage_config_patch(tablet_version):
    bw = 1 << 7     # 128 MB/s
    iops = 1 << 16

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

    storage.ZoneBlockCount = 2048
    storage.HotZoneRequestCountFactor = 2
    storage.ColdZoneRequestCountFactor = 1

    storage.BlockDigestsEnabled = True

    if tablet_version == 2:
        storage.UseTestBlockDigestGenerator = True
        storage.DigestedBlocksPercentage = 100
        storage.DumpBlockCommitIdsIntoProfileLog = True
        storage.DumpBlobUpdatesIntoProfileLog = True

    return storage


class TestCase(object):

    def __init__(self, name, config_path, tablet_version, storage_config_patch=None):
        self.name = name
        self.config_path = config_path
        self.tablet_version = tablet_version
        self.storage_config_patch = storage_config_patch


TESTS = [
    TestCase(
        "version1-overlay-disk",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-1-overlay-disk.txt",
        1,
    ),
    TestCase(
        "version1-overlay-disk-fresh",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-1-overlay-disk-fresh.txt",
        1,
    ),
    TestCase(
        "version1-overlay-disk-checkpoint",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-1-overlay-disk-checkpoint.txt",
        1,
    ),
    TestCase(
        "version1-overlay-disk-complex",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-1-overlay-disk-complex.txt",
        1,
    ),
    TestCase(
        "version1-logical-used-blocks",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-1-logical-used-blocks.txt",
        1,
    ),
    TestCase(
        "version2-overlay-disk",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-2-overlay-disk.txt",
        2,
    ),
    TestCase(
        "version2-overlay-disk-checkpoint",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-2-overlay-disk-checkpoint.txt",
        2,
    ),
    TestCase(
        "version2-overlay-disk-fresh",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-2-overlay-disk-fresh.txt",
        2,
    ),
    TestCase(
        "version2-overlay-disk-complex",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-version-2-overlay-disk-complex.txt",
        2,
    ),
    TestCase(
        "base-version2-overlay-version1-disk",
        "cloud/blockstore/tests/loadtest/local-overlay/local-tablet-base-version-2-overlay-version-1-disk.txt",
        2,
    ),
]


def __run_test(test_case):
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    storage_config_patch = test_case.storage_config_patch
    if storage_config_patch is None:
        storage_config_patch = default_storage_config_patch(test_case.tablet_version)

    env = LocalLoadTest(
        "",
        server_app_config=server,
        storage_config_patches=[storage_config_patch],
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
    config = common.get_param("config")
    if config is None:
        test_case.config_path = common.source_path(test_case.config_path)
        return __run_test(test_case)
