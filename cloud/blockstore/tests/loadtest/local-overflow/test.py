import pytest
import yatest.common as common

from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import run_test


class TestCase(object):

    def __init__(self, name, config_path, stat_filter=None, dynamic_disk_count=1):
        self.name = name
        self.config_path = config_path
        self.stat_filter = stat_filter
        self.dynamic_disk_count = dynamic_disk_count


TESTS = [
    # NOTE: BS group disintegration may happen before
    # E_BS_OUT_OF_SPACE(2147483662), in that case retries are timed out and
    # resulting E_RETRY_TIMEOUT(2147483662) error is considered as a success.
    # At the end of the test we read some data from disk to ensure that tablet
    # is available.
    TestCase(
        "version1",
        "cloud/blockstore/tests/loadtest/local-overflow/local-tablet-version-1.txt",
        "ReassignTablet",
    ),
    TestCase(
        "version1-throttled",
        "cloud/blockstore/tests/loadtest/local-overflow/local-tablet-version-1-throttled.txt",
        ["ThrottlerRejected", "ReassignTablet"],
    ),
    # NOTE: BS group disintegration may happen before
    # E_BS_OUT_OF_SPACE(2147483662), in that case retries are timed out and
    # resulting E_RETRY_TIMEOUT(2147483662) error is considered as a success.
    # At the end of the test we read some data from disk to ensure that tablet
    # is available.
    TestCase(
        "version2",
        "cloud/blockstore/tests/loadtest/local-overflow/local-tablet-version-2.txt",
        "ReassignTablet",
    ),
    #    our test framework does not work with 2+ dynamic disks =(
    #    TestCase(
    #        "version1-reassign",
    #        "cloud/blockstore/tests/loadtest/local-overflow/local-tablet-version-1.txt",
    #        "AppCriticalEvents",
    #        2
    #    ),
    #    TestCase(
    #        "version2-reassign",
    #        "cloud/blockstore/tests/loadtest/local-overflow/local-tablet-version-2.txt",
    #        "AppCriticalEvents",
    #        2
    #    ),
]


def __run_test(test_case):
    storage = TStorageServiceConfig()
    storage.ThrottlingEnabled = True
    storage.HDDSystemChannelPoolKind = "system"
    storage.SSDSystemChannelPoolKind = "system"
    storage.HybridSystemChannelPoolKind = "system"
    storage.HDDLogChannelPoolKind = "system"
    storage.SSDLogChannelPoolKind = "system"
    storage.HybridLogChannelPoolKind = "system"
    storage.HDDIndexChannelPoolKind = "system"
    storage.SSDIndexChannelPoolKind = "system"
    storage.HybridIndexChannelPoolKind = "system"
    # disable backpressure
    storage.MaxWriteCostMultiplier = 1

    env = LocalLoadTest(
        "",
        storage_config_patches=[storage],
        dynamic_pdisks=[dict(user_kind=1, disk_size=10 * 1024 * 1024 * 1024)
                        for x in range(test_case.dynamic_disk_count)],
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1",
                 kind="system", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2",
                 kind="rot", pdisk_user_kind=1)
        ],
    )

    try:
        ret = run_test(
            "overflow-%s" % test_case.name,
            common.source_path(test_case.config_path),
            env.nbs_port,
            env.mon_port,
            stat_filter=test_case.stat_filter,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    return __run_test(test_case)
