import pytest

import yatest.common as common

from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists


def default_storage_config(tablet_version, backups_folder):
    storage = storage_config_with_default_limits()

    storage.InactiveClientsTimeout = 10000
    storage.DiskPrefixLengthWithBlockChecksumsInBlobs = 1 << 30  # 1 GiB
    storage.CheckBlockChecksumsInBlobsUponRead = True

    if tablet_version == 2:
        storage.BlockDigestsEnabled = True
        storage.DigestedBlocksPercentage = 100
        storage.DumpBlockCommitIdsIntoProfileLog = True
        storage.DumpBlobUpdatesIntoProfileLog = True

    storage.TabletBootInfoBackupFilePath = \
        backups_folder + "/tablet_boot_info_backup.txt"
    storage.PathDescriptionBackupFilePath = \
        backups_folder + "/path_description_backup.txt"

    return storage


class TestCase(object):
    __test__ = False

    def __init__(
            self,
            name,
            tablet_version,
            config_path,
            stat_filter=None,
            tracking_enabled=False,
            track_filter=None,
    ):
        self.name = name
        self.tablet_version = tablet_version
        self.config_path = config_path
        self.stat_filter = stat_filter
        self.tracking_enabled = tracking_enabled
        self.track_filter = track_filter


CRITICAL_EVENTS = [
    "DiskAllocationFailure",
    "InvalidTabletConfig",
    "InvalidVolumeBillingSettings",
    "ReassignTablet",
    "TabletCommitIdOverflow"
]


TESTS = [
    TestCase(
        "version1",
        1,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-1.txt",
        ["ThrottlerPostponed", CRITICAL_EVENTS],
    ),
    TestCase(
        "version1-throttled-tracked",
        1,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-1-throttled.txt",
        tracking_enabled=True,
        stat_filter=["Throttled", CRITICAL_EVENTS],
        track_filter="Postponed",
    ),
    TestCase(
        "version1-two-clients",
        1,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-1-two-clients.txt",
        tracking_enabled=False,
        stat_filter=CRITICAL_EVENTS,
    ),
    TestCase(
        "version1-throttled-boosted",
        1,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-1-throttled-boosted.txt",
        stat_filter=["ThrottlerPostponed", CRITICAL_EVENTS],
    ),
    TestCase(
        "version1-two-clients-remount-ro",
        1,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-1-two-clients-remount-ro.txt",
        stat_filter=CRITICAL_EVENTS,
    ),
    TestCase(
        "version1-two-clients-remount-rw",
        1,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-1-two-clients-remount-rw.txt",
        stat_filter=CRITICAL_EVENTS,
    ),
    TestCase(
        "version1-smallreqs",
        1,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-1-smallreqs.txt",
        stat_filter=CRITICAL_EVENTS,
    ),
    TestCase(
        "version2",
        2,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-2.txt",
        stat_filter=CRITICAL_EVENTS,
    ),
    TestCase(
        "version2-smallreqs",
        2,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-2-smallreqs.txt",
        stat_filter=CRITICAL_EVENTS,
    ),
    TestCase(
        "version2-smallreqs-skew",
        2,
        "cloud/blockstore/tests/loadtest/local/local-tablet-version-2-smallreqs-skew.txt",
        stat_filter=CRITICAL_EVENTS,
    ),
]


def __run_test(test_case):
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    backups_folder = get_unique_path_for_current_test(
        output_path=common.output_path(),
        sub_folder="backups",
    )
    ensure_path_exists(backups_folder)

    env = LocalLoadTest(
        "",
        server_app_config=server,
        tracking_enabled=test_case.tracking_enabled,
        storage_config_patches=[
            default_storage_config(test_case.tablet_version, backups_folder)
        ],
        use_in_memory_pdisks=True,
        bs_cache_file_path=backups_folder + "/bs_cache.txt",
    )

    try:
        ret = run_test(
            test_case.name,
            test_case.config_path,
            env.nbs_port,
            env.mon_port,
            stat_filter=test_case.stat_filter,
            nbs_log_path=env.nbs_log_path,
            track_filter=test_case.track_filter,
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


def test_load_custom():
    config = common.get_param("config")
    if config is not None:
        return __run_test(TestCase(
            "custom",
            common.source_path(config),
            stat_filter=common.get_param("stat_filter"),
            tracking_enabled=common.get_param("tracking_enabled"),
        ))
