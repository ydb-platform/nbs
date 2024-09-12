# import os
import pytest

import yatest.common as common

from cloud.blockstore.public.sdk.python.client import CreateClient, Session
import cloud.blockstore.public.sdk.python.protos as protos
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import run_test

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists


def default_storage_config(cache_folder):
    storage = storage_config_with_default_limits()
    storage.SSDSystemChannelPoolKind = "ssd"
    storage.SSDLogChannelPoolKind = "ssd"
    storage.SSDIndexChannelPoolKind = "ssd"
    storage.SSDMixedChannelPoolKind = "ssd"
    storage.SSDMergedChannelPoolKind = "ssd"

    storage.TabletBootInfoBackupFilePath = \
        cache_folder + "/tablet_boot_info_backup.txt"
    storage.PathDescriptionBackupFilePath = \
        cache_folder + "/path_description_backup.txt"

    return storage


def storage_config_with_emergency_mode(cache_folder):
    storage = default_storage_config(cache_folder)
    storage.HiveProxyFallbackMode = True
    storage.SSProxyFallbackMode = True
    return storage


class TestCase(object):

    def __init__(self, name, config_path):
        self.name = name
        self.config_path = config_path


TESTS = [
    TestCase(
        "default",
        "cloud/blockstore/tests/loadtest/local-emergency/local-tablet-version-default.txt",
    ),
]


def __run_test(test_case):
    cache_folder = get_unique_path_for_current_test(
        output_path=common.output_path(),
        sub_folder="cache",
    )
    ensure_path_exists(cache_folder)

    storage_config_patches = [
        default_storage_config(cache_folder),
        storage_config_with_emergency_mode(cache_folder),
    ]

    env = LocalLoadTest(
        "",
        storage_config_patches=storage_config_patches,
        dynamic_pdisks=[dict(user_kind=1)],
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="system", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=1)
        ],
        bs_cache_file_path=cache_folder + "/bs_cache.txt",
    )

    client = CreateClient(env.endpoint)
    client.create_volume("vol0", 4096, 1000000, 1, protos.EStorageMediaKind.Value("STORAGE_MEDIA_SSD"))

    session = Session(client, "vol0", "")
    session.mount_volume()
    session.write_blocks(0, [b'\1' * 4096])
    session.unmount_volume()

    static_pdisk_paths = []
    for info in env.pdisks_info:
        if info["pdisk_user_kind"] == 0:
            static_pdisk_paths += [info["pdisk_path"]]
    assert len(static_pdisk_paths) == 1

    # Destroy static group in order to emulate emergency.
    # TODO: survive outage of kikimr static tablets.
    # os.remove(static_pdisk_paths[0])

    env.kikimr_cluster.restart_nodes()
    env.nbs.restart()

    try:
        ret = run_test(
            "emergency-%s" % test_case.name,
            common.source_path(test_case.config_path),
            env.nbs_port,
            env.mon_port,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    return __run_test(test_case)
