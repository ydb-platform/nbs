import pytest

import yatest.common as common

from cloud.blockstore.public.sdk.python.client import CreateClient, Session
import cloud.blockstore.public.sdk.python.protos as protos
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import run_test

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists


def default_storage_config(backups_folder, use_binary_format_for_backups):
    storage = storage_config_with_default_limits()
    storage.SSDSystemChannelPoolKind = "ssd"
    storage.SSDLogChannelPoolKind = "ssd"
    storage.SSDIndexChannelPoolKind = "ssd"
    storage.SSDMixedChannelPoolKind = "ssd"
    storage.SSDMergedChannelPoolKind = "ssd"

    storage.TabletBootInfoBackupFilePath = \
        backups_folder + "/tablet_boot_info_backup.txt"
    storage.PathDescriptionBackupFilePath = \
        backups_folder + "/path_description_backup.txt"

    storage.UseBinaryFormatForPathDescriptionBackup = use_binary_format_for_backups

    return storage


def storage_config_with_emergency_mode(backups_folder, use_binary_format_for_backups):
    storage = default_storage_config(backups_folder, use_binary_format_for_backups)
    storage.HiveProxyFallbackMode = True
    storage.SSProxyFallbackMode = True
    storage.DontPassSchemeShardDirWhenRegisteringNodeInEmergencyMode = True
    storage.DisableLocalService = True
    return storage


# TODO(svartmetal): remove this when YDB learns not to erase dynamic groups
# data after formatting of static pdisk
def spoil_bs_controller_config(kikimr_cluster):
    flat_bs_controller = [{
        "info": {
            "channels": [{
                "channel": 0,
                "channel_erasure_name": str(kikimr_cluster.config.static_erasure),
                "history": [{
                    "from_generation": 0,
                    "group_id": 100500
                }]
            }]
        }
    }]
    kikimr_cluster.config.yaml_config["system_tablets"]["flat_bs_controller"] = flat_bs_controller
    kikimr_cluster.config.write_proto_configs(kikimr_cluster.config_path)


def format_static_pdisks(kikimr_cluster):
    for node_id in kikimr_cluster.config.all_node_ids():
        for pdisk in kikimr_cluster.config.pdisks_info:
            if pdisk["pdisk_user_kind"] == 0:
                kikimr_cluster.nodes[node_id].format_pdisk(**pdisk)


class TestCase(object):

    def __init__(self, name, config_path, use_binary_format_for_backups):
        self.name = name
        self.config_path = config_path
        self.use_binary_format_for_backups = use_binary_format_for_backups


TESTS = [
    TestCase(
        "default",
        "cloud/blockstore/tests/loadtest/local-emergency/local-tablet-version-default.txt",
        False,  # UseBinaryFormatForBackups
    ),
    TestCase(
        "default",
        "cloud/blockstore/tests/loadtest/local-emergency/local-tablet-version-default.txt",
        True,  # UseBinaryFormatForBackups
    ),
]


def __run_test(test_case):
    backups_folder = get_unique_path_for_current_test(
        output_path=common.output_path(),
        sub_folder="backups",
    )
    ensure_path_exists(backups_folder)

    env = LocalLoadTest(
        "",
        storage_config_patches=[
            default_storage_config(
                backups_folder,
                test_case.use_binary_format_for_backups,
            ),
        ],
        dynamic_pdisks=[dict(user_kind=1)],
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="system", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=1)
        ],
        bs_cache_file_path=backups_folder + "/bs_cache.txt",
        load_configs_from_cms=False,
    )

    client = CreateClient(env.endpoint)
    client.create_volume("vol0", 4096, 1000000, 1, protos.EStorageMediaKind.Value("STORAGE_MEDIA_SSD"))

    session = Session(client, "vol0", "")
    session.mount_volume()
    session.write_blocks(100500, [b'\1' * 4096])

    client.execute_action(action="BackupPathDescriptions", input_bytes=str.encode(""))
    client.execute_action(action="BackupTabletBootInfos", input_bytes=str.encode(""))

    format_static_pdisks(env.kikimr_cluster)
    # spoil config to prevent BS Controller from starting otherwise it will
    # erase dynamic groups data
    spoil_bs_controller_config(env.kikimr_cluster)
    env.kikimr_cluster.restart_nodes()

    env.nbs.storage_config_patches = [
        storage_config_with_emergency_mode(
            backups_folder,
            test_case.use_binary_format_for_backups,
        ),
    ]
    env.nbs.restart()

    data = session.read_blocks(100499, 3, "")
    # check data (that was written) together with left & right neighborhood
    assert data == [b''] + [b'\1' * 4096] + [b'']
    session.unmount_volume()

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
