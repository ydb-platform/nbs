import os
import pytest
import threading
import time

import yatest.common as common

from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import run_test


class FailAndHealBSGroupInBackground(threading.Thread):

    def __init__(self, pdisk_path, kikimr):
        super().__init__()
        self.__pdisk_path = pdisk_path
        self.__kikimr = kikimr

    def run(self, *args, **kwargs):
        # let the test write some data
        time.sleep(30)
        # temporarily break BS group
        os.rename(self.__pdisk_path, self.__pdisk_path + ".bkp")
        self.__kikimr.restart_nodes()
        # wait some time to provoke potentially non-recoverable side-effects
        time.sleep(60)
        # heal the BS group
        os.rename(self.__pdisk_path + ".bkp", self.__pdisk_path)
        self.__kikimr.restart_nodes()


def default_storage_config():
    storage = storage_config_with_default_limits()
    storage.SSDSystemChannelPoolKind = "system"
    storage.SSDLogChannelPoolKind = "system"
    storage.SSDIndexChannelPoolKind = "system"
    storage.SSDMixedChannelPoolKind = "ssd"
    storage.SSDMergedChannelPoolKind = "ssd"
    return storage


class TestCase(object):

    def __init__(self, name, config_path):
        self.name = name
        self.config_path = config_path


TESTS = [
    TestCase(
        "default",
        "cloud/blockstore/tests/loadtest/local-bsgroup-failure/local-tablet-version-default.txt",
    ),
]


def __run_test(test_case):
    env = LocalLoadTest(
        "",
        storage_config_patches=[default_storage_config()],
        dynamic_pdisks=[dict(user_kind=1)],
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="system", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=1),
        ],
    )

    pdisk_path = ""
    for info in env.pdisks_info:
        if info["pdisk_user_kind"] == 1:
            pdisk_path = info["pdisk_path"]
            break
    assert pdisk_path != ""

    failAndHealTask = FailAndHealBSGroupInBackground(
        pdisk_path,
        env.kikimr_cluster,
    )
    failAndHealTask.start()

    try:
        ret = run_test(
            test_case.name,
            common.source_path(test_case.config_path),
            env.nbs_port,
            env.mon_port,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    failAndHealTask.join()
    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    return __run_test(test_case)
