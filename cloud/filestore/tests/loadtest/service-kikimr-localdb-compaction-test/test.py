import enum
import logging
import os
import threading
from time import sleep

import pytest
import yatest.common as common
from cloud.filestore.tests.python.lib.loadtest import run_load_test
from cloud.filestore.tests.python.lib.test_helpers import (
    get_restart_interval,
    get_storage_config,
    write_storage_config,
)


def set_new_compaction_policy(enabled: bool):
    logging.info(f"Setting NewLocalDBCompactionPolicyEnabled to {enabled}")
    storage_config = get_storage_config()
    storage_config.NewLocalDBCompactionPolicyEnabled = enabled
    write_storage_config(storage_config)
    sleep(get_restart_interval() * 1.5)


class OpType(enum.Enum):
    ENABLE_NEW_COMPACTION_POLICY = 1
    DISABLE_NEW_COMPACTION_POLICY = 2
    START_LOADTEST = 3


SEQUENCES = [
    [OpType.START_LOADTEST, OpType.ENABLE_NEW_COMPACTION_POLICY],
    [
        OpType.START_LOADTEST,
        OpType.ENABLE_NEW_COMPACTION_POLICY,
        OpType.DISABLE_NEW_COMPACTION_POLICY,
    ],
    [OpType.ENABLE_NEW_COMPACTION_POLICY, OpType.START_LOADTEST],
    [
        OpType.ENABLE_NEW_COMPACTION_POLICY,
        OpType.START_LOADTEST,
        OpType.DISABLE_NEW_COMPACTION_POLICY,
    ],
]


class Case(object):
    def __init__(self, name, config_path):
        self.name = name
        self.config_path = config_path


TESTS = [
    Case(
        "create-remove",
        "cloud/filestore/tests/loadtest/service-kikimr-localdb-compaction-test/create-remove.txt",
    ),
    Case(
        "read-write-validation",
        "cloud/filestore/tests/loadtest/service-kikimr-localdb-compaction-test/read-write-validation.txt",
    ),
]


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
@pytest.mark.parametrize("ops", SEQUENCES)
def test_load(test_case, ops):
    logging.info(f"Running test {test_case.name}; ops: {ops}")

    test_case.config_path = common.source_path(test_case.config_path)

    completed = False

    def loadtest_wrapper(
        test_case_name, test_case_config_path, nfs_server_port
    ):
        nonlocal completed
        run_load_test(test_case_name, test_case_config_path, nfs_server_port)
        completed = True

    new_compaction_policy_enabled = False
    for op in ops:
        if op == OpType.START_LOADTEST:
            logging.info("Creating FS and starting the load test")
            t = threading.Thread(
                target=loadtest_wrapper,
                args=(
                    test_case.name,
                    test_case.config_path,
                    os.getenv("NFS_SERVER_PORT"),
                ),
            )
            t.start()
        elif op == OpType.ENABLE_NEW_COMPACTION_POLICY:
            new_compaction_policy_enabled = True
            set_new_compaction_policy(True)
        elif op == OpType.DISABLE_NEW_COMPACTION_POLICY:
            new_compaction_policy_enabled = False
            set_new_compaction_policy(False)

    t.join()
    assert completed, "Load test did not complete successfully"

    # On the test completion, revert the changes back
    if new_compaction_policy_enabled:
        set_new_compaction_policy(False)

    logging.info(os.system("ss -tpna"))
