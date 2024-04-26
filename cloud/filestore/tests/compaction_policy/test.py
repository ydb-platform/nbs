import os
import pytest
from time import sleep
import threading
import logging
import enum

from cloud.filestore.tests.python.lib.loadtest import run_load_test

import google.protobuf.text_format as text_format
from cloud.filestore.config.storage_pb2 import TStorageConfig

import yatest.common as common


def get_nfs_config(name: str) -> str:
    configs_dir = os.getenv("NFS_CONFIG_DIR")
    assert configs_dir is not None, "NFS_CONFIG_DIR is not set"
    return os.path.join(configs_dir, name)

def get_nfs_storage_config_path() -> str:
    return get_nfs_config("storage.txt")

def get_storage_config() -> TStorageConfig:
    nfs_storage_config_path = get_nfs_storage_config_path()
    storage_config = TStorageConfig()
    with open(nfs_storage_config_path) as p:
        storage_config = text_format.Parse(
            p.read(),
            TStorageConfig())
    return storage_config

def set_storage_config(storage_config: TStorageConfig):
    nfs_storage_config_path = get_nfs_storage_config_path()
    with open(nfs_storage_config_path, "w") as f:
        f.write(text_format.MessageToString(storage_config))

RESTART_INTERVAL = 5

class OpType(enum.Enum):
    ENABLE_NEW_COMPACTION_POLICY = 1
    DISABLE_NEW_COMPACTION_POLICY = 2
    START_LOADTEST = 3

SEQUENCES = [
    [OpType.START_LOADTEST, OpType.ENABLE_NEW_COMPACTION_POLICY],
    [OpType.START_LOADTEST, OpType.ENABLE_NEW_COMPACTION_POLICY, OpType.DISABLE_NEW_COMPACTION_POLICY],
    [OpType.ENABLE_NEW_COMPACTION_POLICY, OpType.START_LOADTEST],
    [OpType.ENABLE_NEW_COMPACTION_POLICY, OpType.START_LOADTEST, OpType.DISABLE_NEW_COMPACTION_POLICY],
]

class Case(object):
    def __init__(self, name, config_path):
        self.name = name
        self.config_path = config_path

TESTS = [
    Case(
        "create-remove",
        "cloud/filestore/tests/compaction_policy/create-remove.txt",
    ),
    Case(
        "read-write-validation",
        "cloud/filestore/tests/compaction_policy/read-write-validation.txt",
    ),
]


def set_new_compaction_policy(new_compaction_policy_enabled: bool):
    logging.info(f"Setting NewCompactionPolicyEnabled to {new_compaction_policy_enabled}")
    storage_config = get_storage_config()
    storage_config.NewCompactionPolicyEnabled = new_compaction_policy_enabled
    set_storage_config(storage_config)
    sleep(RESTART_INTERVAL * 1.5)


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
@pytest.mark.parametrize("ops", SEQUENCES)
def test_load(test_case, ops):
    logging.info(f"Running test {test_case.name}; ops: {ops}")

    test_case.config_path = common.source_path(test_case.config_path)

    new_compaction_policy_enabled = False
    for op in ops.ops:
        if op == OpType.START_LOADTEST:
            logging.info("Creating FS and starting the load test")
            t = threading.Thread(target=run_load_test, args=(test_case.name, test_case.config_path, os.getenv("NFS_SERVER_PORT"),))
            t.start()
        elif op == OpType.ENABLE_NEW_COMPACTION_POLICY:
            new_compaction_policy_enabled = True
            set_new_compaction_policy(True)
        elif op == OpType.DISABLE_NEW_COMPACTION_POLICY:
            new_compaction_policy_enabled = False
            set_new_compaction_policy(False)

    t_result = t.join()
    logging.info(f"Test result: {t_result}")

    # On the test completion, revert the changes back
    if new_compaction_policy_enabled:
        set_new_compaction_policy(False)