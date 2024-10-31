import itertools
import pytest

import yatest.common as common

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test
from cloud.blockstore.pylibs.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists


class TestCase(object):

    def __init__(
            self,
            name,
            config_path,
            snapshot,
            tablet_version,
            ipc_type="IPC_GRPC",
    ):
        self.name = name + "%s_%s" % ("_snapshot" if snapshot else "", tablet_version)
        self.config_path = config_path
        self.snapshot = snapshot
        self.tablet_version = tablet_version
        self.ipc_type = ipc_type


def _generate_test_cases():
    return [
        TestCase(name, config_path, snapshot_mode, tablet_version)
        for name, config_path, snapshot_mode, tablet_version in itertools.product(
            ["local-encryption-nonoverlay"],
            ["cloud/blockstore/tests/loadtest/local-encryption/local-encryption-nonoverlay.txt"],
            [False, True],
            ["1", "2"],
        )
    ] + [
        TestCase(name, config_path, encryption_mode, tablet_version, ipc_type)
        for name, config_path, encryption_mode, tablet_version, ipc_type in itertools.product(
            ["local-encryption-nonoverlay-endpoint"],
            ["cloud/blockstore/tests/loadtest/local-encryption/local-encryption-nonoverlay-endpoint.txt"],
            [False, True],
            ["1", "2"],
            ["IPC_GRPC"],
        )
    ] + [
        TestCase(name, config_path, encryption_mode, tablet_version)
        for name, config_path, encryption_mode, tablet_version in itertools.product(
            ["local-encryption-overlay"],
            ["cloud/blockstore/tests/loadtest/local-encryption/local-encryption-overlay.txt"],
            [False, True],
            ["1", "2"],
        )
    ] + [
        TestCase(name, config_path, encryption_mode, tablet_version, ipc_type)
        for name, config_path, encryption_mode, tablet_version, ipc_type in itertools.product(
            ["local-encryption-overlay-endpoint"],
            ["cloud/blockstore/tests/loadtest/local-encryption/local-encryption-overlay-endpoint.txt"],
            [False, True],
            ["1", "2"],
            ["IPC_GRPC"],
        )
    ]


TESTS = _generate_test_cases()


def __prepare_test_config(test_case):
    with open(test_case.config_path, 'r') as file:
        filedata = file.read()

    if test_case.snapshot:
        encryption_params = "KeyHash: \"TestEncryptionKeyHash\""
        create_encryption_params = encryption_params
        zero_rate = '0'
    else:
        key_file_path = common.source_path(
            'cloud/blockstore/tests/loadtest/local-encryption/encryption-key.txt')
        encryption_params = "KeyPath { FilePath: \"" + key_file_path + "\" }"
        create_encryption_params = ""
        zero_rate = '100'

    filedata = filedata.replace('$CREATE_ENCRYPTION_PARAMS', create_encryption_params)
    filedata = filedata.replace('$ENCRYPTION_PARAMS', encryption_params)
    filedata = filedata.replace('$TABLET_VERSION', test_case.tablet_version)
    filedata = filedata.replace('$IPC_TYPE', test_case.ipc_type)
    filedata = filedata.replace('$ZERO_RATE', zero_rate)

    config_folder = get_unique_path_for_current_test(
        output_path=common.output_path(),
        sub_folder="test_configs")
    ensure_path_exists(config_folder)
    prepared_config_path = config_folder + "/" + test_case.name + ".txt"
    with open(prepared_config_path, 'w') as file:
        file.write(filedata)
    return prepared_config_path


def __run_test(test_case):
    nbd_socket_suffix = "_nbd"

    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.ServerConfig.NbdEnabled = True
    server.ServerConfig.NbdSocketSuffix = nbd_socket_suffix
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        use_in_memory_pdisks=True,
    )

    config_path = __prepare_test_config(test_case)

    client = TClientConfig()
    client.NbdSocketSuffix = nbd_socket_suffix

    try:
        ret = run_test(
            test_case.name,
            config_path,
            env.nbs_port,
            env.mon_port,
            nbs_log_path=env.nbs_log_path,
            client_config=client,
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
