import json
import logging
import os
import pytest
import requests
import signal
import subprocess
import time

from contrib.ydb.tests.library.common.yatest_common import PortManager

from cloud.blockstore.config.disk_pb2 import TDiskAgentConfig
from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, \
    TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.disk_agent_runner import LocalDiskAgent
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server, \
    wait_for_disk_agent, wait_for_secure_erase
from cloud.blockstore.tests.python.lib.nonreplicated_setup import setup_nonreplicated, \
    create_file_devices, setup_disk_registry_config_simple, enable_writable_state

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus
from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

import yatest.common as yatest_common


DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 4
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144
DISK_SIZE = 1048576


class Nbs(LocalNbs):
    BINARY_PATH = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")

    def create_volume(self, disk_id, return_code=0):
        logging.info(f'[NBS] create volume "{disk_id}"')

        return self.__rpc(
            "createvolume",
            "--disk-id", disk_id,
            "--blocks-count", str(DISK_SIZE),
            "--storage-media-kind", "nonreplicated",
            "--cloud-id", "yc-nbs",
            "--folder-id", "tests",
            return_code=return_code)

    def describe_volume(self, disk_id, return_code=0):
        logging.info(f'[NBS] describe volume "{disk_id}"')

        return self.__rpc(
            "describevolume",
            "--disk-id", disk_id,
            return_code=return_code)

    def change_device_state(self, device_id, state):
        logging.info(f'[NBS] change device "{device_id}" state to {state}')

        input = {
            "Message": "test",
            "ChangeDeviceState": {
                "DeviceUUID": device_id,
                "State": int(state)
            }
        }

        return self.__rpc(
            "executeaction",
            "--action", "diskregistrychangestate",
            "--input-bytes", json.dumps(input),
            stdout=subprocess.PIPE)

    def set_user_id(self, disk_id, user_id):
        logging.info(f'[NBS] set user id "{user_id}" for "{disk_id}"')

        input = {
            "DiskId": disk_id,
            "UserId": user_id
        }

        return self.__rpc(
            "executeaction",
            "--action", "setuserid",
            "--input-bytes", json.dumps(input),
            stdout=subprocess.PIPE)

    def __rpc(self, *args, **kwargs):
        input = kwargs.get("input")
        return_code = kwargs.get("return_code", 0)

        if input is not None:
            input = (input + "\n").encode("utf8")

        args = [self.BINARY_PATH] + list(args) + [
            "--host", "localhost",
            "--port", str(self.nbs_port),
            "--verbose", "error"]

        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=kwargs.get("stdout"),
            stderr=subprocess.PIPE)

        outs, errs = process.communicate(input=input)

        if outs:
            logging.info("blockstore-client's output: {}".format(outs))

        if errs:
            logging.error("blockstore-client's errors: {}".format(errs))

        assert process.returncode == return_code

        return outs


class BaseTestCase:

    def __init__(self, name):
        self.name = name
        self.canonical_file = None

    def prepare(self, kikimr):
        pass

    def run(self, nbs, devices):
        return None

    def teardown(self):
        pass


class TestNullNotifyService(BaseTestCase):

    def run(self, nbs, devices):
        nbs.create_volume("vol0")
        nbs.change_device_state(devices[0].uuid, 2)
        nbs.describe_volume("vol0")

        return None


class TestNotifyService(BaseTestCase):

    def __init__(self, name):
        super().__init__(name)

        self.canonical_file = f"{yatest_common.output_path()}/{name}-results.txt"
        self.__pm = PortManager()
        self.__port = self.__pm.get_port()

        notify_bin_path = yatest_common.binary_path(
            "cloud/blockstore/tools/testing/notify-mock/notify-mock")

        certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

        self.notify = subprocess.Popen(
            [
                notify_bin_path,
                '--port', str(self.__port),
                '--ssl-cert-file', os.path.join(certs_dir, 'server.crt'),
                '--ssl-key-file', os.path.join(certs_dir, 'server.key'),
                '--output-path', self.canonical_file
            ],
            stdin=None,
            stdout=None,
            stderr=subprocess.PIPE)

        while True:
            try:
                r = requests.get(f"https://localhost:{self.__port}/ping", verify=False)
                r.raise_for_status()
                logging.info(f"Notify service: {r.text}")
                break
            except Exception as e:
                logging.warning(f"Failed to connect to Notify service ({e}). Retry")
                time.sleep(1)
                continue

    def run(self, nbs, devices):
        nbs.create_volume("vol0")
        nbs.change_device_state(devices[0].uuid, 2)
        nbs.describe_volume("vol0")

        return self.canonical_file

    def prepare(self, kikimr):
        req = msgbus.TConsoleRequest()
        action = req.ConfigureRequest.Actions.add()

        custom_cfg = action.AddConfigItem.ConfigItem.Config.NamedConfigs.add()
        custom_cfg.Name = "Cloud.NBS.NotifyConfig"
        custom_cfg.Config = f'''
            Endpoint: "https://localhost:{self.__port}/notify/v1/send"
        '''.encode('utf-8')

        s = action.AddConfigItem.ConfigItem.UsageScope

        s.TenantAndNodeTypeFilter.Tenant = '/Root/nbs'
        s.TenantAndNodeTypeFilter.NodeType = 'main'

        action.AddConfigItem.ConfigItem.MergeStrategy = console.TConfigItem.MERGE

        response = kikimr.invoke(req, 'ConsoleRequest')
        assert response.Status.Code == StatusIds.SUCCESS

    def teardown(self):
        self.notify.send_signal(signal.SIGTERM)
        self.notify.communicate()

        assert self.notify.returncode == 0


class TestNotifyByUserId(TestNotifyService):

    def run(self, nbs, devices):
        nbs.create_volume("vol0")
        nbs.set_user_id('vol0', 'vasya')
        nbs.change_device_state(devices[0].uuid, 2)
        nbs.describe_volume("vol0")

        return self.canonical_file


class TestNotifyDiskBackOnline(TestNotifyService):

    def run(self, nbs, devices):
        nbs.create_volume("vol0")
        nbs.change_device_state(devices[0].uuid, 2)
        nbs.describe_volume("vol0")
        nbs.change_device_state(devices[0].uuid, 0)
        time.sleep(7)

        return self.canonical_file


def __run_test(test_case):
    logging.info(f"Start: {test_case.name}")

    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    devices = create_file_devices(
        None,   # dir
        DEFAULT_DEVICE_COUNT,
        DEFAULT_BLOCK_SIZE,
        DEFAULT_BLOCK_COUNT_PER_DEVICE)

    disk_agent_config_patch = TDiskAgentConfig()
    disk_agent_config_patch.DedicatedDiskAgent = True
    setup_nonreplicated(
        kikimr_cluster.client,
        [devices],
        disk_agent_config_patch)

    test_case.prepare(kikimr_cluster.client)

    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

    server_app_config.ServerConfig.RootCertsFile = os.path.join(certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

    storage = TStorageServiceConfig()
    storage.AllocationUnitNonReplicatedSSD = 1
    storage.AcquireNonReplicatedDevices = True
    storage.ClientRemountPeriod = 1000
    storage.NonReplicatedInfraTimeout = 60000
    storage.NonReplicatedAgentMinTimeout = 3000
    storage.NonReplicatedAgentMaxTimeout = 3000
    storage.NonReplicatedDiskRecyclingPeriod = 5000
    storage.DisableLocalService = False
    storage.NodeType = 'main'

    nbs = Nbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=kikimr_binary_path,
        nbs_binary_path=nbs_binary_path)

    nbs.start()
    logging.info(f"Wait for nbs (port {nbs.nbs_port}) ...")
    wait_for_nbs_server(nbs.nbs_port)

    nbs_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
    enable_writable_state(nbs.nbs_port, nbs_client_binary_path)
    setup_disk_registry_config_simple(
        devices,
        nbs.nbs_port,
        nbs_client_binary_path)

    # node with DiskAgent

    disk_agent_binary_path = yatest_common.binary_path("cloud/blockstore/apps/disk_agent/diskagentd")
    storage.NodeType = 'disk-agent'
    storage.DisableLocalService = True

    disk_agent = LocalDiskAgent(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=kikimr_binary_path,
        disk_agent_binary_path=disk_agent_binary_path)

    disk_agent.start()
    logging.info("Wait for agent...")
    wait_for_disk_agent(disk_agent.mon_port)

    logging.info("Wait for DiskAgent registration...")
    wait_for_secure_erase(nbs.mon_port)

    ret = None
    try:
        logging.info("Run...")
        ret = test_case.run(nbs, devices)
        logging.info("Done")
    finally:
        nbs.stop()
        disk_agent.stop()
        kikimr_cluster.stop()
        logging.info("Remove temporary device files")
        for d in devices:
            d.handle.close()
            os.unlink(d.path)

        test_case.teardown()

    if ret is not None:
        return yatest_common.canonical_file(ret, local=True)

    return None


TESTS = [
    TestNullNotifyService("null"),
    TestNotifyService("notify"),
    TestNotifyByUserId("user-id"),
    TestNotifyDiskBackOnline("back-online")
]


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_notify(test_case):
    return __run_test(test_case)
