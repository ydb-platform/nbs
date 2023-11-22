import requests
import subprocess
import time

from .config import NbsConfigurator

from cloud.storage.core.tools.common.python.core_pattern import core_pattern
from cloud.storage.core.tools.common.python.daemon import Daemon

import yatest.common as yatest_common

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists


class _Counters:

    def __init__(self, data):
        self.__data = data

    def find(self, sensor: str):
        for s in self.__data["sensors"]:
            labels = s.get("labels", {})

            if labels.get("sensor") == sensor:
                return s
        return None


def _get_counters(mon_port):
    url = f"http://localhost:{mon_port}/counters/counters=blockstore/json"
    r = requests.get(url, timeout=10)
    r.raise_for_status()

    return _Counters(r.json())


class Nbs(Daemon):

    def __init__(self, mon_port, server_port, commands, cwd):
        self.__mon_port = mon_port
        self.__port = server_port

        super(Nbs, self).__init__(
            commands=commands,
            cwd=cwd,
            ping_path='/blockstore',
            ping_port=mon_port,
            ping_success_codes=[200],
            core_pattern=core_pattern(commands[0][0], cwd))

    @property
    def port(self):
        return self.__port

    @property
    def mon_port(self):
        return self.__mon_port

    @property
    def counters(self):
        return _get_counters(self.mon_port)


class DiskAgent(Daemon):

    def __init__(self, mon_port, server_port, commands, cwd):
        self.__mon_port = mon_port
        self.__port = server_port

        super(DiskAgent, self).__init__(
            commands=commands,
            cwd=cwd,
            ping_path='/blockstore/disk_agent',
            ping_port=mon_port,
            ping_success_codes=[200],
            core_pattern=core_pattern(commands[0][0], cwd))

    @property
    def port(self):
        return self.__port

    @property
    def mon_port(self):
        return self.__mon_port

    @property
    def counters(self):
        return _get_counters(self.mon_port)

    def wait_for_registration(self, delay_sec=1, max_retry_number=None):
        url = f"http://localhost:{self.mon_port}/blockstore/disk_agent"

        i = 0
        while True:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            if r.text.find('Registered') != -1:
                return True
            i += 1
            if max_retry_number is not None and i >= max_retry_number:
                break
            time.sleep(delay_sec)

        return False


def start_nbs(config: NbsConfigurator):
    exe_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")

    cwd = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="nbs-server"
    )
    ensure_path_exists(cwd)

    config_path = get_unique_path_for_current_test(
        output_path=cwd,
        sub_folder="cfg"
    )
    ensure_path_exists(config_path)

    config.install(config_path)

    commands = [exe_path] + config.params

    nbs = Nbs(
        mon_port=config.mon_port,
        server_port=config.server_port,
        commands=[commands],
        cwd=cwd,
    )

    nbs.start()

    return nbs


def start_disk_agent(config: NbsConfigurator):
    exe_path = yatest_common.binary_path("cloud/blockstore/apps/disk_agent/diskagentd")

    cwd = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="nbs-disk-agent"
    )
    ensure_path_exists(cwd)

    config_path = get_unique_path_for_current_test(
        output_path=cwd,
        sub_folder="cfg"
    )
    ensure_path_exists(config_path)

    config.install(config_path)

    commands = [exe_path] + config.params

    agent = DiskAgent(
        mon_port=config.mon_port,
        server_port=config.server_port,
        commands=[commands],
        cwd=cwd,
    )

    agent.start()

    return agent


def __modify_scheme(ydb):
    request = """
        ModifyScheme {
            WorkingDir: "/Root"
            OperationType: ESchemeOpCreateSubDomain
            SubDomain {
                Name: "nbs"
                Coordinators: 0
                Mediators: 0
                PlanResolution: 50
                TimeCastBucketsPerMediator: 2
        """
    for pool in ydb.config.dynamic_storage_pools:
        request += f"""
    StoragePools {{
        Name: "{pool['name']}"
        Kind: "{pool['kind']}"
    }}
    """
    request += "}}"

    subprocess.check_call([
        ydb.config.binary_path,
        "--server", f"grpc://localhost:{list(ydb.nodes.values())[0].port}",
        "db", "schema", "exec",
        request,
    ])


def __enable_custom_cms_configs(ydb):
    req = msgbus.TConsoleRequest()
    configs_config = req.SetConfigRequest.Config.ConfigsConfig

    restrictions = configs_config.UsageScopeRestrictions

    restrictions.AllowedTenantUsageScopeKinds.append(
        console.TConfigItem.NamedConfigsItem)
    restrictions.AllowedNodeTypeUsageScopeKinds.append(
        console.TConfigItem.NamedConfigsItem)

    response = ydb.client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def start_ydb():
    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd"),
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            {"name": "dynamic_storage_pool:1", "kind": "hdd", "pdisk_user_kind": 0},
            {"name": "dynamic_storage_pool:2", "kind": "ssd", "pdisk_user_kind": 0}
        ])

    ydb = kikimr_cluster_factory(configurator=configurator)
    ydb.start()

    __modify_scheme(ydb)
    __enable_custom_cms_configs(ydb)

    return ydb
