import json
import requests
import subprocess
import socket
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


def _match(labels, query):
    for name, value in query.items():
        v = labels.get(name)
        if v is None or v != value:
            return False
    return True


class _Counters:

    def __init__(self, data):
        self.__data = data

    def find(self, query):
        for s in self.__data["sensors"]:
            if _match(s.get("labels", {}), query):
                return s

        return None

    def find_all(self, queries):
        r = []
        for s in self.__data["sensors"]:
            for query in queries:
                if _match(s.get("labels", {}), query):
                    r.append(s)
                    break

        return r

    def __rep__(self):
        return json.dumps(self.__data)

    def __str__(self):
        return json.dumps(self.__data, indent=4)


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

    def __init__(self, mon_port, server_port, commands, cwd, config_path):
        self.__mon_port = mon_port
        self.__port = server_port
        self.__config_path = config_path

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
    def config_path(self):
        return self.__config_path

    @property
    def counters(self):
        return _get_counters(self.mon_port)

    def is_registered(self):
        url = f"http://localhost:{self.mon_port}/blockstore/disk_agent"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.find('Registered') != -1

    def wait_for_registration(self, delay_sec=1, max_retry_number=None):
        i = 0
        while True:
            if self.is_registered():
                return True
            i += 1
            if max_retry_number is not None and i >= max_retry_number:
                break
            time.sleep(delay_sec)

        return False


def start_nbs(config: NbsConfigurator, name='nbs-server', ydb_ssl_port=None):
    exe_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")

    cwd = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder=f"{name}-{config.ic_port}"
    )
    ensure_path_exists(cwd)

    config_path = get_unique_path_for_current_test(
        output_path=cwd,
        sub_folder="cfg"
    )
    ensure_path_exists(config_path)

    config.install(config_path, ydb_ssl_port=ydb_ssl_port)

    commands = [exe_path] + config.params

    nbs = Nbs(
        mon_port=config.mon_port,
        server_port=config.server_port,
        commands=[commands],
        cwd=cwd,
    )

    nbs.start()

    return nbs


def start_disk_agent(config: NbsConfigurator, name='disk-agent'):
    exe_path = yatest_common.binary_path("cloud/blockstore/apps/disk_agent/diskagentd")

    cwd = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder=f"{name}-{config.ic_port}"
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
        config_path=config_path,
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


def start_ydb(grpc_ssl_enable=False):
    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd"),
        use_in_memory_pdisks=True,
        grpc_ssl_enable=grpc_ssl_enable,
        dynamic_storage_pools=[
            {"name": "dynamic_storage_pool:1", "kind": "hdd", "pdisk_user_kind": 0},
            {"name": "dynamic_storage_pool:2", "kind": "ssd", "pdisk_user_kind": 0}
        ])

    ydb = kikimr_cluster_factory(configurator=configurator)
    ydb.start()

    __modify_scheme(ydb)
    __enable_custom_cms_configs(ydb)

    return ydb


def get_fqdn():
    name = socket.gethostname()
    r = socket.getaddrinfo(name, None, flags=socket.AI_CANONNAME)

    assert len(r) > 0

    _, _, _, canonname, _ = r[0]

    return canonname.lower()
