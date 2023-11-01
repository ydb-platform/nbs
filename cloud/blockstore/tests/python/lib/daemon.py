import subprocess

from .config import NbsConfigurator

from cloud.storage.core.tools.common.python.core_pattern import core_pattern
from cloud.storage.core.tools.common.python.daemon import Daemon

import yatest.common as yatest_common

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, \
    ensure_path_exists


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


def start_ydb():
    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=yatest_common.binary_path("ydb/apps/ydbd/ydbd"),
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            {"name": "dynamic_storage_pool:1", "kind": "hdd", "pdisk_user_kind": 0},
            {"name": "dynamic_storage_pool:2", "kind": "ssd", "pdisk_user_kind": 0}
        ])

    ydb = kikimr_cluster_factory(configurator=configurator)
    ydb.start()

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

    return ydb
