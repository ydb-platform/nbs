import logging
import os
import time

import yatest.common as yatest_common
from cloud.blockstore.config.server_pb2 import TServerConfig, \
    TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import \
    thread_count, wait_for_nbs_server, get_utils_counters, get_sensor
from cloud.blockstore.tests.python.lib.nonreplicated_setup import \
    setup_nonreplicated, enable_writable_state
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class CommonPaths:

    def __init__(self):
        self.kikimr_binary_path = \
            yatest_common.binary_path("ydb/apps/ydbd/ydbd")
        self.nbs_binary_path = \
            yatest_common.binary_path(
                "cloud/blockstore/apps/server/nbsd")
        self.certs_dir = \
            yatest_common.source_path('cloud/blockstore/tests/certs')
        self.nbs_client_path = \
            yatest_common.binary_path(
                "cloud/blockstore/apps/client/blockstore-client")


def get_dr_actor_count(nbs_mon_port):
    sensors = get_utils_counters(nbs_mon_port)["sensors"]
    return get_sensor(sensors, default_value=0,
                      database="/Root/nbs", slot="static",
                      execpool="User", sensor="ActorsAliveByActivity",
                      activity="NCloud::NBlockStore::NStorage::TDiskRegistryActor")


def wait_for_dr_metrics(nbs_mon_port):
    for i in range(60):
        actors_count = get_dr_actor_count(nbs_mon_port)
        if (actors_count > 0):
            break

        logging.info("Waiting for DR actor...")
        time.sleep(2)
    assert actors_count > 0


def setup_kikimr(paths):
    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=paths.kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()
    return kikimr_cluster, configurator


def setup_nbs(paths, kikimr_cluster, configurator, system_tablets_priority):
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    server_app_config.ServerConfig.RootCertsFile = os.path.join(
        paths.certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(paths.certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(paths.certs_dir, 'server.key')

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.SchemeShardDir = "/Root/nbs"
    storage.NodeType = 'nbs'
    storage.SystemTabletsPriority = system_tablets_priority

    nbs = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=paths.kikimr_binary_path,
        nbs_binary_path=paths.nbs_binary_path)

    nbs.start()

    wait_for_nbs_server(nbs.nbs_port)
    return nbs


def test_system_tablets_priority():
    common_paths = CommonPaths()

    kikimr_cluster, configurator = setup_kikimr(common_paths)

    # Initialize DR tablet configs.
    setup_nonreplicated(kikimr_cluster.client, [], agent_count=0)

    nbs_low_priority1 = setup_nbs(
        common_paths, kikimr_cluster, configurator, system_tablets_priority=-1)

    enable_writable_state(nbs_low_priority1.nbs_port,
                          common_paths.nbs_client_path)
    wait_for_dr_metrics(nbs_low_priority1.mon_port)

    nbs_low_priority2 = setup_nbs(
        common_paths, kikimr_cluster, configurator, system_tablets_priority=-2)
    nbs_high_priority = setup_nbs(
        common_paths, kikimr_cluster, configurator, system_tablets_priority=1)

    # DR shouldn't have been moved.
    wait_for_dr_metrics(nbs_low_priority1.mon_port)
    # Stop the node with DR.
    nbs_low_priority1.stop()

    # Hive should have moved DR to the node with highest priority.
    wait_for_dr_metrics(nbs_high_priority.mon_port)
    nbs_high_priority.stop()

    # DR should start on the node with lowest priority if it's the only one left.
    wait_for_dr_metrics(nbs_low_priority2.mon_port)
    nbs_low_priority2.stop()

    kikimr_cluster.stop()
