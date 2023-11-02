import json
import os
import socket

from subprocess import run, PIPE

from os.path import join

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, \
    TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, get_nbs_counters, get_sensor_by_name

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import yatest.common as yatest_common


def __query_available_storage(nbs_port):
    p = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")

    args = [
        p, "queryavailablestorage",
        "--host", "localhost",
        "--port", str(nbs_port),
        "--verbose", "error",
        "--agent-id", socket.getfqdn()
    ]

    r = run(args, stdout=PIPE, universal_newlines=True)

    assert r.returncode == 0

    return json.loads(r.stdout)


def test_spare_node():
    kikimr_binary_path = yatest_common.binary_path("ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    nbs_binary_path = yatest_common.binary_path(
        "cloud/blockstore/apps/server/nbsd")

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    server_app_config.ServerConfig.NodeType = 'nbs_control'

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

    server_app_config.ServerConfig.RootCertsFile = join(certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

    pm = yatest_common.network.PortManager()

    nbs_port = pm.get_port()
    nbs_secure_port = pm.get_port()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage = TStorageServiceConfig()
    storage.DisableLocalService = True
    storage.KnownSpareNodes.append(socket.getfqdn())

    nbs = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        nbs_secure_port=nbs_secure_port,
        nbs_port=nbs_port,
        kikimr_binary_path=kikimr_binary_path,
        nbs_binary_path=nbs_binary_path)

    nbs.start()

    wait_for_nbs_server(nbs.nbs_port)

    r = __query_available_storage(nbs.nbs_port)

    assert json.dumps(r) == "{}"

    sensors = get_nbs_counters(nbs.mon_port)['sensors']
    is_spare_node = get_sensor_by_name(sensors, 'server', 'IsSpareNode')

    assert is_spare_node == 1

    nbs.stop()
