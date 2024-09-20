import os
import requests
import signal
import time


from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server

from contrib.ydb.core.protos import config_pb2

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import yatest.common as yatest_common


def test_config_dispatcher():
    kikimr_binary_path = yatest_common.binary_path('contrib/ydb/apps/ydbd/ydbd')

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name='dynamic_storage_pool:1', kind='hdd', pdisk_user_kind=0),
            dict(name='dynamic_storage_pool:2', kind='ssd', pdisk_user_kind=0)
        ])

    nbs_binary_path = yatest_common.binary_path('cloud/blockstore/apps/server/nbsd')

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    server_app_config.ServerConfig.NodeType = 'nbs'

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

    server_app_config.ServerConfig.RootCertsFile = os.path.join(certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

    pm = yatest_common.network.PortManager()

    nbs_port = pm.get_port()
    nbs_secure_port = pm.get_port()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    # file config
    storage = TStorageServiceConfig()
    storage.ConfigsDispatcherServiceEnabled = True
    storage.ConfigDispatcherSettings.AllowList.Names.append('NameserviceConfigItem')

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

    naming_config = configurator.names_txt
    node = naming_config.Node.add(
        NodeId=2,
        Address='::1',
        Port=65535,
        Host='somewhere',
    )

    node.WalleLocation.DataCenter = 'xyz'
    node.WalleLocation.Rack = 'somewhere'
    node.WalleLocation.Body = 1

    app_config = config_pb2.TAppConfig()
    app_config.NameserviceConfig.MergeFrom(naming_config)

    kikimr_cluster.client.add_config_item(app_config)

    def wait_cms():
        for _ in range(10):
            url = f'http://localhost:{nbs.mon_port}/actors/dnameserver'
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            if r.text.find('somewhere') != -1:
                return True
            else:
                time.sleep(10)
        return False

    assert wait_cms()

    os.kill(nbs.pid, signal.SIGTERM)
