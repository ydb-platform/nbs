import os
import signal

from google.protobuf.text_format import MessageToString

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import yatest.common as yatest_common


CFG_PREFIX = 'Cloud.NBS.'


def enable_custom_cms_configs(client):
    req = msgbus.TConsoleRequest()
    configs_config = req.SetConfigRequest.Config.ConfigsConfig
    restrictions = configs_config.UsageScopeRestrictions
    restrictions.AllowedTenantUsageScopeKinds.append(
        console.TConfigItem.NamedConfigsItem)
    restrictions.AllowedNodeTypeUsageScopeKinds.append(
        console.TConfigItem.NamedConfigsItem)

    response = client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def update_cms_config(client, name, config, node_type):
    req = msgbus.TConsoleRequest()
    action = req.ConfigureRequest.Actions.add()

    custom_cfg = action.AddConfigItem.ConfigItem.Config.NamedConfigs.add()
    custom_cfg.Name = CFG_PREFIX + name
    custom_cfg.Config = MessageToString(config, as_one_line=True).encode()

    s = action.AddConfigItem.ConfigItem.UsageScope

    s.TenantAndNodeTypeFilter.Tenant = '/Root/nbs'
    s.TenantAndNodeTypeFilter.NodeType = node_type

    action.AddConfigItem.ConfigItem.MergeStrategy = 1  # OVERWRITE

    response = client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def test_node_type():
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")

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
    storage.DisableLocalService = True

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

    enable_custom_cms_configs(kikimr_cluster.client)

    # svm
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.SchemeShardDir = "/Root/nbs"

    update_cms_config(kikimr_cluster.client, 'StorageServiceConfig', storage, 'nbs_control')

    # bare metal hosts
    storage = TStorageServiceConfig()
    storage.DisableLocalService = True
    storage.SchemeShardDir = "/Root/nbs"

    update_cms_config(kikimr_cluster.client, 'StorageServiceConfig', storage, 'nbs')

    # global
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.SchemeShardDir = "/Root/nbs"

    update_cms_config(kikimr_cluster.client, 'StorageServiceConfig', storage, '')

    nbs.start()

    wait_for_nbs_server(nbs.nbs_port)

    client = NbsClient(nbs.nbs_port)
    # DisableLocalService = 1 for nbs
    assert client.get_storage_service_config().get("DisableLocalService", 0) == 1

    os.kill(nbs.pid, signal.SIGTERM)
