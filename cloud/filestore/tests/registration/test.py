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

import yatest.common as common
import google.protobuf.text_format as text_format

from library.python.testing.recipe import declare_recipe, set_env

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.storage.core.protos.authorization_mode_pb2 import EAuthorizationMode
from cloud.filestore.tests.python.lib.common import shutdown, get_restart_interval
from cloud.filestore.tests.python.lib.server import NfsServer, wait_for_nfs_server
from cloud.filestore.tests.python.lib.daemon_config import NfsServerConfigGenerator

import yatest.common as yatest_common


CFG_PREFIX = 'Cloud.NFS.'


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

    s.TenantAndNodeTypeFilter.Tenant = '/Root/nfs'
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

    server_config = TServerAppConfig()
    server_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    storage_config = TStorageConfig()

    domain = kikimr_configurator.domains_txt.Domain[0].Name

    nfs_configurator = NfsServerConfigGenerator(
        binary_path=nfs_binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=args.verbose,
        kikimr_port=kikimr_port,
        domain=domain,
        restart_interval=get_restart_interval(args.restart_interval),
        access_service_port=access_service_port,
        storage_config=storage_config,
    )
    nfs_configurator.generate_configs(kikimr_configurator.domains_txt, kikimr_configurator.names_txt)

    nfs_server = NfsServer(configurator=nfs_configurator)

    enable_custom_cms_configs(kikimr_cluster.client)

    # svm
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.SchemeShardDir = "/Root/nfs"

    update_cms_config(kikimr_cluster.client, 'StorageServiceConfig', storage, 'nfs_control')

    # bare metal hosts
    storage = TStorageServiceConfig()
    storage.DisableLocalService = True
    storage.SchemeShardDir = "/Root/nfs"

    update_cms_config(kikimr_cluster.client, 'StorageServiceConfig', storage, 'nfs_host')

    # global
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.SchemeShardDir = "/Root/nfs"

    update_cms_config(kikimr_cluster.client, 'StorageServiceConfig', storage, '')

    nfs_server.start()

    wait_for_nfs_server(nfs_server.nfs_port)

    client = NfsClient(nbs.nbs_port)
    # DisableLocalService = 1 for nbs
    assert client.get_storage_service_config().get("DisableLocalService", 0) == 1

    os.kill(nbs.pid, signal.SIGTERM)
