import pytest
from collections import namedtuple

from cloud.filestore.tests.python.lib.client import NfsCliClient
from cloud.filestore.config.server_pb2 import TServerAppConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.tests.python.lib.server import NfsServer, wait_for_nfs_server
from cloud.filestore.tests.python.lib.server import wait_for_filestore_vhost
from cloud.filestore.tests.python.lib.daemon_config import NfsServerConfigGenerator
from cloud.filestore.tests.python.lib.daemon_config import NfsVhostConfigGenerator

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus

from google.protobuf.text_format import MessageToString

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import yatest.common as yatest_common


CFG_PREFIX = 'Cloud.Filestore.'


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

    s.TenantAndNodeTypeFilter.Tenant = '/Root'
    s.TenantAndNodeTypeFilter.NodeType = node_type

    action.AddConfigItem.ConfigItem.MergeStrategy = 1  # OVERWRITE

    response = client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def setup_cms_configs(kikimr_client):
    enable_custom_cms_configs(kikimr_client)

    # filestore-server
    storage = TStorageConfig()
    storage.NodeIndexCacheMaxNodes = 100
    storage.SchemeShardDir = "/Root"

    update_cms_config(kikimr_client, 'StorageConfig', storage, 'filestore_server')

    # vhost
    storage = TStorageConfig()
    storage.NodeIndexCacheMaxNodes = 200
    storage.SchemeShardDir = "/Root"

    update_cms_config(kikimr_client, 'StorageConfig', storage, 'filestore_vhost')

    # global
    storage = TStorageConfig()
    storage.NodeIndexCacheMaxNodes = 300
    storage.SchemeShardDir = "/Root"

    update_cms_config(kikimr_client, 'StorageConfig', storage, '')


def setup_kikimr(is_secure_kikimr):
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        grpc_ssl_enable=is_secure_kikimr,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    return configurator


def setup_filestore(configurator, kikimr_cluster, secure_kikimr, secure_filestore, node_type, binary_path, configurator_type):
    kikimr_port = list(kikimr_cluster.nodes.values())[0].port
    kikimr_ssl_port = list(kikimr_cluster.nodes.values())[0].grpc_ssl_port

    server_config = TServerAppConfig()

    storage_config = TStorageConfig()
    if secure_filestore and secure_kikimr:
        storage_config.NodeRegistrationRootCertsFile = configurator.grpc_tls_ca_path
        storage_config.NodeRegistrationCert.CertFile = configurator.grpc_tls_cert_path
        storage_config.NodeRegistrationCert.CertPrivateKeyFile = configurator.grpc_tls_key_path
    storage_config.NodeType = node_type

    domain = configurator.domains_txt.Domain[0].Name

    port = kikimr_port
    if secure_filestore and kikimr_ssl_port is not None:
        port = kikimr_ssl_port

    nfs_configurator = configurator_type(
        binary_path=binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=True,
        kikimr_port=port,
        domain=domain,
        storage_config=storage_config,
        use_secure_registration=secure_filestore
    )

    return nfs_configurator


def check_filestore(server, nfs_configurator):
    nfs_client_binary_path = yatest_common.binary_path("cloud/filestore/apps/client/filestore-client")

    client = NfsCliClient(nfs_client_binary_path, nfs_configurator.port)
    assert client.get_storage_service_config().get("NodeIndexCacheMaxNodes") == 100

    server.stop()

    return True


def setup_and_run_test_for_server(is_secure_kikimr, is_secure_filestore):
    kikimr_configurator = setup_kikimr(is_secure_kikimr)

    kikimr_cluster = kikimr_cluster_factory(configurator=kikimr_configurator)
    kikimr_cluster.start()

    nfs_binary_path = yatest_common.binary_path("cloud/filestore/apps/server/filestore-server")
    nfs_configurator = setup_filestore(
        kikimr_configurator,
        kikimr_cluster,
        is_secure_kikimr,
        is_secure_filestore,
        "filestore_server",
        nfs_binary_path,
        NfsServerConfigGenerator)

    nfs_configurator.generate_configs(
        kikimr_configurator.domains_txt,
        kikimr_configurator.names_txt)

    setup_cms_configs(kikimr_cluster.client)

    nfs_server = NfsServer(configurator=nfs_configurator)
    nfs_server.start()

    try:
        wait_for_nfs_server(nfs_server, nfs_configurator.port)
    except RuntimeError:
        return False

    return check_filestore(nfs_server, nfs_configurator)


def setup_and_run_test_for_vhost(is_secure_kikimr, is_secure_filestore):
    kikimr_configurator = setup_kikimr(is_secure_kikimr)

    kikimr_cluster = kikimr_cluster_factory(configurator=kikimr_configurator)
    kikimr_cluster.start()

    nfs_binary_path = yatest_common.binary_path("cloud/filestore/apps/vhost/filestore-vhost")
    nfs_configurator = setup_filestore(
        kikimr_configurator,
        kikimr_cluster,
        is_secure_kikimr,
        is_secure_filestore,
        "filestore_vhost",
        nfs_binary_path,
        NfsVhostConfigGenerator)

    nfs_configurator.generate_configs(
        kikimr_configurator.domains_txt,
        kikimr_configurator.names_txt)

    setup_cms_configs(kikimr_cluster.client)

    nfs_server = NfsServer(configurator=nfs_configurator)
    nfs_server.start()

    try:
        wait_for_filestore_vhost(nfs_server, nfs_configurator.port)
    except RuntimeError:
        return False

    return True


TestCase = namedtuple("TestCase", "SecureKikimr SecureFilestore Result")
Scenarios = [
    TestCase(SecureKikimr=False, SecureFilestore=False, Result=True),
    TestCase(SecureKikimr=True, SecureFilestore=True, Result=True),
    TestCase(SecureKikimr=False, SecureFilestore=True, Result=False),
]


@pytest.mark.parametrize("secure_kikimr, secure_filestore, result", Scenarios)
def test_server_registration(secure_kikimr, secure_filestore, result):
    assert setup_and_run_test_for_server(secure_kikimr, secure_filestore) == result


@pytest.mark.parametrize("secure_kikimr, secure_filestore, result", Scenarios)
def test_vhost_registration(secure_kikimr, secure_filestore, result):
    assert setup_and_run_test_for_vhost(secure_kikimr, secure_filestore) == result
