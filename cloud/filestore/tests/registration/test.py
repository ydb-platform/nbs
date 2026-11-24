import pytest
from collections import namedtuple
import logging

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.config.server_pb2 import TServerAppConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.tests.python.lib.server import FilestoreServer, wait_for_filestore_server
from cloud.filestore.tests.python.lib.server import wait_for_filestore_vhost
from cloud.filestore.tests.python.lib.daemon_config import FilestoreServerConfigGenerator
from cloud.filestore.tests.python.lib.daemon_config import FilestoreVhostConfigGenerator
from cloud.storage.core.tests.ydb import Ydb

from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.core.protos import msgbus_pb2 as msgbus

from google.protobuf.text_format import MessageToString


import yatest.common as yatest_common

logger = logging.getLogger(__name__)

CFG_PREFIX = 'Cloud.Filestore.'
kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")


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


def setup_cms_configs(kikimr_client):
    enable_custom_cms_configs(kikimr_client)

    # filestore-server
    storage = TStorageConfig()
    storage.PreferredBlockSizeMultiplier = 100
    storage.SchemeShardDir = "/Root/nfs"

    update_cms_config(kikimr_client, 'StorageConfig', storage, 'filestore_server')

    # vhost
    storage = TStorageConfig()
    storage.PreferredBlockSizeMultiplier = 200
    storage.SchemeShardDir = "/Root/nfs"

    update_cms_config(kikimr_client, 'StorageConfig', storage, 'filestore_vhost')

    # global
    storage = TStorageConfig()
    storage.PreferredBlockSizeMultiplier = 300
    storage.SchemeShardDir = "/Root/nfs"

    update_cms_config(kikimr_client, 'StorageConfig', storage, '')


def wait_for_process(wait_function, process, port, check_function):
    try:
        wait_function(process, port)
    except RuntimeError:
        return False

    result = True
    if check_function is not None:
        result = check_function(process, port)

    process.stop()

    return result


def setup_kikimr(is_secure_kikimr):
    return Ydb.start(
        binary_path=kikimr_binary_path,
        grpc_ssl_enable=is_secure_kikimr)


def setup_filestore(
        ydb,
        secure_kikimr,
        secure_filestore,
        node_type,
        binary_path,
        configurator_type,
        ic_port=None):
    server_config = TServerAppConfig()

    storage_config = TStorageConfig()
    if secure_kikimr:
        storage_config.NodeRegistrationRootCertsFile = ydb.tls_ca_path
        storage_config.NodeRegistrationCert.CertFile = ydb.tls_cert_path
        storage_config.NodeRegistrationCert.CertPrivateKeyFile = ydb.tls_key_path
    storage_config.NodeType = node_type

    domain = ydb.domains_txt.Domain[0].Name

    port = ydb.port
    if secure_kikimr:
        port = ydb.ssl_port

    filestore_configurator = configurator_type(
        binary_path=binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=True,
        kikimr_port=port,
        domain=domain,
        storage_config=storage_config,
        use_secure_registration=secure_filestore,
        ic_port=ic_port
    )

    return filestore_configurator


def check_filestore_server(server, port):
    filestore_client_binary_path = yatest_common.binary_path("cloud/filestore/apps/client/filestore-client")

    client = FilestoreCliClient(filestore_client_binary_path, port)
    result = client.get_storage_service_config().get("PreferredBlockSizeMultiplier") == 100

    return result


def run_filestore(
        binary_path,
        is_secure_kikimr,
        is_secure_filestore,
        ydb,
        config_generator,
        ping_process_fn,
        check_process_fn,
        ic_port=None):

    filestore_configurator = setup_filestore(
        ydb,
        is_secure_kikimr,
        is_secure_filestore,
        "filestore_server",
        binary_path,
        config_generator,
        ic_port)

    filestore_configurator.generate_configs(
        ydb.domains_txt,
        ydb.names_txt)

    filestore_server = FilestoreServer(
        kikimr_binary_path=kikimr_binary_path,
        configurator=filestore_configurator,
        dynamic_storage_pools=ydb.dynamic_storage_pools,
        secure_kikimr=is_secure_kikimr,
    )
    filestore_server.start()

    ic_port = filestore_configurator.ic_port

    return wait_for_process(ping_process_fn, filestore_server, filestore_configurator.port, check_process_fn), ic_port


def setup_and_run_test_for_server(is_secure_kikimr, is_secure_filestore):
    ydb = setup_kikimr(is_secure_kikimr)

    setup_cms_configs(ydb.client)

    filestore_binary_path = yatest_common.binary_path("cloud/filestore/apps/server/filestore-server")

    result = run_filestore(
        filestore_binary_path,
        is_secure_kikimr,
        is_secure_filestore,
        ydb,
        FilestoreServerConfigGenerator,
        wait_for_filestore_server,
        check_filestore_server)

    ydb.stop()

    return result[0]


def setup_and_run_test_for_vhost(is_secure_kikimr, is_secure_filestore):
    ydb = setup_kikimr(is_secure_kikimr)

    setup_cms_configs(ydb.client)

    filestore_binary_path = yatest_common.binary_path("cloud/filestore/apps/vhost/filestore-vhost")

    result = run_filestore(
        filestore_binary_path,
        is_secure_kikimr,
        is_secure_filestore,
        ydb,
        FilestoreVhostConfigGenerator,
        wait_for_filestore_vhost,
        None)

    ydb.stop()

    return result[0]


def setup_and_run_registration_migration(
        binary_path,
        config_generator,
        wait_process,
        check_process):
    ydb = setup_kikimr(True)

    setup_cms_configs(ydb.client)

    ic_port = None
    for setting in (False, True, False):
        result = run_filestore(
            binary_path,
            setting,
            setting,
            ydb,
            config_generator,
            wait_process,
            check_process,
            ic_port)
        if result[0] is False:
            return result[0]

        if ic_port is None:
            ic_port = result[1]
        else:
            assert ic_port == result[1]

    ydb.stop()

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


def test_server_registration_migration():
    result = setup_and_run_registration_migration(
        yatest_common.binary_path("cloud/filestore/apps/server/filestore-server"),
        FilestoreServerConfigGenerator,
        wait_for_filestore_server,
        check_filestore_server)
    assert result is True


def test_vhost_registration_migration():
    result = setup_and_run_registration_migration(
        yatest_common.binary_path("cloud/filestore/apps/vhost/filestore-vhost"),
        FilestoreVhostConfigGenerator,
        wait_for_filestore_vhost,
        None)
    assert result is True
