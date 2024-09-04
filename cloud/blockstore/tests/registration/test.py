import pytest

from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, start_disk_agent

from contrib.ydb.core.protos import msgbus_pb2 as msgbus
from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from google.protobuf.text_format import MessageToString

from collections import namedtuple


CFG_PREFIX = 'Cloud.NBS.'


def update_cms_config(ydb_client, name, config, node_type):
    req = msgbus.TConsoleRequest()
    action = req.ConfigureRequest.Actions.add()

    custom_cfg = action.AddConfigItem.ConfigItem.Config.NamedConfigs.add()
    custom_cfg.Name = CFG_PREFIX + name
    custom_cfg.Config = MessageToString(config, as_one_line=True).encode()

    s = action.AddConfigItem.ConfigItem.UsageScope

    s.TenantAndNodeTypeFilter.Tenant = '/Root/nbs'
    s.TenantAndNodeTypeFilter.NodeType = node_type

    action.AddConfigItem.ConfigItem.MergeStrategy = 1  # OVERWRITE

    response = ydb_client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def setup_cms_configs(ydb_client):
    # blockstore-server control
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = 'nbs_control'
    storage.SchemeShardDir = '/Root/nbs'

    update_cms_config(ydb_client, 'StorageServiceConfig', storage, 'nbs_control')

    # blockstore-server
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = 'nbs'
    storage.SchemeShardDir = '/Root/nbs'

    update_cms_config(ydb_client, 'StorageServiceConfig', storage, 'nbs')

    # disk-agent
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = 'disk-agent'
    storage.SchemeShardDir = '/Root/nbs'

    update_cms_config(ydb_client, 'StorageServiceConfig', storage, 'disk-agent')

    # global
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = ''
    storage.SchemeShardDir = '/Root/nbs'

    update_cms_config(ydb_client, 'StorageServiceConfig', storage, '')


def prepare(ydb, kikimr_ssl, blockstore_ssl, node_type):
    nbs_configurator = NbsConfigurator(ydb, ssl_registration=blockstore_ssl)
    nbs_configurator.generate_default_nbs_configs()

    if blockstore_ssl and kikimr_ssl:
        nbs_configurator.files['storage'].NodeRegistrationRootCertsFile = ydb.config.grpc_tls_ca_path
        nbs_configurator.files['storage'].NodeRegistrationCert.CertFile = ydb.config.grpc_tls_cert_path
        nbs_configurator.files['storage'].NodeRegistrationCert.CertPrivateKeyFile = ydb.config.grpc_tls_key_path
    nbs_configurator.files['storage'].NodeType = node_type
    nbs_configurator.files['storage'].DisableLocalService = False

    return nbs_configurator


def setup_and_run_test_for_server(kikimr_ssl, blockstore_ssl, node_type):
    ydb = start_ydb(grpc_ssl_enable=kikimr_ssl)
    setup_cms_configs(ydb.client)

    nbs = start_nbs(prepare(ydb, kikimr_ssl, blockstore_ssl, node_type))

    client = NbsClient(nbs.port)

    r = client.get_storage_service_config().get('ManuallyPreemptedVolumesFile')
    assert r == node_type

    nbs.kill()

    return True


def setup_and_run_test_for_da(kikimr_ssl, blockstore_ssl):
    ydb = start_ydb(grpc_ssl_enable=kikimr_ssl)

    setup_cms_configs(ydb.client)

    nbs = start_nbs(prepare(ydb, kikimr_ssl, blockstore_ssl, 'nbs_control'))

    da_configurator = prepare(ydb, kikimr_ssl, blockstore_ssl, 'disk-agent')
    da_configurator.files["disk-agent"] = generate_disk_agent_txt(agent_id='')
    da = start_disk_agent(da_configurator)

    da.kill()
    nbs.kill()

    return True


TestCase = namedtuple('TestCase', 'SecureKikimr SecureBlockstore Result')
Scenarios = [
    TestCase(SecureKikimr=False, SecureBlockstore=False, Result=True),
    TestCase(SecureKikimr=True, SecureBlockstore=True, Result=True)
]


@pytest.mark.parametrize('kikimr_ssl, blockstore_ssl, result', Scenarios)
def test_control_registration(kikimr_ssl, blockstore_ssl, result):
    assert setup_and_run_test_for_server(kikimr_ssl, blockstore_ssl, 'nbs_control') == result


@pytest.mark.parametrize('kikimr_ssl, blockstore_ssl, result', Scenarios)
def test_server_registration(kikimr_ssl, blockstore_ssl, result):
    assert setup_and_run_test_for_server(kikimr_ssl, blockstore_ssl, 'nbs') == result


@pytest.mark.parametrize('kikimr_ssl, blockstore_ssl, result', Scenarios)
def test_da_registration(kikimr_ssl, blockstore_ssl, result):
    assert setup_and_run_test_for_da(kikimr_ssl, blockstore_ssl) == result
