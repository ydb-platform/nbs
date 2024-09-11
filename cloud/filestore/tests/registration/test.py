from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.tests.python.lib.server import NfsServer, wait_for_nfs_server
from cloud.filestore.tests.python.lib.daemon_config import NfsServerConfigGenerator

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import yatest.common as yatest_common


def setup_and_run_test(is_secure_kikimr, is_secure_filestore):
    kikimr_binary_path = yatest_common.binary_path("ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        grpc_ssl_enable=is_secure_kikimr,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    nfs_binary_path = yatest_common.binary_path("cloud/filestore/apps/server/filestore-server")

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port
    kikimr_ssl_port = list(kikimr_cluster.nodes.values())[0].grpc_ssl_port

    server_config = TServerAppConfig()
    server_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    storage_config = TStorageConfig()

    if is_secure_filestore and is_secure_kikimr:
        storage_config.NodeRegistrationRootCertsFile = configurator.grpc_tls_ca_path
        storage_config.NodeRegistrationCert.CertFile = configurator.grpc_tls_cert_path
        storage_config.NodeRegistrationCert.CertPrivateKeyFile = configurator.grpc_tls_key_path

    storage_config.NodeType = "filestore_server"

    domain = configurator.domains_txt.Domain[0].Name

    port = kikimr_port
    if is_secure_filestore and kikimr_ssl_port is not None:
        port = kikimr_ssl_port

    nfs_configurator = NfsServerConfigGenerator(
        binary_path=nfs_binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=True,
        kikimr_port=port,
        domain=domain,
        storage_config=storage_config,
        use_secure_registration=is_secure_filestore
    )
    nfs_configurator.generate_configs(configurator.domains_txt, configurator.names_txt)

    nfs_server = NfsServer(configurator=nfs_configurator)

    nfs_server.start()

    try:
        wait_for_nfs_server(nfs_server, nfs_configurator.port)
    except RuntimeError:
        return False

    nfs_server.stop()

    return True


def test_registration_non_secure():
    assert setup_and_run_test(False, False)


def test_registration_secure():
    assert setup_and_run_test(True, True)


def test_fail_registration_at_wrong_port():
    assert not setup_and_run_test(False, True)
