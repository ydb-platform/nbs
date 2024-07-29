from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.tests.python.lib.server import NfsServer, wait_for_nfs_server
from cloud.filestore.tests.python.lib.daemon_config import NfsServerConfigGenerator

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import yatest.common as yatest_common


def setup_and_run_test(is_secure):
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        grpc_ssl_enable=is_secure,
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

    if is_secure:
        server_config.ServerConfig.RootCertsFile = configurator.grpc_tls_ca_path
        cert = server_config.ServerConfig.Certs.add()
        cert.CertFile = configurator.grpc_tls_cert_path
        cert.CertPrivateKeyFile = configurator.grpc_tls_key_path

    server_config.ServerConfig.NodeType = "nfs_control"

    storage_config = TStorageConfig()

    domain = configurator.domains_txt.Domain[0].Name

    nfs_configurator = NfsServerConfigGenerator(
        binary_path=nfs_binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=True,
        kikimr_port=kikimr_port,
        domain=domain,
        storage_config=storage_config,
        use_secure_registration=is_secure,
        grpc_ssl_port=kikimr_ssl_port if is_secure else None
    )
    nfs_configurator.generate_configs(configurator.domains_txt, configurator.names_txt)

    nfs_server = NfsServer(configurator=nfs_configurator)

    nfs_server.start()

    wait_for_nfs_server(nfs_server, nfs_configurator.port)

    nfs_server.stop()


def test_node_type_nonsecure():
    setup_and_run_test(False)


def test_node_type_secure():
    setup_and_run_test(True)
