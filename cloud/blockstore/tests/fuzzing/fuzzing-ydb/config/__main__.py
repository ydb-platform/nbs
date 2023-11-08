import os
import yatest.common

from cloud.blockstore.config.server_pb2 import \
    TServerAppConfig, TServerConfig, TKikimrServiceConfig, TNullServiceConfig
from cloud.blockstore.tests.python.lib.test_base import thread_count
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs

from library.python.testing.recipe import declare_recipe

from contrib.ydb.core.protos.config_pb2 import TDomainsConfig
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


STORAGE_POOL = [
    dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
    dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
]


def kikimr_start():
    kikimr_binary_path = yatest.common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        has_cluster_uuid=False,
        use_in_memory_pdisks=True,
        dynamic_pdisks=[],
        dynamic_storage_pools=STORAGE_POOL)

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    return kikimr_cluster, configurator


def nbs_server_kikimr_configure(kikimr_cluster, configurator):
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.VhostEnabled = True
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    nbs = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        contract_validation=False,
        tracking_enabled=False,
        enable_access_service=False,
        enable_tls=False,
        dynamic_storage_pools=STORAGE_POOL)

    nbs.write_configs()
    return nbs


def nbs_server_null_configure():
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.VhostEnabled = True
    server_app_config.ServerConfig.StrictContractValidation = False

    server_app_config.NullServiceConfig.CopyFrom(TNullServiceConfig())
    server_app_config.NullServiceConfig.AddReadResponseData = True

    nbs = LocalNbs(
        0,
        TDomainsConfig(),
        server_app_config=server_app_config,
        contract_validation=False,
        tracking_enabled=False,
        enable_access_service=False,
        enable_tls=False,
        dynamic_storage_pools=STORAGE_POOL)

    nbs.write_configs()
    return nbs


def start(argv):
    if yatest.common.get_param("nbs_service_kind", "null") == "kikimr":
        kikimr_cluster, configurator = kikimr_start()
        nbs = nbs_server_kikimr_configure(kikimr_cluster, configurator)

        config_file_path = os.path.join(nbs.config_path(), "kikimr_port.txt")
        with open(config_file_path, "w") as config_file:
            config_file.write(str(list(kikimr_cluster.nodes.values())[0].port))
    else:
        nbs_server_null_configure()


def stop(argv):
    pass


if __name__ == "__main__":
    declare_recipe(start, stop)
