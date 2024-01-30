import yatest.common as common

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test
from cloud.storage.core.protos.authorization_mode_pb2 import EAuthorizationMode


def test_load():
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = False
    server.ServerConfig.RootCertsFile = common.source_path(
        "cloud/blockstore/tests/certs/server.crt")
    cert = server.ServerConfig.Certs.add()
    cert.CertFile = common.source_path(
        "cloud/blockstore/tests/certs/server.crt")
    cert.CertPrivateKeyFile = common.source_path(
        "cloud/blockstore/tests/certs/server.key")
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    storage = TStorageServiceConfig()
    storage.AuthorizationMode = EAuthorizationMode.Value("AUTHORIZATION_REQUIRE")
    storage.FolderId = "test_folder_id"

    env = LocalLoadTest(
        "",
        server_app_config=server,
        enable_access_service=True,
        storage_config_patches=[storage],
        enable_tls=True,
        use_in_memory_pdisks=True,
    )

    env.access_service.authenticate("test_auth_token")
    env.access_service.authorize("test_auth_token")

    client = TClientConfig()
    client.RootCertsFile = common.source_path(
        "cloud/blockstore/tests/certs/server.crt")
    client.AuthToken = "test_auth_token"

    try:
        ret = run_test(
            "load",
            common.source_path(
                "cloud/blockstore/tests/loadtest/local-auth/local.txt"),
            env.nbs_secure_port,
            env.mon_port,
            client_config=client,
            enable_tls=True,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret
