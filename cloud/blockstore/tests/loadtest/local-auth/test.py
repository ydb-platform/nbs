import logging
import subprocess

import yatest.common as common

from pathlib import Path

from google.protobuf.text_format import MessageToString

from cloud.blockstore.config.client_pb2 import TClientAppConfig, TClientConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test
from cloud.storage.core.protos.authorization_mode_pb2 import EAuthorizationMode
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from cloud.storage.core.tools.testing.access_service_nebius.lib import NewAccessService


def create_server_app_config():
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.RootCertsFile = common.source_path(
        "cloud/blockstore/tests/certs/server.crt")
    cert = server.ServerConfig.Certs.add()
    cert.CertFile = common.source_path(
        "cloud/blockstore/tests/certs/server.crt")
    cert.CertPrivateKeyFile = common.source_path(
        "cloud/blockstore/tests/certs/server.key")
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    return server


def create_storage_service_config(folder_id="test_folder_id"):
    storage = TStorageServiceConfig()
    storage.AuthorizationMode = EAuthorizationMode.Value("AUTHORIZATION_REQUIRE")
    storage.FolderId = folder_id
    return storage


def test_load():
    server = create_server_app_config()
    server.ServerConfig.StrictContractValidation = False
    server.ServerConfig.ThreadsCount = thread_count()

    storage = create_storage_service_config("test_folder_id")

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


def create_client_config():
    client = TClientAppConfig()
    client.ClientConfig.CopyFrom(TClientConfig())
    client.ClientConfig.RootCertsFile = common.source_path(
        "cloud/blockstore/tests/certs/server.crt")
    return client


class _TestFixture:
    _binary_path = common.binary_path("cloud/blockstore/apps/client/blockstore-client")

    def __init__(self, access_service_type=AccessService, folder_id="test_folder_id"):
        server = create_server_app_config()
        storage = create_storage_service_config(folder_id)
        self._local_load_test = LocalLoadTest(
            "",
            server_app_config=server,
            enable_access_service=True,
            storage_config_patches=[storage],
            enable_tls=True,
            use_in_memory_pdisks=True,
            access_service_type=access_service_type,
        )
        self._client_config_path = Path(common.output_path()) / "client-config.txt"
        self._client_config = create_client_config()
        self._client_config.ClientConfig.SecurePort = self._local_load_test.nbs_secure_port
        self._client_config.ClientConfig.RetryTimeout = 1
        self._flush_config()
        self.folder_id = folder_id
        self._auth_token = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._local_load_test.tear_down()

    def run(self, *args, **kwargs):
        args = [self._binary_path, *args, "--config", str(self._client_config_path)]
        script_input = kwargs.get("input")
        if script_input is not None:
            script_input = script_input + "\n"

        env = {}
        if self._auth_token is not None:
            env['IAM_TOKEN'] = self._auth_token
        logging.info("running command: %s" % args)
        result = subprocess.run(
            args,
            cwd=kwargs.get("cwd"),
            check=False,
            capture_output=True,
            input=script_input,
            text=True,
            env=env,
        )
        return result

    def create_volume(self):
        result = self.run("createvolume", "--disk-id", "vol0", "--blocks-count", "25000")
        logging.info("Disk creation stdout: %s, stderr: %s", result.stdout, result.stderr)
        return result

    @property
    def access_service(self):
        return self._local_load_test.access_service

    def set_auth_token(self, token: str):
        self._auth_token = token

    def _flush_config(self):
        self._client_config_path.write_text(MessageToString(self._client_config))


def test_auth_unauthorized():
    with _TestFixture() as env:
        token = "test_auth_token"
        env.set_auth_token(token)
        env.access_service.authenticate(token)
        result = env.create_volume()
        assert result.returncode != 0
        assert "E_UNAUTHORIZED" in result.stderr


def test_auth_unauthenticated():
    with _TestFixture() as env:
        token = "test_auth_token"
        env.set_auth_token(token)
        env.access_service.authorize(token)
        result = env.create_volume()
        assert result.returncode != 0
        assert "E_UNAUTHORIZED" in result.stderr


def test_auth_empty_token():
    with _TestFixture() as env:
        env.set_auth_token("")
        env.access_service.authorize("test_auth_token")
        result = env.create_volume()
        assert result.returncode != 0
        assert "E_UNAUTHORIZED" in result.stderr


def test_new_auth_authorization_ok():
    with _TestFixture(NewAccessService) as env:
        token = "test_auth_token"
        env.set_auth_token(token)
        env.access_service.create_account(
            token,
            token,
            is_unknown_subject=False,
            permissions=[
                {"permission": "nbsInternal.disks.create", "resource": env.folder_id},
            ],
        )
        result = env.create_volume()
        assert result.returncode == 0


def test_new_auth_unauthorized():
    with _TestFixture(NewAccessService) as env:
        token = "test_auth_token"
        env.set_auth_token(token)
        env.access_service.create_account(
            "test_user",
            token,
            is_unknown_subject=False,
            permissions=[
                {"permission": "nbsInternal.disks.create", "resource": "some_other_folder"},
            ],
        )
        result = env.create_volume()
        assert result.returncode != 0
        assert "E_UNAUTHORIZED" in result.stderr


def test_new_auth_unauthenticated():
    with _TestFixture(NewAccessService) as env:
        env.set_auth_token("some_other_token")
        result = env.create_volume()
        assert result.returncode != 0
        assert "E_UNAUTHORIZED" in result.stderr


def test_new_auth_unknown_subject():
    with _TestFixture(NewAccessService) as env:
        token = "test_token"
        env.set_auth_token(token)
        env.access_service.create_account(
            "test_user",
            token,
            is_unknown_subject=True,
            permissions=[
                {"permission": "nbsInternal.disks.create", "resource": env.folder_id},
            ],
        )
        result = env.create_volume()
        assert result.returncode != 0
        assert "E_UNAUTHORIZED" in result.stderr
