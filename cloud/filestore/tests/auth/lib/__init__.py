import os

import yatest.common as common

from pathlib import Path

from google.protobuf.text_format import MessageToString

from cloud.filestore.config.client_pb2 import TClientAppConfig, TClientConfig
from cloud.filestore.tests.python.lib.client import NfsCliClient
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from cloud.storage.core.tools.testing.access_service_new.lib import NewAccessService


class TestFixture:
    def __init__(self):
        self.port = os.getenv("NFS_SERVER_SECURE_PORT")
        self.binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
        self._client_config_path = Path(common.output_path()) / "client-config.txt"
        self.folder_id = os.getenv("TEST_FOLDER_ID")
        access_service_port = os.getenv("ACCESS_SERVICE_PORT")
        access_service_control_port = os.getenv("ACCESS_SERVICE_CONTROL_PORT")
        self.access_service = AccessService(
            "localhost",
            access_service_port,
            access_service_control_port,
        )
        if os.getenv("ACCESS_SERVICE_TYPE") == "new":
            self.access_service = NewAccessService(
                "localhost",
                int(access_service_port),
                int(access_service_control_port),
            )
        self.create_client_config()

    def create_client_config(self):
        client_config = TClientAppConfig()
        client_config.ClientConfig.CopyFrom(TClientConfig())
        client_config.ClientConfig.RootCertsFile = common.source_path(
            "cloud/filestore/tests/certs/server.crt")
        client_config.ClientConfig.SecurePort = int(self.port)
        client_config.ClientConfig.RetryTimeout = 1
        self._client_config_path.write_text(MessageToString(client_config))

    def get_client(self, auth_token):
        client = NfsCliClient(
            self.binary_path,
            self.port,
            cwd=common.output_path(),
            auth_token=auth_token,
            config_path=str(self._client_config_path),
            check_exit_code=False,
        )
        return client
