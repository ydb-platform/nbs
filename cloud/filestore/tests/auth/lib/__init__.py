import os

import yatest.common as common

from pathlib import Path

from google.protobuf.text_format import MessageToString

from cloud.filestore.config.client_pb2 import TClientAppConfig, TClientConfig
from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from cloud.storage.core.tools.testing.access_service_new.lib import NewAccessService


class TestFixture:
    def __init__(self):
        self.__port = os.getenv("NFS_SERVER_SECURE_PORT")
        self.__binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
        self.__client_config_path = Path(common.output_path()) / "client-config.txt"
        self.folder_id = os.getenv("TEST_FOLDER_ID")
        access_service_port = os.getenv("ACCESS_SERVICE_PORT")
        access_service_control_port = os.getenv("ACCESS_SERVICE_CONTROL_PORT")
        if os.getenv("ACCESS_SERVICE_TYPE") == "new":
            self.access_service = NewAccessService(
                "localhost",
                int(access_service_port),
                int(access_service_control_port),
            )
        else:
            self.access_service = AccessService(
                "localhost",
                access_service_port,
                access_service_control_port,
            )
        client_config = TClientAppConfig()
        client_config.ClientConfig.CopyFrom(TClientConfig())
        client_config.ClientConfig.RootCertsFile = common.source_path(
            "cloud/filestore/tests/certs/server.crt")
        client_config.ClientConfig.SecurePort = int(self.__port)
        self.__client_config_path.write_text(MessageToString(client_config))

    def get_client(self, auth_token, use_unix_socket=False):
        # auth_token MUST be a non-empty string; otherwise, the client will look
        # for the IAM token config at the default path, which does not exist.
        client = FilestoreCliClient(
            self.__binary_path,
            self.__port,
            cwd=common.output_path(),
            auth_token=auth_token,
            config_path=str(self.__client_config_path),
            check_exit_code=False,
            return_json=True,
            use_unix_socket=use_unix_socket,
            verbose=True,
        )
        return client
