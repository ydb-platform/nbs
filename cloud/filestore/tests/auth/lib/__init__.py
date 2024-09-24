import os

import yatest.common as common

from cloud.filestore.tests.python.lib.client import NfsCliClient
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from cloud.storage.core.tools.testing.access_service_new.lib import NewAccessService


class TestFixture:
    def __init__(self):
        self.port = os.getenv("NFS_SERVER_PORT")
        self.binary_path = common.binary_path("cloud/filestore/apps/client/filestore-client")
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

    def get_client(self, auth_token):
        client = NfsCliClient(
            self.binary_path,
            self.port,
            cwd=common.output_path(),
            auth_token=auth_token,
        )
        return client
