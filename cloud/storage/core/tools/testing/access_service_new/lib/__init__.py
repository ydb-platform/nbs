import json
import os

import requests

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.storage.core.tools.common.python.daemon import Daemon
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists


SERVICE_NAME = "access_service_new"


class AccessServiceServer(Daemon):
    def __init__(
        self,
        binary_path: str,
        config_path: str,
        work_dir: str
    ):
        command = [binary_path]
        command += ["--config-path", config_path]
        super(AccessServiceServer, self).__init__(
            commands=[command],
            cwd=work_dir,
            service_name=SERVICE_NAME,
        )


class NewAccessService:
    # See: https://github.com/ydb-platform/nbs/blob/main/contrib/ydb/core/protos/auth.proto#L52
    access_service_type = "Nebius_v1"

    def __init__(
        self,
        host: str,
        port: int,
        control_port: int,
        working_dir: str | None = None,
        binary_path: str | None = None,
        cert_file: str = "",
        cert_key_file: str = "",
    ):
        if working_dir is None:
            working_dir = get_unique_path_for_current_test(
                output_path=yatest_common.output_path(),
                sub_folder=""
            )
            ensure_path_exists(working_dir)
        if binary_path is None:
            binary_path = yatest_common.binary_path(
                "cloud/storage/core/tools/testing/access_service_new/mock/access-service-mock",
            )
        self._host = host
        config = {
            "port": port,
            "control_port": control_port,
            "cert_file": cert_file,
            "cert_key_file": cert_key_file,
        }
        self._control_port = control_port
        config_file = os.path.join(working_dir, 'access_service_config.txt')
        with open(config_file, 'w') as file:
            json.dump(config, file)
        self.__daemon = AccessServiceServer(
            binary_path,
            config_file,
            working_dir,
        )

    @property
    def daemon(self):
        return self.__daemon

    def start(self):
        self.__daemon.start()

    def create_account(
        self,
        user_id: str,
        token: str,
        is_unknown_subject: bool,
        permissions: list
    ):
        requests.post(
            f"http://{self._host}:{self._control_port}/",
            json={
                "is_unknown_subject": is_unknown_subject,
                "token": token,
                "id": user_id,
                "permissions": permissions,
            }
        ).raise_for_status()

    @property
    def pid(self):
        return self.__daemon.pid

    def stop(self):
        self.__daemon.stop()
