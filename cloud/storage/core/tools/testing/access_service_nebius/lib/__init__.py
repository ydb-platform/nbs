import json
import os

import requests

from cloud.storage.core.tools.common.python.daemon import Daemon

from cloud.tasks.test.common.processes import register_process, kill_processes


SERVICE_NAME = "access_service_nebius"


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


class AccessServiceLauncher:
    def __init__(
            self,
            binary_path: str,
            port: int,
            control_port: int,
            working_dir: str,
            cert_file: str,
            cert_key_file: str,
        ):
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

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    def create_account(
        self,
        user_id: str,
        token: str,
        is_unknown_subject: bool,
        permissions: list
    ):
        requests.post(
            f"localhost:{self._control_port}/",
            json={
                "is_unknown_subject": is_unknown_subject,
                "token": token,
                "id": user_id,
                "permissions": permissions,
            }
        ).raise_for_status()

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)
