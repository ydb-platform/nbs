import json
import os
from pathlib import Path

from cloud.storage.core.tools.common.python.daemon import Daemon
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.tasks.test.common.processes import register_process, kill_processes


SERVICE_NAME = "access_service_nebius"


class AccessServiceServer(Daemon):
    def __init__(self, config_path: str, work_dir: str):
        command = [yatest_common.binary_path(
            "cloud/storage/core/tools/testing/access_service_nebius/service/access-service-mock")]
        command += ["--config-path", config_path]
        super(AccessServiceServer, self).__init__(
            commands=[command],
            cwd=work_dir,
            service_name=SERVICE_NAME,
        )

# viewer_permissions = [
#             {"permission": "disk-manager.disks.get", "resource": folder_id},
#             {"permission": "disk-manager.disks.list", "resource": folder_id},
#         ]
#         editor_permissions = [
#             {"permission": "disk-manager.disks.create", "resource": folder_id},
#             {"permission": "disk-manager.disks.delete", "resource": folder_id},
#             {"permission": "disk-manager.disks.update", "resource": folder_id},
#         ]
#         config = {
#             "port": self.__access_service_port,
#             "cert_file": cert_file,
#             "cert_key_file": cert_key_file,
#             "accounts": [
#                 {
#                     "permissions": editor_permissions + viewer_permissions,
#                     "id": "DiskEditor",
#                     "is_unknown_subject": False,
#                     "token": "DiskEditorToken",
#                 },
#                 {
#                     "permissions": viewer_permissions,
#                     "id": "Viewer",
#                     "is_unknown_subject": False,
#                     "token": "ViewerToken",
#                 },
#                 {
#                     "permissions": [
#                         {"permission": "disk-manager.disks.create", "resource": "resource"},
#                     ],
#                     "id": "WrongResource",
#                     "is_unknown_subject": False,
#                     "token": "WrongResourceToken",
#                 },
#                 {
#                     "permissions": editor_permissions,
#                     "id": "UnknownSubject",
#                     "is_unknown_subject": True,
#                     "token": "UnknownSubjectToken",
#                 },
#             ],
#         }

class AccessServiceLauncher:
    def __init__(
            self,
            cert_file: str,
            cert_key_file: str,
            folder_id: str,
            accounts_config_path: str,
        ):


        self.__port_manager = yatest_common.PortManager()
        self.__access_service_port = self.__port_manager.get_port()
        config = {
            "port": self.__access_service_port,
            "cert_file": cert_file,
            "cert_key_file": cert_key_file,
            "accounts": accounts,
        }
        assert len(set([x["token"] for x in config["accounts"]])) == len(config["accounts"])
        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)

        config_file = os.path.join(working_dir, 'access_service_config.txt')
        with open(config_file, 'w') as file:
            json.dump(config, file)
        self.__daemon = AccessServiceServer(
            config_file,
            working_dir,
        )

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def access_service_port(self):
        return self.__access_service_port
