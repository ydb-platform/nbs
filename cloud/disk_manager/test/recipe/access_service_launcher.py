import os

from cloud.storage.core.tools.common.python.daemon import Daemon
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.disk_manager.test.common.processes import register_process, kill_processes

DEFAULT_CONFIG_TEMPLATE = """
Port: {port}
CertFile: "{cert_file}"
PrivateKeyFile: "{private_key_file}"
IamTokenToUserId: <
    key: "TestToken"
    value: "UserAllowAll"
>
IamTokenToUserId: <
    key: "TestTokenDisksOnly"
    value: "UserAllowDisksOnly"
>
Rules: <
    IdPattern: "UserAllowAll"
    PermissionPattern: ".*"
>
Rules: <
    IdPattern: "UserAllowDisksOnly"
    PermissionPattern: "disk-manager\\\\.disks\\\\..*"
>
"""

SERVICE_NAME = "access_service"


class AccessServiceServer(Daemon):

    def __init__(self, config_file, working_dir):
        command = [yatest_common.binary_path(
            "cloud/disk_manager/test/mocks/accessservice/accessservice-mock")]
        command += [
            "--config", config_file
        ]
        super(AccessServiceServer, self).__init__(
            commands=[command],
            cwd=working_dir)


class AccessServiceLauncher:

    def __init__(self, cert_file, private_key_file):
        self.__port_manager = yatest_common.PortManager()
        self.__port = self.__port_manager.get_port()

        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)

        config_file = os.path.join(working_dir, 'access_service_config.txt')
        with open(config_file, "w") as f:
            f.write(DEFAULT_CONFIG_TEMPLATE.format(
                port=self.__port,
                cert_file=cert_file,
                private_key_file=private_key_file
            ))
        self.__daemon = AccessServiceServer(config_file, working_dir)

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return self.__port
