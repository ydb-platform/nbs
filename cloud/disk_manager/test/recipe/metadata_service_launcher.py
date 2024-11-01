import os

from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.tasks.test.common.processes import register_process, kill_processes
from cloud.blockstore.pylibs.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

DEFAULT_CONFIG_TEMPLATE = """
Port: {port}
AccessToken: "TestToken"
"""

SERVICE_NAME = "metadata_service"


class MetadataServiceServer(Daemon):

    def __init__(self, config_file, working_dir):
        command = [yatest_common.binary_path(
            "cloud/disk_manager/test/mocks/metadata/metadata-mock")]
        command += [
            "--config", config_file
        ]
        super(MetadataServiceServer, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name=SERVICE_NAME)


class MetadataServiceLauncher:

    def __init__(self):
        self.__port_manager = yatest_common.PortManager()
        self.__port = self.__port_manager.get_port()

        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)

        config_file = os.path.join(working_dir, 'metadata_service_config.txt')
        with open(config_file, "w") as f:
            f.write(DEFAULT_CONFIG_TEMPLATE.format(
                port=self.__port,
            ))
        self.__daemon = MetadataServiceServer(config_file, working_dir)

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def url(self):
        return "http://localhost:{port}".format(port=self.__port)
