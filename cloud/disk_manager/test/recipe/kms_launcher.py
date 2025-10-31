from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.tasks.test.common.processes import register_process, kill_processes
from ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import ydb.tests.library.common.yatest_common as yatest_common

SERVICE_NAME = "kms"


class KmsServer(Daemon):

    def __init__(self, port, working_dir):
        command = [yatest_common.binary_path(
            "cloud/disk_manager/test/mocks/kms/kms-mock")]
        command += ["--port", str(port)]
        super(KmsServer, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name=SERVICE_NAME)


class KmsLauncher:

    def __init__(self):
        self.__port_manager = yatest_common.PortManager()
        self.__port = self.__port_manager.get_port()

        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)

        self.__daemon = KmsServer(self.__port, working_dir)

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return self.__port
