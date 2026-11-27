import os

from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.tasks.test.common.processes import register_process, kill_processes
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

SERVICE_NAME = "compute"

EXPORT_CREDENTIALS_TEMPLATE = """
ExportCredentials {{
    {resource_field}: "{resource_id}"
    Credentials {{
        access_key_id: "{access_key_id}"
        secret_access_key: "{secret_access_key}"
    }}
}}
"""


class ComputeServer(Daemon):

    def __init__(self, port, working_dir, config_file=None):
        command = [yatest_common.binary_path(
            "cloud/disk_manager/test/mocks/compute/compute-mock")]
        if config_file is None:
            command += ["--port", str(port)]
        else:
            command += ["--config", config_file]
        super(ComputeServer, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name=SERVICE_NAME)


class ComputeLauncher:

    # export_credentials maps ("snapshot" | "image", resource id) to
    # (access key id, secret access key) returned by GetExportCredentials.
    def __init__(self, export_credentials=None):
        self.__port_manager = yatest_common.PortManager()
        self.__port = self.__port_manager.get_port()

        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)

        config_file = None
        if export_credentials:
            config_file = os.path.join(working_dir, 'compute_config.txt')
            with open(config_file, "w") as f:
                f.write('ListenAddress: "localhost:{}"\n'.format(self.__port))
                for (kind, resource_id), (key_id, secret) in export_credentials.items():
                    f.write(EXPORT_CREDENTIALS_TEMPLATE.format(
                        resource_field="SnapshotId" if kind == "snapshot" else "ImageId",
                        resource_id=resource_id,
                        access_key_id=key_id,
                        secret_access_key=secret,
                    ))

        self.__daemon = ComputeServer(self.__port, working_dir, config_file)

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    def stop_service(self):
        self.__daemon.stop()

    @property
    def port(self):
        return self.__port
