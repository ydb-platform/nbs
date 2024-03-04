import logging

import yatest

from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.tasks.test.common.processes import register_process, kill_processes
from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists

_logger = logging.getLogger(__file__)

SERVICE_NAME = "s3"


class S3Service(Daemon):
    def __init__(self, port, working_dir):
        command = [
            yatest.common.binary_path('contrib/python/moto/bin/moto_server'),
            "s3",
            "--port", str(port)
        ]
        super(S3Service, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name=SERVICE_NAME)


class S3Launcher:
    def __init__(self):
        self.__port = yatest.common.network.PortManager().get_port()
        working_dir = get_unique_path_for_current_test(
            output_path=yatest.common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)
        self.__daemon = S3Service(self.__port, working_dir)

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return self.__port
