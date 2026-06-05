import logging

from cloud.storage.core.tools.common.python.daemon import Daemon
from cloud.tasks.test.common.processes import register_process, kill_processes
from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

_logger = logging.getLogger(__file__)

SERVICE_NAME = "s3"
S3_QUOTA_PROXY_SERVICE_NAME = "s3-quota-proxy"


class S3Service(Daemon):
    def __init__(self, port, working_dir):
        command = [
            yatest_common.binary_path('contrib/python/moto/bin/moto_server'),
            "s3",
            "--port", str(port)
        ]
        super(S3Service, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name=SERVICE_NAME)


class S3QuotaProxyService(Daemon):
    def __init__(self, port, s3_port, quotas, working_dir):
        command = [
            yatest_common.binary_path(
                'cloud/disk_manager/test/mocks/s3_quota_proxy/s3quota-proxy'
            ),
            "--port", str(port),
            "--s3-port", str(s3_port),
        ]
        for quota in quotas:
            command += ["--quota", quota]

        super(S3QuotaProxyService, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name=S3_QUOTA_PROXY_SERVICE_NAME)


class S3Launcher:
    def __init__(self, quotas=None):
        if quotas is None:
            quotas = []

        self.__port_manager = yatest_common.PortManager()
        self.__moto_port = self.__port_manager.get_port()
        self.__port = (
            self.__port_manager.get_port()
            if quotas else self.__moto_port
        )

        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)
        self.__moto = S3Service(self.__moto_port, working_dir)
        self.__quota_proxy = None
        if quotas:
            self.__quota_proxy = S3QuotaProxyService(
                self.__port,
                self.__moto_port,
                quotas,
                working_dir,
            )

    def start(self):
        self.__moto.start()
        register_process(SERVICE_NAME, self.__moto.pid)
        if self.__quota_proxy is not None:
            self.__quota_proxy.start()
            register_process(
                S3_QUOTA_PROXY_SERVICE_NAME,
                self.__quota_proxy.pid,
            )

    @staticmethod
    def stop():
        kill_processes(S3_QUOTA_PROXY_SERVICE_NAME)
        kill_processes(SERVICE_NAME)

    def stop_service(self):
        if self.__quota_proxy is not None:
            self.__quota_proxy.stop()
        self.__moto.stop()

    @property
    def port(self):
        return self.__port
