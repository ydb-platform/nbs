import logging
import requests
import time

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.storage.core.tools.common.python.daemon import Daemon
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists

logger = logging.getLogger(__name__)


class AccessService:
    # See: https://github.com/ydb-platform/nbs/blob/main/ydb/core/protos/auth.proto#L52
    access_service_type = "Yandex_v2"

    def __init__(self, host, port, control_server_port):
        self.__host = host
        self.__port = port
        self.__control_server_port = control_server_port
        self.__binary_path = yatest_common.binary_path(
            "cloud/storage/core/tools/testing/access_service/mock/accessservice-mock")
        self.__cwd = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        self.__control_url = "http://{}:{}".format(
            self.__host,
            self.__control_server_port)
        ensure_path_exists(self.__cwd)
        command = [self.__binary_path]
        command += [
            "--host", self.__host,
            "--port", str(self.__port),
            "--control-server-port", str(self.__control_server_port)
        ]
        self.__daemon = Daemon(
            commands=[command],
            cwd=self.__cwd,
            service_name="access_service",
        )

    @property
    def daemon(self):
        return self.__daemon

    def start(self):
        try:
            logger.debug("Running access_service with cwd: " + self.__cwd)
            self.__daemon.start()

            attempts = 0

            while True:
                try:
                    requests.get(self.__url("ping")).raise_for_status()
                    break
                except Exception as e:
                    logger.info("ping attempt %i failed: %s" % (attempts, e))
                    attempts += 1

                    if attempts == 10:
                        raise

                    time.sleep(1)

        except Exception:
            logger.exception("access_service start failed")
            self.stop()
            raise

    def stop(self):
        self.__daemon.stop()

    def __get_pid(self):
        return self.__daemon.pid

    def __url(self, path):
        return "{}/{}".format(self.__control_url, path)

    def __put_token(self, path, iam_token):
        account_data = {"user_account": {"id": iam_token}}
        requests.put(
            self.__url(path),
            json=account_data).raise_for_status()

    def authenticate(self, iam_token):
        self.__put_token("authenticate", iam_token)

    def authorize(self, iam_token):
        self.__put_token("authorize", iam_token)

    @property
    def pid(self):
        return self.__get_pid()
