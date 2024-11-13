import os
import json
from contrib.ydb.tests.library.harness.daemon import Daemon
import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists


class RootKmsMock(Daemon):

    def __init__(self):

        self.__port_manager = yatest_common.PortManager()
        self.__port = self.__port_manager.get_port()

        output_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder="root-kms")
        ensure_path_exists(output_dir)

        certs_dir = os.path.join(output_dir, "certs")
        ensure_path_exists(certs_dir)

        self.__certs_dir = certs_dir

        binary_path = yatest_common.binary_path(
            "cloud/blockstore/tools/testing/root-kms-mock/root-kms-mock")

        config_path = os.path.join(output_dir, "config.txt")

        config = {
            "port": self.port,
            "keys": ["nbs-root-kms-key"],
            "certs_output_dir": certs_dir,
        }

        with open(config_path, "w") as f:
            json.dump(config, f)

        super(RootKmsMock, self).__init__(
            command=[binary_path, "--config-path", config_path],
            cwd=output_dir,
            timeout=180,
            stdout_file=os.path.join(output_dir, "stdout.txt"),
            stderr_file=os.path.join(output_dir, "stderr.txt"))

    @property
    def port(self):
        return self.__port

    @property
    def ca_path(self):
        return os.path.join(self.__certs_dir, 'ca.crt')

    @property
    def client_cert_path(self):
        return os.path.join(self.__certs_dir, 'client.crt')

    @property
    def client_key_path(self):
        return os.path.join(self.__certs_dir, 'client.key')
