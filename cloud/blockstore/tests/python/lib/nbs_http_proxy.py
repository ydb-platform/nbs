import os

from cloud.blockstore.pylibs.ydb.tests.library.harness.daemon import Daemon
from cloud.blockstore.pylibs.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common


CONFIG_TEMPLATE = """
SecurePort: {secure_port}
Port: {insecure_port}
Certs: <
    CertFile: "{cert_file}"
    CertPrivateKeyFile: "{private_key_file}"
>
NbsServerHost: "localhost"
NbsServerPort: {nbs_port}
NbsServerCertFile: "{root_certs_file}"
NbsServerInsecure: {nbs_server_insecure}
RootCertsFile: "{root_certs_file}"
"""


class _HttpProxyDaemon(Daemon):

    def __init__(self, config_file, working_dir, secure_port=None, insecure_port=None):
        command = [yatest_common.binary_path(
            "cloud/blockstore/tools/http_proxy/blockstore-http-proxy")]
        command += [
            "--config", config_file
        ]

        super(_HttpProxyDaemon, self).__init__(
            command=command,
            cwd=working_dir,
            timeout=180)

        self.__secure_port = secure_port
        self.__insecure_port = insecure_port

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, tb):
        self.stop()

    @property
    def pid(self):
        return super(_HttpProxyDaemon, self).daemon.process.pid

    @property
    def secure_port(self):
        return self.__secure_port

    @property
    def insecure_port(self):
        return self.__insecure_port


def create_nbs_http_proxy(secure_port, insecure_port, nbs_port, nbs_server_insecure, certs_dir):
    working_dir = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder="")

    ensure_path_exists(working_dir)
    config_file = os.path.join(working_dir, 'http_proxy.txt')

    with open(config_file, "w") as f:
        f.write(CONFIG_TEMPLATE.format(
            secure_port=secure_port,
            insecure_port=insecure_port,
            cert_file=os.path.join(certs_dir, "server.crt"),
            private_key_file=os.path.join(certs_dir, "server.key"),
            nbs_port=nbs_port,
            nbs_server_insecure=nbs_server_insecure,
            root_certs_file=os.path.join(certs_dir, "server.crt")))

    return _HttpProxyDaemon(
        config_file,
        working_dir,
        secure_port=secure_port,
        insecure_port=insecure_port)
