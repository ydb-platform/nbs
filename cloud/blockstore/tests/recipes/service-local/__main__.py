import os
import signal
import tempfile

from library.python.testing.recipe import declare_recipe, set_env

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TLocalServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, get_free_socket_path

import yatest.common as yatest_common


PID_FILE_NAME = "service_local_nbs_server_recipe.pid"


def start(argv):
    server = TServerAppConfig()
    server.LocalServiceConfig.CopyFrom(TLocalServiceConfig())
    server.LocalServiceConfig.DataDir = tempfile.gettempdir()

    nbs_socket_path = get_free_socket_path("nbs_socket")

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')
    set_env("TEST_CERT_FILES_DIR", certs_dir)

    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.RootCertsFile = os.path.join(certs_dir, 'server.crt')
    cert = server.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')
    server.ServerConfig.UnixSocketPath = nbs_socket_path
    server.ServerConfig.NbdEnabled = True
    server.ServerConfig.NbdSocketSuffix = "_nbd"
    server.ServerConfig.VhostEnabled = True

    config_sub_folder = "nbs_configs_secure"

    nbs_binary_path = yatest_common.binary_path(
        "cloud/blockstore/apps/server_lightweight/nbsd-lightweight")

    nbs = LocalNbs(
        0,
        server_app_config=server,
        config_sub_folder=config_sub_folder,
        enable_tls=True,
        nbs_binary_path=nbs_binary_path)
    nbs.start()

    set_env("SERVICE_LOCAL_SECURE_NBS_SERVER_PORT", str(nbs.nbs_secure_port))
    set_env("SERVICE_LOCAL_INSECURE_NBS_SERVER_PORT", str(nbs.nbs_port))
    set_env("SERVICE_LOCAL_MONITORING_PORT", str(nbs.mon_port))
    set_env("SERVICE_LOCAL_NBS_SOCKET_PATH", nbs_socket_path)

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(nbs.pid))

    wait_for_nbs_server(nbs.nbs_port)


def stop(argv):
    def kill(pid_file_name):
        with open(pid_file_name) as f:
            pid = int(f.read())
            os.kill(pid, signal.SIGTERM)

    kill(PID_FILE_NAME)


if __name__ == "__main__":
    declare_recipe(start, stop)
