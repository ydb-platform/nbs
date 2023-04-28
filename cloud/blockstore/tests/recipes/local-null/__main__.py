import os
import signal

from library.python.testing.recipe import declare_recipe, set_env

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TNullServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.nbs_http_proxy import create_nbs_http_proxy
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, wait_for_nbs_server_proxy, get_free_socket_path

from ydb.core.protos.config_pb2 import TDomainsConfig

import yatest.common as yatest_common


PID_FILE_NAME = "local_null_nbs_server_recipe.pid"
PROXY_PID_FILE_NAME = "local_null_nbs_server_recipe_proxy.pid"


def start(argv):
    server = TServerAppConfig()
    server.NullServiceConfig.CopyFrom(TNullServiceConfig())
    server.NullServiceConfig.AddReadResponseData = True

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

    nbs = LocalNbs(0, TDomainsConfig(), server, config_sub_folder=config_sub_folder, enable_tls=True)
    nbs.start()

    set_env("LOCAL_NULL_SECURE_NBS_SERVER_PORT", str(nbs.nbs_secure_port))
    set_env("LOCAL_NULL_INSECURE_NBS_SERVER_PORT", str(nbs.nbs_port))
    set_env("LOCAL_NULL_MONITORING_PORT", str(nbs.mon_port))
    set_env("LOCAL_NULL_NBS_SOCKET_PATH", nbs_socket_path)

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(nbs.pid))

    wait_for_nbs_server(nbs.nbs_port)

    if 'with-http-proxy' in argv:
        port_manager = yatest_common.network.PortManager()
        proxy_secure_port = port_manager.get_port()
        proxy_insecure_port = port_manager.get_port()
        proxy = create_nbs_http_proxy(proxy_secure_port, proxy_insecure_port, nbs.nbs_secure_port, False, certs_dir)
        proxy.start()

        set_env("LOCAL_NULL_SECURE_NBS_SERVER_PROXY_PORT", str(proxy_secure_port))
        set_env("LOCAL_NULL_INSECURE_NBS_SERVER_PROXY_PORT", str(proxy_insecure_port))

        with open(PROXY_PID_FILE_NAME, "w") as f:
            f.write(str(proxy.pid))

        wait_for_nbs_server_proxy(proxy_secure_port)


def stop(argv):
    def kill(pid_file_name):
        with open(pid_file_name) as f:
            pid = int(f.read())
            os.kill(pid, signal.SIGTERM)

    kill(PID_FILE_NAME)

    if os.path.exists(PROXY_PID_FILE_NAME):
        kill(PROXY_PID_FILE_NAME)


if __name__ == "__main__":
    declare_recipe(start, stop)
