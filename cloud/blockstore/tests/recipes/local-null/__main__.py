import os
import signal

from library.python.testing.recipe import declare_recipe, set_env

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TNullServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, \
    wait_for_nbs_server, get_free_socket_path
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

import yatest.common as yatest_common


PID_FILE_NAME = "local_null_nbs_server_recipe.pid"
ERR_LOG_FILE_NAMES_FILE = "local_null_nbs_server_recipe.err_log_files"

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

    nbs_binary_path = yatest_common.binary_path(
        "cloud/blockstore/apps/server_lightweight/nbsd-lightweight")

    nbs = LocalNbs(
        0,
        server_app_config=server,
        config_sub_folder=config_sub_folder,
        enable_tls=True,
        nbs_binary_path=nbs_binary_path)
    nbs.start()

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, nbs.stderr_file_name)

    set_env("LOCAL_NULL_SECURE_NBS_SERVER_PORT", str(nbs.nbs_secure_port))
    set_env("LOCAL_NULL_INSECURE_NBS_SERVER_PORT", str(nbs.nbs_port))
    set_env("LOCAL_NULL_MONITORING_PORT", str(nbs.mon_port))
    set_env("LOCAL_NULL_NBS_SOCKET_PATH", nbs_socket_path)

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(nbs.pid))

    wait_for_nbs_server(nbs.nbs_port)


def stop(argv):
    def kill(pid_file_name):
        with open(pid_file_name) as f:
            pid = int(f.read())
            os.kill(pid, signal.SIGTERM)

    kill(PID_FILE_NAME)

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError("Errors found in recipe log files: {}".format(errors))


if __name__ == "__main__":
    declare_recipe(start, stop)
