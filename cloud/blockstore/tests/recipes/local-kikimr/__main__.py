import argparse
import os
import signal

from library.python.testing.recipe import declare_recipe, set_env

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.discovery_pb2 import TDiscoveryServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server

from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)
from cloud.storage.core.tests.ydb import Ydb

import yatest.common as yatest_common


PID_FILE_NAME = "local_kikimr_nbs_server_recipe.pid"
ERR_LOG_FILE_NAMES_FILE = "local_kikimr_nbs_server_recipe.err_log_files"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--nbs-package-path", action='store', default=None)
    parser.add_argument("--use-ic-version-check", action='store_true', default=False)
    args = parser.parse_args(argv)

    nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")
    if args.nbs_package_path is not None:
        nbs_binary_path = yatest_common.build_path(
            "{}/usr/bin/blockstore-server".format(args.nbs_package_path)
        )

    ydb = Ydb.load(os.environ["YDB_FILE"])

    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.ServerConfig.NbdEnabled = True
    server_app_config.ServerConfig.VhostEnabled = True
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')
    set_env("TEST_CERT_FILES_DIR", certs_dir)

    server_app_config.ServerConfig.RootCertsFile = os.path.join(certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

    pm = yatest_common.network.PortManager()

    nbs_port = pm.get_port()
    nbs_secure_port = pm.get_port()

    instance_list_file = os.path.join(yatest_common.output_path(), "static_instances.txt")
    with open(instance_list_file, "w") as f:
        print("localhost\t%s\t%s" % (nbs_port, nbs_secure_port), file=f)

    discovery_config = TDiscoveryServiceConfig()
    discovery_config.InstanceListFile = instance_list_file

    nbs = LocalNbs(
        ydb.port,
        ydb.domains_txt,
        server_app_config=server_app_config,
        enable_tls=True,
        load_configs_from_cms=True,
        discovery_config=discovery_config,
        dynamic_storage_pools=ydb.dynamic_storage_pools,
        nbs_secure_port=nbs_secure_port,
        nbs_port=nbs_port,
        kikimr_binary_path=ydb.binary_path,
        nbs_binary_path=nbs_binary_path,
        use_ic_version_check=args.use_ic_version_check)

    nbs.setup_cms(ydb.client)

    nbs.start()

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, nbs.stderr_file_name)

    set_env("LOCAL_KIKIMR_SECURE_NBS_SERVER_PORT", str(nbs.nbs_secure_port))
    set_env("LOCAL_KIKIMR_INSECURE_NBS_SERVER_PORT", str(nbs.nbs_port))

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(nbs.pid))

    wait_for_nbs_server(nbs.nbs_port)


def stop(argv):
    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        os.kill(pid, signal.SIGTERM)
    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError("Errors during recipe execution:\n" + "\n".join(errors))


if __name__ == "__main__":
    declare_recipe(start, stop)
