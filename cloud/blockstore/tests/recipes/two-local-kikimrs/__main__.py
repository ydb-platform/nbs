import argparse
import os
import signal

from library.python.testing.recipe import declare_recipe, set_env

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.discovery_pb2 import TDiscoveryServiceConfig

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

import yatest.common as yatest_common


PID_FILE_NAME = "two_local_kikimrs_nbs_server_recipe.pid"
ERR_LOG_FILE_NAMES_FILE = "two_local_kikimr_nbs_server_recipe.err_log_files"

def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--kikimr-package-path", action='store', default=None)
    parser.add_argument("--nbs-package-path", action='store', default=None)
    parser.add_argument("--use-log-files", action='store_true', default=False)
    parser.add_argument("--use-ic-version-check", action='store_true', default=False)
    parser.add_argument("--nbs-instances-count", action='store', default=2)
    args = parser.parse_args(argv)

    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")
    if args.kikimr_package_path is not None:
        kikimr_binary_path = yatest_common.build_path(
            "{}/ydbd".format(args.kikimr_package_path)
        )

    nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")
    if args.nbs_package_path is not None:
        nbs_binary_path = yatest_common.build_path(
            "{}/usr/bin/blockstore-server".format(args.nbs_package_path)
        )

    pm = yatest_common.network.PortManager()
    clusters = []
    nbs_servers = []

    nbs_instances_count = args.nbs_instances_count
    set_env("CLUSTERS_COUNT", str(nbs_instances_count))

    for i in range(nbs_instances_count):
        configurator = KikimrConfigGenerator(
            erasure=None,
            binary_path=kikimr_binary_path,
            use_in_memory_pdisks=True,
            dynamic_storage_pools=[
                dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
                dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
            ],
            use_log_files=args.use_log_files)

        kikimr_cluster = kikimr_cluster_factory(configurator=configurator, sub_folder_name='kikimr_configs_{}'.format(i))
        kikimr_cluster.start()
        clusters.append((configurator, kikimr_cluster))

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

        nbs_port = pm.get_port()
        nbs_secure_port = pm.get_port()

        instance_list_file = os.path.join(yatest_common.output_path(), "static_instances_{}.txt".format(i))
        with open(instance_list_file, "w") as f:
            print("localhost\t%s\t%s" % (nbs_port, nbs_secure_port), file=f)

        discovery_config = TDiscoveryServiceConfig()
        discovery_config.InstanceListFile = instance_list_file

        kikimr_port = list(kikimr_cluster.nodes.values())[0].port

        nbs = LocalNbs(
            kikimr_port,
            configurator.domains_txt,
            server_app_config=server_app_config,
            enable_tls=True,
            load_configs_from_cms=True,
            discovery_config=discovery_config,
            nbs_secure_port=nbs_secure_port,
            nbs_port=nbs_port,
            kikimr_binary_path=kikimr_binary_path,
            nbs_binary_path=nbs_binary_path,
            use_ic_version_check=args.use_ic_version_check,
            config_sub_folder="nbs_configs_{}".format(i)
            )

        nbs.setup_cms(kikimr_cluster.client)
        nbs.start()
        nbs_servers.append(nbs)

        append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, nbs.stderr_file_name)

        set_env("LOCAL_KIKIMR_KIKIMR_ROOT_{}".format(i), configurator.domain_name)
        set_env("LOCAL_KIKIMR_KIKIMR_SERVER_PORT_{}".format(i), str(kikimr_port))
        set_env("LOCAL_KIKIMR_SECURE_NBS_SERVER_PORT_{}".format(i), str(nbs.nbs_secure_port))
        set_env("LOCAL_KIKIMR_INSECURE_NBS_SERVER_PORT_{}".format(i), str(nbs.nbs_port))

    with open(PID_FILE_NAME, "w") as f:
        for nbs in nbs_servers:
            f.write(str(nbs.pid) + "\n")

    for nbs in nbs_servers:
        wait_for_nbs_server(nbs.nbs_port)


def stop(argv):
    with open(PID_FILE_NAME) as f:
        for line in f:
            pid = int(line.strip())
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError("Errors during recipe execution:\n" + "\n".join(errors))


if __name__ == "__main__":
    declare_recipe(start, stop)
