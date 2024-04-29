import argparse
import logging
import os

import yatest.common as common
import google.protobuf.text_format as text_format

from library.python.testing.recipe import declare_recipe, set_env

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.storage.core.protos.authorization_mode_pb2 import EAuthorizationMode
from cloud.filestore.tests.python.lib.common import shutdown, get_restart_interval
from cloud.filestore.tests.python.lib.server import NfsServer, wait_for_nfs_server
from cloud.filestore.tests.python.lib.daemon_config import NfsServerConfigGenerator


PID_FILE_NAME = "local_kikimr_nfs_server_recipe.pid"
PDISK_SIZE = 32 * 1024 * 1024 * 1024

logger = logging.getLogger(__name__)


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--kikimr-package-path", action="store", default=None)
    parser.add_argument("--nfs-package-path", action="store", default=None)
    parser.add_argument("--use-log-files", action="store_true", default=False)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--in-memory-pdisks", action="store_true", default=False)
    parser.add_argument("--restart-interval", action="store", default=None)
    parser.add_argument("--storage-config-patch", action="store", default=None)
    args = parser.parse_args(argv)

    kikimr_binary_path = common.binary_path("cloud/storage/core/tools/testing/ydb/bin/ydbd")
    if args.kikimr_package_path is not None:
        kikimr_binary_path = common.build_path("{}/Berkanavt/kikimr/bin/kikimr".format(args.kikimr_package_path))

    kikimr_configurator = KikimrConfigGenerator(
        erasure=None,
        use_in_memory_pdisks=args.in_memory_pdisks,
        binary_path=kikimr_binary_path,
        has_cluster_uuid=False,
        static_pdisk_size=PDISK_SIZE,
        use_log_files=args.use_log_files,
    )

    kikimr_cluster = kikimr_cluster_factory(configurator=kikimr_configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    set_env("KIKIMR_ROOT", kikimr_configurator.domain_name)
    set_env("KIKIMR_SERVER_PORT", str(kikimr_port))

    nfs_binary_path = common.binary_path("cloud/filestore/apps/server/filestore-server")

    if args.nfs_package_path is not None:
        nfs_binary_path = common.build_path("{}/usr/bin/filestore-server".format(args.nfs_package_path))

    access_service_port = int(os.getenv("ACCESS_SERVICE_PORT") or 0)

    server_config = TServerAppConfig()
    server_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    storage_config = TStorageConfig()
    if args.storage_config_patch:
        with open(common.source_path(args.storage_config_patch)) as p:
            storage_config = text_format.Parse(
                p.read(),
                TStorageConfig())
    if access_service_port:
        server_config.ServerConfig.RootCertsFile = common.source_path("cloud/filestore/tests/certs/server.crt")

        server_config.ServerConfig.Certs.add()
        server_config.ServerConfig.Certs[0].CertFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.ServerConfig.Certs[0].CertPrivateKeyFile = common.source_path(
            "cloud/filestore/tests/certs/server.key"
        )

        storage_config.AuthorizationMode = EAuthorizationMode.Value("AUTHORIZATION_REQUIRE")
        storage_config.FolderId = "test_folder"

    domain = kikimr_configurator.domains_txt.Domain[0].Name

    nfs_configurator = NfsServerConfigGenerator(
        binary_path=nfs_binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=args.verbose,
        kikimr_port=kikimr_port,
        domain=domain,
        restart_interval=get_restart_interval(args.restart_interval),
        access_service_port=access_service_port,
        storage_config=storage_config,
    )
    nfs_configurator.generate_configs(kikimr_configurator.domains_txt, kikimr_configurator.names_txt)

    nfs_server = NfsServer(configurator=nfs_configurator)
    nfs_server.start()

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(nfs_server.pid))

    wait_for_nfs_server(nfs_server, nfs_configurator.port)

    set_env("NFS_SERVER_PORT", str(nfs_configurator.port))
    set_env("NFS_MON_PORT", str(nfs_configurator.mon_port))
    set_env("NFS_DOMAIN", str(domain))
    set_env("NFS_CONFIG_DIR", str(nfs_configurator.configs_dir))
    if access_service_port:
        set_env("NFS_SERVER_SECURE_PORT", str(server_config.ServerConfig.SecurePort))


def stop(argv):
    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        shutdown(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
