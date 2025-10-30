import argparse
import logging
import os
import pathlib
import tempfile

import yatest.common as common
import google.protobuf.text_format as text_format

from library.python.testing.recipe import declare_recipe, set_env

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.storage.core.protos.authorization_mode_pb2 import EAuthorizationMode
from cloud.filestore.tests.python.lib.common import shutdown, get_restart_interval
from cloud.filestore.tests.python.lib.server import FilestoreServer, wait_for_filestore_server
from cloud.filestore.tests.python.lib.daemon_config import FilestoreServerConfigGenerator
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from cloud.storage.core.tools.testing.access_service_new.lib import NewAccessService
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

PID_FILE_NAME = "local_kikimr_nfs_server_recipe.pid"
KIKIMR_PID_FILE_NAME = "local_kikimr_nfs_server_recipe.kikimr_pid"
ERR_LOG_FILE_NAMES_FILE = "local_kikimr_nfs_server_recipe.err_log_files"
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
    parser.add_argument("--bs-cache-file-path", action="store", default=None)
    parser.add_argument("--use-unix-socket", action="store_true", default=False)
    args = parser.parse_args(argv)

    kikimr_binary_path = common.binary_path("cloud/storage/core/tools/testing/ydb/bin/ydbd")
    if args.kikimr_package_path is not None:
        kikimr_binary_path = common.build_path("{}/ydbd".format(args.kikimr_package_path))

    kikimr_configurator = KikimrConfigGenerator(
        erasure=None,
        use_in_memory_pdisks=args.in_memory_pdisks,
        binary_path=kikimr_binary_path,
        static_pdisk_size=PDISK_SIZE,
        use_log_files=args.use_log_files,
        bs_cache_file_path=args.bs_cache_file_path,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0),
        ],
    )

    kikimr_cluster = kikimr_cluster_factory(configurator=kikimr_configurator)
    kikimr_cluster.start()

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    set_env("KIKIMR_ROOT", kikimr_configurator.domain_name)
    set_env("KIKIMR_SERVER_PORT", str(kikimr_port))

    filestore_binary_path = common.binary_path("cloud/filestore/apps/server/filestore-server")

    if args.nfs_package_path is not None:
        filestore_binary_path = common.build_path("{}/usr/bin/filestore-server".format(args.nfs_package_path))

    access_service_port = int(os.getenv("ACCESS_SERVICE_PORT") or 0)

    server_config = TServerAppConfig()
    server_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    storage_config = TStorageConfig()
    if args.storage_config_patch:
        with open(common.source_path(args.storage_config_patch)) as p:
            storage_config = text_format.Parse(
                p.read(),
                TStorageConfig())
    if args.use_unix_socket:
        # Create in temp directory because we would like a shorter path
        server_unix_socket_path = str(
            pathlib.Path(tempfile.mkdtemp(dir="/tmp")) / "filestore.sock")
        set_env("NFS_SERVER_UNIX_SOCKET_PATH", server_unix_socket_path)
        server_config.ServerConfig.UnixSocketPath = server_unix_socket_path

    secure = False
    if access_service_port:
        secure = True

        server_config.ServerConfig.RootCertsFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.ServerConfig.Certs.add()
        server_config.ServerConfig.Certs[0].CertFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.ServerConfig.Certs[0].CertPrivateKeyFile = common.source_path(
            "cloud/filestore/tests/certs/server.key"
        )

        storage_config.AuthorizationMode = EAuthorizationMode.Value("AUTHORIZATION_REQUIRE")
        folder_id = "test_folder"
        storage_config.FolderId = folder_id
        set_env("TEST_FOLDER_ID", folder_id)

    domain = kikimr_configurator.domains_txt.Domain[0].Name

    access_service_type = AccessService
    if os.getenv("ACCESS_SERVICE_TYPE") == "new":
        access_service_type = NewAccessService
    restart_interval = get_restart_interval(args.restart_interval)
    filestore_configurator = FilestoreServerConfigGenerator(
        binary_path=filestore_binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=args.verbose,
        kikimr_port=kikimr_port,
        domain=domain,
        restart_interval=restart_interval,
        access_service_port=access_service_port,
        storage_config=storage_config,
        secure=secure,
        access_service_type=access_service_type,
    )
    filestore_configurator.generate_configs(kikimr_configurator.domains_txt, kikimr_configurator.names_txt)

    filestore_server = FilestoreServer(
        configurator=filestore_configurator,
        kikimr_binary_path=kikimr_binary_path,
        dynamic_storage_pools=kikimr_configurator.dynamic_storage_pools,
    )
    filestore_server.start()

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, filestore_server.stderr_file_name)

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(filestore_server.pid))

    with open(KIKIMR_PID_FILE_NAME, "w") as f:
        f.write(str(list(kikimr_cluster.nodes.values())[0].pid))

    wait_for_filestore_server(filestore_server, filestore_configurator.port)

    set_env("NFS_SERVER_PORT", str(filestore_configurator.port))
    set_env("NFS_MON_PORT", str(filestore_configurator.mon_port))
    set_env("NFS_DOMAIN", str(domain))
    set_env("NFS_CONFIG_DIR", str(filestore_configurator.configs_dir))
    set_env("NFS_RESTART_INTERVAL", str(restart_interval))
    if secure:
        set_env("NFS_SERVER_SECURE_PORT", str(filestore_configurator.secure_port))


def stop(argv):
    logging.info(os.system("ss -tpna"))
    logging.info(os.system("ps aux"))

    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        shutdown(pid)

    with open(KIKIMR_PID_FILE_NAME) as f:
        pid = int(f.read())
        shutdown(pid)

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError("Recipe found errors in the log files: " + "\n".join(errors))


if __name__ == "__main__":
    declare_recipe(start, stop)
