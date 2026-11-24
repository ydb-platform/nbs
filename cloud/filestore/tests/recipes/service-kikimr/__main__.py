import argparse
import logging
import os
import pathlib
import tempfile

import yatest.common as common
import google.protobuf.text_format as text_format

from library.python.testing.recipe import declare_recipe, set_env

from cloud.filestore.config.server_pb2 import TServerAppConfig, TKikimrServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.storage.core.protos.authorization_mode_pb2 import EAuthorizationMode
from cloud.filestore.tests.python.lib.common import shutdown, get_restart_interval
from cloud.filestore.tests.python.lib.server import FilestoreServer, wait_for_filestore_server
from cloud.filestore.tests.python.lib.daemon_config import FilestoreServerConfigGenerator
from cloud.storage.core.tools.testing.access_service.lib import AccessService
from cloud.storage.core.tools.testing.access_service_new.lib import NewAccessService
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    expand_placeholders,
    process_recipe_err_files,
)
from cloud.storage.core.tests.ydb import Ydb

logger = logging.getLogger(__name__)

PID_FILE_NAME = "local_kikimr_nfs_server_recipe.pid"
ERR_LOG_FILE_NAMES_FILE = "local_kikimr_nfs_server_recipe.err_log_files"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--nfs-package-path", action="store", default=None)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--enable-external-data-channel", action="store_true", default=False)
    parser.add_argument("--restart-interval", action="store", default=None)
    parser.add_argument("--storage-config-patch", nargs="+", default=[])
    parser.add_argument("--diag-config-patch", action="store", default=None)
    parser.add_argument("--server-config-patch", action="store", default=None)
    parser.add_argument("--use-unix-socket", action="store_true", default=False)
    parser.add_argument("--trace-sampling-rate", action="store", default=None, type=int)
    parser.add_argument("--use-fast-shard-port", action="store_true", default=False)
    parser.add_argument("--bs-failure-probability", action="store", default=None, type=float)
    args = parser.parse_args(argv)

    ydb = Ydb.load(os.environ["YDB_FILE"])

    filestore_binary_path = common.binary_path("cloud/filestore/apps/server/filestore-server")

    if args.nfs_package_path is not None:
        filestore_binary_path = common.build_path("{}/usr/bin/filestore-server".format(args.nfs_package_path))

    access_service_port = int(os.getenv("ACCESS_SERVICE_PORT") or 0)

    server_config = TServerAppConfig()
    server_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    storage_config = TStorageConfig()
    diag_config = TDiagnosticsConfig()
    for patch in args.storage_config_patch:
        with open(common.source_path(patch)) as p:
            text_format.Merge(p.read(), storage_config)
    if args.diag_config_patch:
        with open(common.source_path(args.diag_config_patch)) as p:
            diag_config = text_format.Parse(
                p.read(),
                TDiagnosticsConfig())
    if args.use_unix_socket:
        # Create in temp directory because we would like a shorter path
        server_unix_socket_path = str(
            pathlib.Path(tempfile.mkdtemp(dir="/tmp")) / "filestore.sock")
        set_env("NFS_SERVER_UNIX_SOCKET_PATH", server_unix_socket_path)
        server_config.ServerConfig.UnixSocketPath = server_unix_socket_path
    if args.server_config_patch:
        with open(common.source_path(args.server_config_patch)) as p:
            server_config_patch = text_format.Parse(
                p.read(),
                TServerAppConfig())
            server_config.MergeFrom(server_config_patch)

    if server_config.ServerConfig.SharedMemoryBasePath != "":
        shared_memory_base_path = expand_placeholders(
            server_config.ServerConfig.SharedMemoryBasePath
        )
        os.makedirs(shared_memory_base_path, exist_ok=True)
        server_config.ServerConfig.SharedMemoryBasePath = shared_memory_base_path

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

    domain = ydb.domains_txt.Domain[0].Name

    access_service_type = AccessService
    if os.getenv("ACCESS_SERVICE_TYPE") == "new":
        access_service_type = NewAccessService
    restart_interval = get_restart_interval(args.restart_interval)
    filestore_configurator = FilestoreServerConfigGenerator(
        binary_path=filestore_binary_path,
        app_config=server_config,
        service_type="kikimr",
        verbose=args.verbose,
        kikimr_port=ydb.port,
        domain=domain,
        restart_interval=restart_interval,
        access_service_port=access_service_port,
        storage_config=storage_config,
        diag_config=diag_config,
        secure=secure,
        access_service_type=access_service_type,
        trace_sampling_rate=args.trace_sampling_rate,
        bs_failure_probability=args.bs_failure_probability,
        use_fast_shard_port=args.use_fast_shard_port,
        enable_external_data_channel=args.enable_external_data_channel,
    )
    filestore_configurator.generate_configs(ydb.domains_txt, ydb.names_txt)

    filestore_server = FilestoreServer(
        configurator=filestore_configurator,
        kikimr_binary_path=ydb.binary_path,
        dynamic_storage_pools=ydb.dynamic_storage_pools,
    )
    filestore_server.start()

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(filestore_server.pid))

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, filestore_server.stderr_file_name)

    wait_for_filestore_server(filestore_server, filestore_configurator.port)

    set_env("NFS_SERVER_PORT", str(filestore_configurator.port))
    set_env("NFS_MON_PORT", str(filestore_configurator.mon_port))
    set_env("NFS_DOMAIN", str(domain))
    if args.use_fast_shard_port:
        set_env(
            "NFS_FAST_SHARD_PORT",
            str(filestore_configurator.fast_shard_port))
    set_env("NFS_CONFIG_DIR", str(filestore_configurator.configs_dir))
    set_env("NFS_RESTART_INTERVAL", str(restart_interval))
    if secure:
        set_env("NFS_SERVER_SECURE_PORT", str(filestore_configurator.secure_port))


def stop(argv):
    logger.info(os.system("ss -tpna"))
    logger.info(os.system("ps aux"))

    if os.path.exists(PID_FILE_NAME):
        with open(PID_FILE_NAME) as f:
            pid = int(f.read())
            shutdown(pid)

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError("Recipe found errors in the log files: " + "\n".join(errors))


if __name__ == "__main__":
    declare_recipe(start, stop)
