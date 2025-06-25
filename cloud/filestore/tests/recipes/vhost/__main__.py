import argparse
import logging
import os
import pathlib
import tempfile
import uuid

import yatest.common as common
import google.protobuf.text_format as text_format

from library.python.testing.recipe import declare_recipe, set_env

from cloud.filestore.config.server_pb2 import TLocalServiceConfig, TServerConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.config.vhost_pb2 import \
    TVhostAppConfig, TVhostServiceConfig, TServiceEndpoint

from cloud.filestore.tests.python.lib.common import \
    shutdown, get_restart_interval, get_restart_flag

from cloud.filestore.tests.python.lib.daemon_config import FilestoreVhostConfigGenerator
from cloud.storage.core.protos.authorization_mode_pb2 import EAuthorizationMode
from cloud.filestore.tests.python.lib.vhost import FilestoreVhost, wait_for_filestore_vhost
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType

from cloud.storage.core.tools.testing.access_service.lib import AccessService
from cloud.storage.core.tools.testing.access_service_new.lib import NewAccessService


PID_FILE_NAME = "nfs_vhost_recipe.pid"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--vhost-package-path", action="store", default=None)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--service", action="store", default=None)
    parser.add_argument("--restart-interval", action="store", default=None)
    parser.add_argument("--restart-flag", action="store", default=None)
    parser.add_argument("--storage-config-patch", action="store", default=None)
    parser.add_argument("--direct-io", action="store_true", default=False)
    parser.add_argument("--use-unix-socket", action="store_true", default=False)

    args = parser.parse_args(argv)

    vhost_binary_path = common.binary_path(
        "cloud/filestore/apps/vhost/filestore-vhost")

    if args.vhost_package_path is not None:
        vhost_binary_path = common.build_path(
            "{}/usr/bin/filestore-vhost".format(args.vhost_package_path)
        )

    uid = str(uuid.uuid4())

    restart_interval = get_restart_interval(args.restart_interval)
    restart_flag = get_restart_flag(args.restart_flag, "qemu-ready-" + uid)

    endpoint_storage_dir = os.getenv("NFS_VHOST_ENDPOINT_STORAGE_DIR")
    if not endpoint_storage_dir:
        endpoint_storage_dir = common.work_path() + '/endpoints-' + uid
    pathlib.Path(endpoint_storage_dir).mkdir(parents=True, exist_ok=True)
    set_env("NFS_VHOST_ENDPOINT_STORAGE_DIR", endpoint_storage_dir)

    config = TVhostAppConfig()
    config.VhostServiceConfig.CopyFrom(TVhostServiceConfig())
    config.VhostServiceConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    config.VhostServiceConfig.EndpointStorageDir = endpoint_storage_dir

    handle_ops_queue_path = common.work_path() + "/handleopsqueue-" + uid
    pathlib.Path(handle_ops_queue_path).mkdir(parents=True, exist_ok=True)
    config.VhostServiceConfig.HandleOpsQueuePath = handle_ops_queue_path

    write_back_cache_path = common.work_path() + "/writebackcache-" + uid
    pathlib.Path(write_back_cache_path).mkdir(parents=True, exist_ok=True)
    config.VhostServiceConfig.WriteBackCachePath = write_back_cache_path

    service_type = args.service or "local"
    kikimr_port = 0
    domain = None

    service_endpoint = TServiceEndpoint()
    if service_type == "local":
        service_endpoint.ClientConfig.Host = "localhost"
        service_endpoint.ClientConfig.Port = int(os.getenv("NFS_SERVER_PORT"))
    elif service_type == "local-noserver":
        config.VhostServiceConfig.LocalServiceConfig.CopyFrom(TLocalServiceConfig())

        fs_root_path = common.ram_drive_path()
        if fs_root_path:
            config.VhostServiceConfig.LocalServiceConfig.RootPath = fs_root_path
        config.VhostServiceConfig.LocalServiceConfig.DirectIoEnabled = args.direct_io
    elif service_type == "kikimr":
        kikimr_port = os.getenv("KIKIMR_SERVER_PORT")
        domain = os.getenv("NFS_DOMAIN")

    config.VhostServiceConfig.ServiceEndpoints.append(service_endpoint)

    server_config = TServerConfig()

    storage_config = TStorageConfig()
    if args.storage_config_patch:
        with open(common.source_path(args.storage_config_patch)) as p:
            storage_config = text_format.Parse(
                p.read(),
                TStorageConfig())
    if args.use_unix_socket:
        # Create in temp directory because we would like a shorter path
        server_unix_socket_path = str(
            pathlib.Path(tempfile.mkdtemp(dir="/tmp")) / "vhost.sock")
        set_env("NFS_VHOST_UNIX_SOCKET_PATH", server_unix_socket_path)
        server_config.UnixSocketPath = server_unix_socket_path

    access_service_port = int(os.getenv("ACCESS_SERVICE_PORT") or 0)
    secure = False
    if access_service_port:
        secure = True

        server_config.RootCertsFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.Certs.add()
        server_config.Certs[0].CertFile = common.source_path("cloud/filestore/tests/certs/server.crt")
        server_config.Certs[0].CertPrivateKeyFile = common.source_path(
            "cloud/filestore/tests/certs/server.key"
        )

        storage_config.AuthorizationMode = EAuthorizationMode.Value("AUTHORIZATION_REQUIRE")
        folder_id = "test_folder"
        storage_config.FolderId = folder_id
        set_env("TEST_FOLDER_ID", folder_id)

    access_service_type = AccessService
    if os.getenv("ACCESS_SERVICE_TYPE") == "new":
        access_service_type = NewAccessService

    config.ServerConfig.CopyFrom(server_config)

    vhost_configurator = FilestoreVhostConfigGenerator(
        binary_path=vhost_binary_path,
        app_config=config,
        service_type="local" if service_type == "local-noserver" else service_type,
        verbose=args.verbose,
        kikimr_port=kikimr_port,
        domain=domain,
        secure=secure,
        restart_interval=restart_interval,
        restart_flag=restart_flag,
        storage_config=storage_config,
        access_service_type=access_service_type,
    )

    filestore_vhost = FilestoreVhost(vhost_configurator)
    filestore_vhost.start()

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(filestore_vhost.pid))

    wait_for_filestore_vhost(filestore_vhost, vhost_configurator.port)

    if restart_interval:
        if restart_flag is not None:
            set_env("QEMU_SET_READY_FLAG", restart_flag)

    set_env("NFS_VHOST_PORT", str(vhost_configurator.port))
    if service_type == "local-noserver":
        set_env("NFS_SERVER_PORT", str(vhost_configurator.local_service_port))


def stop(argv):
    logging.info(os.system("ss -tpna"))
    logging.info(os.system("ps aux"))

    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        shutdown(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
