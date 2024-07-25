import argparse
import os
import pathlib
import uuid

import yatest.common as common
import google.protobuf.text_format as text_format

from library.python.testing.recipe import declare_recipe, set_env

from cloud.filestore.config.server_pb2 import TLocalServiceConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.config.vhost_pb2 import \
    TVhostAppConfig, TVhostServiceConfig, TServiceEndpoint

from cloud.filestore.tests.python.lib.common import \
    shutdown, get_restart_interval, get_restart_flag

from cloud.filestore.tests.python.lib.daemon_config import NfsVhostConfigGenerator
from cloud.filestore.tests.python.lib.vhost import NfsVhost, wait_for_nfs_vhost
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType


PID_FILE_NAME = "nfs_vhost_recipe.pid"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--vhost-package-path", action="store", default=None)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--service", action="store", default=None)
    parser.add_argument("--restart-interval", action="store", default=None)
    parser.add_argument("--restart-flag", action="store", default=None)
    parser.add_argument("--storage-config-patch", action="store", default=None)
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

    endpoint_storage_dir = common.work_path() + '/endpoints-' + uid
    pathlib.Path(endpoint_storage_dir).mkdir(parents=True, exist_ok=True)

    config = TVhostAppConfig()
    config.VhostServiceConfig.CopyFrom(TVhostServiceConfig())
    config.VhostServiceConfig.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    config.VhostServiceConfig.EndpointStorageDir = endpoint_storage_dir

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
    elif service_type == "kikimr":
        kikimr_port = os.getenv("KIKIMR_SERVER_PORT")
        domain = os.getenv("NFS_DOMAIN")

    config.VhostServiceConfig.ServiceEndpoints.append(service_endpoint)

    storage_config = TStorageConfig()
    if args.storage_config_patch:
        with open(common.source_path(args.storage_config_patch)) as p:
            storage_config = text_format.Parse(
                p.read(),
                TStorageConfig())

    vhost_configurator = NfsVhostConfigGenerator(
        binary_path=vhost_binary_path,
        app_config=config,
        service_type="local" if service_type == "local-noserver" else service_type,
        verbose=args.verbose,
        kikimr_port=kikimr_port,
        domain=domain,
        restart_interval=restart_interval,
        restart_flag=restart_flag,
        storage_config=storage_config,
    )

    nfs_vhost = NfsVhost(vhost_configurator)
    nfs_vhost.start()

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(nfs_vhost.pid))

    wait_for_nfs_vhost(nfs_vhost, vhost_configurator.port)

    if restart_interval:
        set_env("NFS_VHOST_ENDPOINT_STORAGE_DIR", endpoint_storage_dir)
        if restart_flag is not None:
            set_env("QEMU_SET_READY_FLAG", restart_flag)

    set_env("NFS_VHOST_PORT", str(vhost_configurator.port))
    if service_type == "local-noserver":
        set_env("NFS_SERVER_PORT", str(vhost_configurator.local_service_port))


def stop(argv):
    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pid = int(f.read())
        shutdown(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
