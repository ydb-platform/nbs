import argparse
import logging
import os
import subprocess

import yatest.common as common

from library.python.testing.recipe import declare_recipe, set_env

from cloud.filestore.config.server_pb2 import \
    TServerAppConfig, TLocalServiceConfig
from cloud.filestore.tests.python.lib.common import shutdown
from cloud.filestore.tests.python.lib.server import FilestoreServer, wait_for_filestore_server
from cloud.filestore.tests.python.lib.daemon_config import FilestoreServerConfigGenerator
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

PID_FILE_NAME = "local_service_nfs_server_recipe.pid"
ERR_LOG_FILE_NAMES_FILE = "local_service_nfs_server_recipe.err_log_files"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--nfs-package-path", action="store", default=None)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--service", action="store", default="null")
    args = parser.parse_args(argv)

    filestore_binary_path = common.binary_path(
        "cloud/filestore/apps/server/filestore-server")

    if args.nfs_package_path is not None:
        filestore_binary_path = common.build_path(
            "{}/usr/bin/filestore-server".format(args.nfs_package_path)
        )

    server_config = TServerAppConfig()
    server_config.LocalServiceConfig.CopyFrom(TLocalServiceConfig())

    fs_root_path = common.ram_drive_path()
    if fs_root_path:
        server_config.LocalServiceConfig.RootPath = fs_root_path

    filestore_configurator = FilestoreServerConfigGenerator(
        binary_path=filestore_binary_path,
        app_config=server_config,
        service_type=args.service,
        verbose=args.verbose)

    filestore_server = FilestoreServer(configurator=filestore_configurator)
    filestore_server.start()

    with open(PID_FILE_NAME, "w") as f:
        f.write(str(filestore_server.pid))

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, filestore_server.stderr_file_name)

    wait_for_filestore_server(filestore_server, filestore_configurator.port)

    set_env("NFS_SERVER_PORT", str(filestore_configurator.port))
    set_env("NFS_MON_PORT", str(filestore_configurator.mon_port))


def stop(argv):
    logging.info(os.system("ss -tpna"))
    logging.info(os.system("ps aux"))

    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pid = int(f.read())

        # Used for debugging filestore-server hangs
        bt = subprocess.getoutput(
            f'sudo gdb --batch -p {pid} -ex "thread apply all bt"'
        )
        logging.warning(f"PID {pid}: backtrace:\n{bt}")

        shutdown(pid)

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError("Errors found in recipe error files:\n" + "\n".join(errors))


if __name__ == "__main__":
    declare_recipe(start, stop)
