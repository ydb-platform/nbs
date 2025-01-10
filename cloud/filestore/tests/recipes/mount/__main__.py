import argparse
import logging
import os

import yatest.common as common

from library.python.testing.recipe import declare_recipe, set_env

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.common import shutdown


PID_FILE_NAME = "local_mount_nfs_share_recipe.pid"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--filesystem", action="store", default="nfs_share")
    parser.add_argument("--mount-path", action="append", default=[])
    parser.add_argument("--verbose", action="store_true", default=False)
    args = parser.parse_args(argv)

    port = os.getenv("NFS_SERVER_PORT")

    client_path = common.binary_path(
        "cloud/filestore/apps/client/filestore-client")

    client = FilestoreCliClient(
        binary_path=client_path,
        port=port,
        verbose=args.verbose,
        cwd=common.output_path())

    client.create(
        args.filesystem,
        "test_cloud",
        "test_folder",
        verbose=True)

    if len(args.mount_path) == 0:
        args.mount_path = ["nfs_mount"]

    paths = []
    pids = []
    for path in args.mount_path:
        path = common.work_path(path)
        if not os.path.exists(path):
            os.mkdir(path)

        pids.append(client.mount(args.filesystem, path))

        paths.append(path)

    with open(PID_FILE_NAME, 'w') as f:
        f.write(",".join([str(pid) for pid in pids]))

    set_env("NFS_MOUNT_PATH", ",".join(paths))


def stop(argv):
    # TODO(#2831): remove this debug information
    logging.info(os.system("ps aux"))

    if not os.path.exists(PID_FILE_NAME):
        return

    with open(PID_FILE_NAME) as f:
        pids = [int(x) for x in f.read().split(",")]
        for pid in pids:
            shutdown(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
