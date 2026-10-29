import argparse
import logging
import os
import time

import yatest.common as common

from library.python.testing.recipe import declare_recipe, set_env

from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.common import shutdown

from cloud.storage.core.tools.common.python.daemon import extract_log_tail

logger = logging.getLogger(__name__)

PID_FILE_NAME = "local_mount_nfs_share_recipe.pid"
MOUNT_PATH_FILE_NAME = "local_mount_nfs_share_recipe.mount_path"


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--filesystem", action="store", default="nfs_share")
    parser.add_argument("--mount-path", action="append", default=[])
    parser.add_argument("--verbose", action="store_true", default=False)
    return parser.parse_args(argv)


def start(argv):
    args = parse_args(argv)

    for state_file in (PID_FILE_NAME, MOUNT_PATH_FILE_NAME):
        try:
            os.unlink(state_file)
        except FileNotFoundError:
            pass

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
    with open(PID_FILE_NAME, 'w') as pid_file, \
            open(MOUNT_PATH_FILE_NAME, 'w') as path_file:
        for path in args.mount_path:
            path = common.work_path(path)
            if not os.path.exists(path):
                os.mkdir(path)

            pid = client.mount(args.filesystem, path)
            pid_file.write("{}\n".format(pid))
            pid_file.flush()
            paths.append(path)
            path_file.write("{}\n".format(path))
            path_file.flush()

    set_env("NFS_MOUNT_PATH", ",".join(paths))


def stop(argv):
    args = parse_args(argv)
    logger.info(os.system("ps aux"))
    logger.info(os.system("mount"))

    listings = []
    try:
        if os.path.exists(MOUNT_PATH_FILE_NAME):
            with open(MOUNT_PATH_FILE_NAME) as f:
                paths = [path.strip() for path in f if path.strip()]
            inspect_mounts(args.filesystem, paths, listings)
    finally:
        # SIGKILL may remain pending on a blocked FUSE request. Do not wait.
        for listing, log_path in listings:
            if listing.process.poll() is None:
                try:
                    listing.process.kill()
                except OSError:
                    logger.exception("failed to kill mount listing; output: %s", log_path)
        try:
            stop_clients()
        finally:
            collect_listings(listings)


def inspect_mounts(filesystem, paths, listings):
    # Start all listings before waiting, while the clients are still running.
    deadline = time.monotonic() + 5
    for index, path in enumerate(paths):
        log_path = common.output_path("mount-ls-{}.log".format(index))
        try:
            with open(log_path, "wb") as output:
                output.write("FileSystemId: {}\nMount path: {}\n\n".format(
                    filesystem, os.path.abspath(path)).encode("utf-8"))
                output.flush()
                logger.info("Mount path: %s; listing: %s", path, log_path)
                listing = common.execute(
                    ["ls", "-la", path], stdout=output, stderr=output,
                    wait=False, check_exit_code=False)
                listings.append((listing, log_path))
        except Exception:
            logger.exception("failed to start listing for %s", path)
    while any(listing.process.poll() is None for listing, _ in listings):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.1, remaining))


def collect_listings(listings):
    for listing, log_path in listings:
        exit_code = listing.process.poll()
        if exit_code is None:
            logger.warning("Mount listing pid %s is still running after SIGKILL and client shutdown; output: %s",
                           listing.process.pid, log_path)
        else:
            logger.info("Mount listing exit code: %s; output: %s", exit_code, log_path)
        try:
            tail = extract_log_tail(log_path, max_lines=100, label="Mount listing")
            logger.info("Mount listing (%s):\n%s", log_path,
                        "\n".join(tail))
        except OSError:
            logger.exception("could not read mount listing output: %s", log_path)


def stop_clients():
    if os.path.exists(PID_FILE_NAME):
        with open(PID_FILE_NAME) as f:
            for pid in f:
                shutdown(int(pid.strip()))


if __name__ == "__main__":
    declare_recipe(start, stop)
