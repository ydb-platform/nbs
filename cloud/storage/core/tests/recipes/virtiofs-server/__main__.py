import os
import logging
import signal
import uuid
import argparse

import yatest.common as common

from cloud.storage.core.tools.testing.virtiofs_server.lib import VirtioFsServer
from cloud.storage.core.tools.testing.qemu.lib.recipe import recipe_set_env, recipe_get_env
from library.python.testing.recipe import declare_recipe
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

logger = logging.getLogger(__name__)
ERR_LOG_FILE_NAMES_FILE = "virtiofs_server_recipe.err_log_files"


def _get_mount_paths():
    if 'ASAN_SYMBOLIZER_PATH' in os.environ:
        toolchain = os.path.dirname(os.path.dirname(
            os.environ['ASAN_SYMBOLIZER_PATH']))

    mounts = [("source_path", common.source_path()),  # need to mound original source root as test environment has links into it
              ("build_path", common.build_path()),
              ("toolchain", toolchain)]

    if "TEST_TOOL" in os.environ:
        test_tool_dir = os.path.dirname(os.environ["TEST_TOOL"])
        mounts.append(tuple(("test_tool", test_tool_dir)))

    if common.ram_drive_path():
        mounts.append(tuple(("tmpfs_path", common.ram_drive_path())))

    return mounts


def create_virtiofs_socket(tag):
    filepath = '/tmp/{}-{}'.format(tag, uuid.uuid4())

    logger.info("create virtiofs socket {}".format(filepath))

    return filepath


def start(argv):
    args = _parse_args(argv)
    for index in range(args.server_count):
        start_server(args, index)


def start_server(args, index):
    logger.info("start virtiofs-servers")

    for tag, path in _get_mount_paths():

        socket = create_virtiofs_socket(tag)

        recipe_set_env("VIRTIOFS_SOCKET_{}".format(tag), socket, index)

        virtiofs_server_binary = common.binary_path(
            "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")

        virtiofs = VirtioFsServer(virtiofs_server_binary,
                                  socket,
                                  path)

        virtiofs.start(common.output_path(), tag)
        append_recipe_err_files(
            ERR_LOG_FILE_NAMES_FILE, virtiofs.daemon.stderr_file_name
        )

        recipe_set_env("VIRTIOFS_PID_{}".format(tag),
                       str(virtiofs.virtiofs_server.daemon.process.pid),
                       index)


def stop(argv):
    args = _parse_args(argv)
    for index in range(args.server_count):
        stop_server(args, index)

    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError(
            "Errors occurred during virtiofs-server execution: {}".format(errors)
        )


def stop_server(args, index):
    logger.info("shutdown virtiofs-servers")
    for tag, _ in _get_mount_paths():
        pid = recipe_get_env("VIRTIOFS_PID_{}".format(tag), index)
        if pid:
            logger.info("will kill virtiofs-server with pid `%s`", pid)
            try:
                os.kill(int(pid), signal.SIGTERM)
            except OSError:
                logger.exception("While killing pid `%s`", pid)
                raise


def _parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server-count", help="Number of virtiofs servers to start")
    args = parser.parse_args(argv)

    if args.server_count == "$VIRTIOFS_SERVER_COUNT":
        args.server_count = 1
    else:
        args.server_count = int(args.server_count)

    return args


if __name__ == "__main__":
    declare_recipe(start, stop)
