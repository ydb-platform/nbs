import os
import logging
import signal
import uuid
import library.python.testing.recipe

import yatest.common as common

import core.config
from cloud.storage.core.tools.testing.virtiofs_server.lib import VirtioFsServer
from library.python.testing.recipe import declare_recipe


logger = logging.getLogger(__name__)


def _get_mount_paths():
    if "TEST_TOOL" in os.environ:
        test_tool_dir = os.path.dirname(os.environ["TEST_TOOL"])
    else:
        test_tool_dir = core.config.tool_root()

    if 'ASAN_SYMBOLIZER_PATH' in os.environ:
        toolchain = os.path.dirname(os.path.dirname(
            os.environ['ASAN_SYMBOLIZER_PATH']))

    mounts = [("source_path", common.source_path()),  # need to mound original source root as test environment has links into it
              ("build_path", common.build_path()),
              ("test_tool", test_tool_dir),
              ("toolchain", toolchain)]
    if common.ram_drive_path():
        mounts.append(tuple(("tmpfs_path", common.ram_drive_path())))
    return mounts


def create_virtiofs_socket(tag):
    filepath = '/tmp/{}-{}'.format(tag, uuid.uuid4())

    logger.info("create virtiofs socket {}".format(filepath))

    return filepath


def start(argv):
    logger.info("start virtiofs-servers")

    for tag, path in _get_mount_paths():

        socket = create_virtiofs_socket(tag)

        library.python.testing.recipe.set_env(
            "VIRTIOFS_SOCKET_{}".format(tag), socket)

        virtiofs_server_binary = common.binary_path(
            "cloud/storage/core/tools/testing/virtiofs_server/bin/virtiofs-server")

        virtiofs = VirtioFsServer(virtiofs_server_binary,
                                  socket,
                                  path)

        virtiofs.start(common.output_path(), tag)

        library.python.testing.recipe.set_env(
            "VIRTIOFS_PID_{}".format(tag), str(virtiofs.virtiofs_server.daemon.process.pid))


def stop(argv):
    logger.info("shutdown virtiofs-servers")

    for tag, _ in _get_mount_paths():
        if "VIRTIOFS_PID_{}".format(tag) in os.environ:
            pid = os.environ["VIRTIOFS_PID_{}".format(tag)]

            logger.info("will kill virtiofs-server with pid `%s`", pid)
            try:
                os.kill(int(pid), signal.SIGTERM)
            except OSError:
                logger.exception("While killing pid `%s`", pid)
                raise


if __name__ == "__main__":
    declare_recipe(start, stop)
