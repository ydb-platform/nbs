import os
import yatest.common as common

import core.config


def get_mount_paths():
    if "TEST_TOOL" in os.environ:
        test_tool_dir = os.path.dirname(os.environ["TEST_TOOL"])
    else:
        test_tool_dir = core.config.tool_root()

    if 'ASAN_SYMBOLIZER_PATH' in os.environ:
        toolchain = os.path.dirname(os.path.dirname(
            os.environ['ASAN_SYMBOLIZER_PATH']))

    mounts = [("source_path", common.source_path(), os.getenv("VIRTIOFS_SOCKET_{}".format("source_path"))),  # need to mound original source root as test environment has links into it
              ("build_path", common.build_path(), os.getenv("VIRTIOFS_SOCKET_{}".format("build_path"))),
              ("test_tool", test_tool_dir, os.getenv("VIRTIOFS_SOCKET_{}".format("test_tool"))),
              ("toolchain", toolchain, os.getenv("VIRTIOFS_SOCKET_{}".format("toolchain")))]
    if common.ram_drive_path():
        mounts.append(tuple(("tmpfs_path", common.ram_drive_path(), os.getenv("VIRTIOFS_SOCKET_{}".format("tmpfs_path")))))
    return mounts
