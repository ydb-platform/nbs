import os
import yatest.common as common


def env_with_guest_index(env, guest_index):
    if guest_index == 0:
        return env

    return "{}__{}".format(env, guest_index)


def get_mount_paths(inst_index=0):
    if 'ASAN_SYMBOLIZER_PATH' in os.environ:
        toolchain = os.path.dirname(os.path.dirname(
            os.environ['ASAN_SYMBOLIZER_PATH']))

    def socket_path(sock_type):
        return os.getenv(env_with_guest_index("VIRTIOFS_SOCKET_{}".format(sock_type), inst_index))

    mounts = [("source_path", common.source_path(), socket_path("source_path")),  # need to mount original source root as test environment has links into it
              ("build_path", common.build_path(), socket_path("build_path")),
              ("toolchain", toolchain, socket_path("toolchain"))]

    if "TEST_TOOL" in os.environ:
        test_tool_dir = os.path.dirname(os.environ["TEST_TOOL"])
        mounts.append(tuple(("test_tool", test_tool_dir, socket_path("test_tool"))))

    if common.ram_drive_path():
        mounts.append(tuple(("tmpfs_path", common.ram_drive_path(), socket_path("tmpfs_path"))))

    return mounts
