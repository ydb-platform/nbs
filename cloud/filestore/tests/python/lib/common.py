import os
import signal
import time
import yatest.common as common

import cloud.filestore.public.sdk.python.client as client


def wait_for(predicate, timeout_seconds=5.0, step_seconds=0.5, multiply=2, max_step_seconds=5.0):
    finish_time = time.time() + timeout_seconds
    while time.time() < finish_time:
        if predicate():
            return True
        step_seconds = min(step_seconds * multiply, max_step_seconds)
        time.sleep(step_seconds)
    return False


def __is_dead(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return True
    else:
        return False


def shutdown(pid, timeout=60):
    try:
        os.kill(pid, signal.SIGTERM)
        if not wait_for(lambda: __is_dead(pid), timeout):
            os.kill(pid, signal.SIGKILL)
    except BaseException:
        pass


def daemon_log_files(prefix, cwd):
    files = [
        ("stdout_file", ".out"),
        ("stderr_file", ".err"),
    ]

    index = 0
    while True:
        ret = {}
        for tag, suffix in files:
            name = "{}_{}{}".format(prefix, index, suffix) \
                if index else "{}{}".format(prefix, suffix)

            path = os.path.abspath(os.path.join(cwd, name))
            if not os.path.exists(path):
                ret[tag] = path

        if len(ret) == len(files):
            break

        index += 1

    for path in ret.values():
        with open(path, mode='w'):
            pass

    return ret


def get_filestore_mount_paths(paths=None):
    if paths is None:
        paths = os.getenv("NFS_MOUNT_PATH").split(",")

    for path in paths:
        if len(path) == 0:
            raise RuntimeError("Invalid path")

        if not os.path.isdir(path):
            raise RuntimeError("Path is not a directory: {}".format(path))

        if not os.path.ismount(path):
            raise RuntimeError("Path is not a mount point: {}".format(path))

    return paths


def get_filestore_mount_path(path=None):
    return get_filestore_mount_paths([path] if path is not None else None)[0]


def is_grpc_error(exception):
    if isinstance(exception, client.ClientError):
        return exception.facility == client.EFacility.FACILITY_GRPC.value

    return False


def get_restart_interval(interval):
    if interval is None or interval in [
        "$NFS_RESTART_INTERVAL",
        "$VHOST_RESTART_INTERVAL",
        "$GANESHA_RESTART_INTERVAL"
    ]:
        return None

    return int(interval)


def get_restart_flag(flag, name):
    if flag is None or flag in [
        "$NFS_RESTART_FLAG",
        "$VHOST_RESTART_FLAG",
        "$GANESHA_RESTART_FLAG",
    ]:
        return None

    return os.path.join(common.work_path(), name)
