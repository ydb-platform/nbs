import os
import stat
import sys
import time

from contrib.ydb.tests.library.harness.daemon import Daemon

VIRTIOFSD_SUPERVISOR = r"""
import errno
import fcntl
import os
import signal
import socket
import subprocess
import sys
import time

socket_path = sys.argv[1]
cmd = sys.argv[2:]
child = None
stopping = False


def set_inheritable(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)


def stop(signum, frame):
    global stopping
    stopping = True
    if child is not None and child.poll() is None:
        child.terminate()


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

try:
    os.unlink(socket_path)
except OSError as e:
    if e.errno != errno.ENOENT:
        raise

listener = socket.socket(socket.AF_UNIX)
listener.bind(socket_path)
listener.listen(1)
set_inheritable(listener.fileno())

try:
    while not stopping:
        child = subprocess.Popen(
            cmd + ["--fd", str(listener.fileno())],
            close_fds=False)

        while not stopping:
            rc = child.poll()
            if rc is not None:
                break
            time.sleep(0.2)

        if stopping:
            if child.poll() is None:
                child.terminate()
                deadline = time.time() + 5
                while child.poll() is None and time.time() < deadline:
                    time.sleep(0.1)
                if child.poll() is None:
                    child.kill()
                    child.wait()
            break

        if rc != 0:
            sys.exit(rc)

        time.sleep(0.1)
finally:
    listener.close()
    try:
        os.unlink(socket_path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
"""


def daemon_log_files(prefix, cwd):
    files = [
        ("stdout_file", ".out"),
        ("stderr_file", ".err"),
    ]

    ret = {}
    for tag, suffix in files:
        name = os.path.abspath(os.path.join(cwd, prefix + suffix))
        with open(name, mode='w'):
            pass

        ret[tag] = name

    return ret


class VirtioFsServer:
    def __init__(self, virtiofs_server_binary, socket_path, fspath):
        self.virtiofs_server_binary = virtiofs_server_binary
        self.socket_path = socket_path
        self.fspath = fspath

    @property
    def daemon(self):
        return self.virtiofs_server

    def _python_binary(self):
        for env in ("YA_PYTHON_BIN", "PYTHON"):
            path = os.getenv(env)
            if path and os.path.exists(path):
                return path

        if os.path.basename(sys.executable).startswith("python"):
            return sys.executable

        for path in ("/usr/bin/python3", "/usr/local/bin/python3"):
            if os.path.exists(path):
                return path

        return "python3"

    def start(self, output_path, tag):
        virtiofs_cmd = [
            self.virtiofs_server_binary,
            "--shared-dir", self.fspath,
        ]
        cmd = [
            self._python_binary(),
            "-c",
            VIRTIOFSD_SUPERVISOR,
            self.socket_path,
        ] + virtiofs_cmd

        self.virtiofs_server = Daemon(
            cmd,
            output_path,
            timeout=180,
            **daemon_log_files(prefix="virtiofs-server-{}".format(tag), cwd=output_path))

        self.virtiofs_server.start()
        self._wait_for_socket()

    def _wait_for_socket(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.virtiofs_server.daemon.process.poll() is not None:
                raise RuntimeError(
                    "virtiofs-server exited before creating socket {}".format(
                        self.socket_path))

            try:
                mode = os.stat(self.socket_path).st_mode
                if stat.S_ISSOCK(mode):
                    return
            except OSError:
                pass

            time.sleep(0.1)

        raise RuntimeError(
            "virtiofs-server did not create socket {} in {} seconds".format(
                self.socket_path, timeout))
