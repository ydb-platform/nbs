import os

from contrib.ydb.tests.library.harness.daemon import Daemon


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

    def start(self, output_path, tag):
        cmd = [self.virtiofs_server_binary, "--socket-path={}".format(self.socket_path), "-o", "source={}".format(self.fspath)]

        self.virtiofs_server = Daemon(
            cmd,
            output_path,
            timeout=180,
            **daemon_log_files(prefix="virtiofs-server-{}".format(tag), cwd=output_path))

        self.virtiofs_server.start()
