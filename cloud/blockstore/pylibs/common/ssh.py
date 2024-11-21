import sys

SSH_DEFAULT_RETRIES_COUNT = 15  # seconds
SSH_DEFAULT_USER = 'root'


class SshException(Exception):
    pass


class SshTestClient:

    class FakeStdout:

        class FakeChannel:
            def exec_command(self, command, *args, **kwargs):
                sys.stdout.write(f'Execute command {command}')

            def recv_exit_status(self):
                return 0

            def exit_status_ready(self):
                return 1

        def __init__(self):
            self.channel = SshTestClient.FakeStdout.FakeChannel()

        def readlines(self):
            return []

        def readline(self, size: int | None = None):
            return b''

        def read(self):
            return b''

    class FakeTransport:
        def __init__(self, ip: str):
            self.ip = ip

        def open_session(self):
            sys.stdout.write(f'Open SSH session {self.ip}')
            return SshTestClient.FakeStdout.FakeChannel()

    def __init__(self, ip: str):
        self.ip = ip

    def exec_command(self, command, *args, **kwargs):
        sys.stdout.write(f'SSH {self.ip}: {command}\n')
        return None, SshTestClient.FakeStdout(), SshTestClient.FakeStdout()

    def get_transport(self):
        return SshTestClient.FakeTransport(self.ip)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class SftpTestClient:

    class SftpTestFile:
        def __init__(self, ip):
            self.ip = ip

        def read(self):
            sys.stdout.write(f'SFTP READ FILE {self.ip}')
            return 'text'

        def write(self, text):
            sys.stdout.write(f'SFTP WRITE FILE {self.ip}')

        def flush(self):
            sys.stdout.write(f'SFTP FLUSH FILE {self.ip}')

        def readlines(self):
            return self.read()

        def close(self):
            sys.stdout.write(f'SFTP CLOSE FILE {self.ip}')

    def __init__(self, ip):
        self.ip = ip

    def put(self, src, dst):
        sys.stdout.write(f'SFTP PUT {self.ip}/{src} -> {dst}\n')

    def chmod(self, dst, flags="r"):
        sys.stdout.write(f'SFTP CHMOD {self.ip}/{dst} f={flags}\n')

    def truncate(self, dst, size):
        sys.stdout.write(f'SFTP TRUNCATE {self.ip}/{dst} size={size}\n')

    def file(self, dst, flags="r"):
        sys.stdout.write(f'SFTP FILE {self.ip}/{dst} f={flags}\n')
        return SftpTestClient.SftpTestFile(self.ip)

    def mkdir(self, path):
        sys.stdout.write(f'SFTP MKDIR {self.ip} dir={path}\n')

    def rmdir(self, path):
        sys.stdout.write(f'SFTP RMDIR {self.ip} dir={path}\n')

    def get(self, remotepath, localpath):
        sys.stdout.write(f'SFTP GET {self.ip} remotepath={remotepath} localpath={localpath}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


def make_ssh_channel_stub(
    dry_run: bool,
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
):
    return SshTestClient.FakeStdout().channel


def make_ssh_client_stub(
    dry_run: bool,
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
):
    return SshTestClient(ip)


def make_sftp_client_stub(
    dry_run: bool,
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
):
    return SftpTestClient(ip)
