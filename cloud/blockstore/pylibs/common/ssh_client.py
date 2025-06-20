import io
import ipaddress
import shlex
import subprocess
import uuid

from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple

from .retry import retry
from .ssh import (
    SftpTestClient,
    SshException,
    SshTestClient,
    SSH_DEFAULT_RETRIES_COUNT,
    SSH_DEFAULT_USER,
)


class SshClient:

    class _Stat(NamedTuple):
        st_size: int
        st_uid: int
        st_gid: int
        st_mode: int
        st_atime: int
        st_mtime: int

    class _ChannelStub:
        def __init__(
            self,
            client: 'SshClient',
            received_return_code: int | None = None,
        ) -> None:
            self._client = client
            self._received_return_code = received_return_code

        def exec_command(
            self,
            command: list[str] | str,
            check: bool = True,
        ) -> tuple[None, 'SshClient._StdoutStub', 'SshClient._StdoutStub']:
            if self._received_return_code is not None:
                # Channel is not actually closed,
                # yet here we mock paramiko interface,
                # and the channel is expected to be closed
                # after executing the command.
                # See: https://docs.paramiko.org/en/3.3/api/channel.html.
                raise SshException(
                    "Trying to execute command in the closed channel")
            return self._client.exec_command(command, check=check)

        def recv_exit_status(self) -> int:
            if self._received_return_code is None:
                return 0
            return self._received_return_code

        @staticmethod
        def exit_status_ready() -> bool:
            # Always true since the command execution is blocking
            return True

    class _StdoutStub:
        # Someone might think that passing ssh client
        # would be a bad engineering, yet, the issue is that
        # here we try to mimic paramiko's interface, and they
        # pass the channel with return codes as a property of the ChannelFile.
        # So, since channel requires command execution,
        # we are passing the client as is.
        def __init__(
            self,
            client: 'SshClient',
            stdout: bytes,
            return_code: int,
        ) -> None:
            self._client = client
            self._stdout = stdout
            self._return_code = return_code
            self._io = io.StringIO(self._stdout.decode())

        @property
        def channel(self) -> 'SshClient._ChannelStub':
            return SshClient._ChannelStub(
                self._client,
                self._return_code,
            )

        def readlines(self) -> list[str]:
            return self._stdout.decode().splitlines()

        def readline(self, size: int | None = None) -> str:
            if size is None:
                return self.read().decode()
            return self._io.readline(size)

        def read(self) -> bytes:
            return self._stdout

    def __init__(self):
        self._host: str | None = None
        self._ssh_key_path: str | None = None
        self._username: str | None = None
        self._key_path_cmd_argument: list[str] = []
        self._authorization_string: str | None = None
        self._scp_authorization_string: str | None = None

    def __enter__(self) -> 'SshClient':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        pass

    def chmod(self, remote_path: str, permissions: int | str) -> None:
        if isinstance(permissions, int):
            permissions = f'{permissions:o}'
        self._exec_command(['chmod', permissions, remote_path], check=True)

    # noinspection PyUnusedLocal
    def configure_ssh_client(
        self,
        ip: str,
        ssh_key_path: str | None = None,
        user: str = SSH_DEFAULT_USER,
        retries: int = SSH_DEFAULT_RETRIES_COUNT
    ) -> None:
        self._host = ip
        self._ssh_key_path = ssh_key_path
        self._username = user
        if ssh_key_path is None:
            self._key_path_cmd_argument = []
        else:
            self._key_path_cmd_argument = ['-i', str(self._ssh_key_path)]
        self._authorization_string = f'{self._username}@{self._host}'
        self._scp_authorization_string = self._authorization_string
        try:
            ip = ipaddress.ip_address(self._host)
            # IPv6 address shall be properly escaped in scp
            if isinstance(ip, ipaddress.IPv6Address):
                self._scp_authorization_string = (
                    f'{self._username}@[{self._host}]')
        except ValueError:
            pass

    def download_file(self, local_path: str, remote_path: str) -> None:
        command_line = [
            'scp',
            '-o', 'ServerAliveInterval=60',
            '-o', 'IdentitiesOnly=yes',
            '-o', 'StrictHostKeyChecking=no',
            '-o', "UserKnownHostsFile=/dev/null",
            *self._key_path_cmd_argument,
            f'{self._scp_authorization_string}:{remote_path}',
            local_path,
        ]
        try:
            subprocess.check_call(command_line)
        except subprocess.SubprocessError as e:
            raise SshException(e)

    # FIXME: maybe we retry too much/with too big of a delay?
    @retry(tries=10, delay=60)
    def exec_command(
        self,
        command: str | list[str],
        check: bool = True,
    ) -> tuple[None, '_StdoutStub', '_StdoutStub']:
        result = self._exec_command(command, check=check)
        return (
            None,
            self._StdoutStub(self, result.stdout, result.returncode),
            self._StdoutStub(self, result.stderr, result.returncode),
        )

    def mkdir(self, remote_path: str) -> None:
        self._exec_command(['mkdir', '-p', remote_path], check=True)

    def open_session(self) -> 'SshClient._ChannelStub':
        return self._ChannelStub(self)

    def rmdir(self, remote_path: str) -> None:
        self._exec_command(['rm', '-rf', str(remote_path)], check=True)

    def stat(self, path: str) -> 'SshClient._Stat':
        result = self._exec_command(
            ['stat',  '-c', '%s %u %g %f %X %Y', path], True)
        stat_parameters = result.stdout.decode().strip().split()
        try:
            [
                st_size,
                st_uid,
                st_gid,
                st_mode_hex,
                st_atime,
                st_mtime,
            ] = stat_parameters
            return self._Stat(
                st_size=int(st_size),
                st_uid=int(st_uid),
                st_gid=int(st_gid),
                st_mode=int(st_mode_hex, 16),
                st_atime=int(st_atime),
                st_mtime=int(st_mtime),
            )
        except (ValueError, TypeError) as e:
            raise SshException(
                f"Incorrect stat output for {path}, "
                f"on host {self._host}, "
                f"parameters: {stat_parameters}",
            ) from e

    def truncate(self, remote_path: str, size: int) -> None:
        self._exec_command(
            ['truncate', '-s', f'{size}', remote_path], check=True)

    @retry(tries=10, delay=20)
    def upload_file(self, local_path: str, remote_path: str) -> None:
        command_line = [
            'scp',
            '-o', 'ServerAliveInterval=60',
            '-o', 'IdentitiesOnly=yes',
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            *self._key_path_cmd_argument,
            local_path,
            f'{self._scp_authorization_string}:{remote_path}',
        ]
        try:
            subprocess.check_call(command_line)
        except subprocess.SubprocessError as e:
            raise SshException(e)

    def _create_ssh_commandline(self, command_line: str) -> list[str]:
        return [
            'ssh',
            '-o', 'ServerAliveInterval=60',
            '-o', 'IdentitiesOnly=yes',
            '-o', 'StrictHostKeyChecking=no',
            '-o', "UserKnownHostsFile=/dev/null",
            *self._key_path_cmd_argument,
            '-T',  # Disable pseudo-terminal allocation.
            self._authorization_string,
            command_line,
        ]

    def _exec_command(
        self,
        command_line: str | list[str],
        check: bool = False,
    ) -> subprocess.CompletedProcess:
        if isinstance(command_line, list):
            command_line = shlex.join(command_line)
        try:
            return subprocess.run(
                self._create_ssh_commandline(command_line),
                capture_output=True,
                check=check,
            )
        except subprocess.CalledProcessError as e:
            raise SshException(e)


class SftpClient:

    _tmp_dir = Path('/tmp/nbs_test_sftp_client')

    class _SftpFile:
        # FIXME: We do not process file modes
        #  yet there is no point in implementing this,
        #  since we probably won't need to work with remote
        #  files except for download/upload.
        def __init__(self, client: SshClient, path: str) -> None:
            self._client = client
            self._path = path

        def read(self) -> str:
            path = SftpClient._tmp_dir / str(uuid.uuid4())
            self._client.download_file(str(path.resolve()), path)
            return path.read_text()

        def write(self, text: str | bytes) -> None:
            path = SftpClient._tmp_dir / str(uuid.uuid4())
            if isinstance(text, str):
                path.write_text(text)
            else:
                path.write_bytes(text)
            self._client.upload_file(str(path.resolve()), self._path)

        def flush(self) -> None:
            pass

        def readlines(self) -> list[str]:
            return self.read().splitlines()

        def close(self) -> None:
            pass

    def __init__(self, client: SshClient) -> None:
        self._client = client
        self._tmp_dir.mkdir(exist_ok=True)

    def put(self, src, dst) -> None:
        self._client.upload_file(src, dst)

    def chmod(self, dst, flags="r") -> None:
        self._client.chmod(dst, flags)

    def truncate(self, dst, size) -> None:
        self._client.truncate(dst, size)

    # noinspection PyUnusedLocal
    def file(self, dst, flags="r") -> 'SftpClient._SftpFile':
        return self._SftpFile(self._client, dst)

    def mkdir(self, path: str) -> None:
        self._client.mkdir(path)

    def rmdir(self, path: str) -> None:
        self._client.rmdir(path)

    def stat(self, path) -> 'SshClient._Stat':
        return self._client.stat(path)

    def get(self, remotepath: str, localpath: str) -> None:
        self._client.download_file(
            local_path=localpath,
            remote_path=remotepath,
        )

    def __enter__(self) -> 'SftpClient':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        pass


@contextmanager
def ssh_client(
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
) -> SshClient:
    with SshClient() as client:
        client.configure_ssh_client(ip, ssh_key_path, user, retries)
        yield client


def make_ssh_channel(
    dry_run: bool,
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
):
    if dry_run:
        return SshTestClient.FakeStdout().channel

    client = SshClient()
    client.configure_ssh_client(ip, ssh_key_path, user, retries)
    return client.open_session()


def make_ssh_client(
    dry_run: bool,
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
):
    if dry_run:
        return SshTestClient(ip)

    return ssh_client(ip, ssh_key_path, user, retries)


@contextmanager
def sftp_client(
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
) -> SftpClient:
    with ssh_client(ip, ssh_key_path, user, retries) as ssh:
        yield SftpClient(ssh)


def make_sftp_client(
    dry_run: bool,
    ip: str,
    ssh_key_path: str = None,
    user: str = SSH_DEFAULT_USER,
    retries: int = SSH_DEFAULT_RETRIES_COUNT
):
    if dry_run:
        return SftpTestClient(ip)

    return sftp_client(ip, ssh_key_path, user, retries)
