from subprocess import CompletedProcess
import sys

DEFAULT_SSH_USER = 'root'


class TestHelpers:

    class TmpFile:

        def __init__(self, id):
            self.name = f'{id}.tmp'
            sys.stdout.write(f'Create tmp file with name=<{self.name}>\n')

        def write(self, text: str):
            sys.stdout.write(f'Write to tmp file with name=<{self.name}>\n')

        def flush(self):
            sys.stdout.write(f'Flush tmp file with name=<{self.name}>\n')

    def __init__(self):
        self._id = 0

    def wait_until_instance_becomes_available_via_ssh(
        self,
        ip: str,
        ssh_key_path: str = None,
        user: str = DEFAULT_SSH_USER
    ) -> None:
        sys.stdout.write(f'Waiting for instance {ip}\n')

    def wait_for_block_device_to_appear(
        self,
        ip: str,
        path: str,
        ssh_key_path: str = None,
        user: str = DEFAULT_SSH_USER
    ) -> None:
        sys.stdout.write(f'Waiting for bdev {ip}/{path}\n')

    def make_get_request(self, url: str) -> int:
        sys.stdout.write(f'GET request url=<{url}>\n')
        return 200

    def make_subprocess_run(self, command) -> CompletedProcess:
        sys.stdout.write(f'Execute subprocess.run command=<{command}>\n')
        return CompletedProcess(command, 0)

    def generate_id(self):
        self._id += 1
        return self._id

    def create_tmp_file(self, suffix=''):
        self._id += 1
        return TestHelpers.TmpFile(self._id)


def make_helpers_stub(dry_run):
    return TestHelpers()


def get_clat_mean_from_fio_report(x):
    if 'clat' in x:
        return x['clat']['mean']  # fio-2

    return x['clat_ns']['mean'] / 1000  # fio-3


def add_common_parser_arguments(parser):
    parser.add_argument(
        '-c',
        '--cluster',
        type=str,
        required=True,
        help='run test on the specified cluster')
    parser.add_argument(
        '--cluster-config-path',
        type=str,
        default=None,
        help='specify path to cluster config file')
    parser.add_argument(
        '--ycp-requests-template-path',
        type=str,
        default=None,
        help='specify path to ycp request templates')
    parser.add_argument(
        '--no-generate-ycp-config',
        dest='generate_ycp_config',
        action='store_false',
        help='do not generate ycp config')
    parser.add_argument(
        '--ssh-key-path',
        type=str,
        default=None,
        help='specify path to ssh key')
    parser.add_argument(
        '--profile-name',
        type=str,
        default=None,
        help='ycp profile name')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='dry run')
