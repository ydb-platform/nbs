import argparse
import jinja2

from .errors import Error
from .test_cases import get_test_config

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.sdk.client import make_client
from cloud.blockstore.public.sdk.python.client import ClientCredentials, ClientError
from cloud.blockstore.pylibs.ycp import Ycp, make_ycp_engine

from library.python import resource


DEFAULT_VM_NAME = 'eternal-640gb-verify-checkpoint-test-vm-a'
DEFAULT_DISK_NAME = 'eternal-640gb-verify-checkpoint-test-disk-a'
DEFAULT_FILE_NAME = '/tmp/load-config.json'


def get_config_template(name: str) -> jinja2.Template:
    return jinja2.Template(resource.find(name).decode('utf8'))


class TestRunner:
    _VALIDATE_CHECKPOINT_CMD = ('IAM_TOKEN=%s %s %s --disk-id=%s --checkpoint-id=%s'
                                ' --read-all --io-depth=32 --config %s --throttling-disabled'
                                ' --client-performance-profile %s')
    _CLIENT_CONFIG_FILE = 'nbs-client.txt'
    _THROTTLER_CONFIG_FILE = 'throttler-profile.txt'

    def __init__(self, module_factories: common.ModuleFactories, args, logger):
        self.args = args
        self.logger = logger
        self.helpers = module_factories.make_helpers(args.dry_run)
        self.ycp = Ycp(
            args.profile_name,
            use_generated_config=args.generate_ycp_config,
            logger=logger,
            engine=make_ycp_engine(args.dry_run),
            ycp_config_generator=module_factories.make_config_generator(args.dry_run),
            helpers=self.helpers,
            ycp_requests_template_path=args.ycp_requests_template_path)
        self.test_config = get_test_config(module_factories, self.ycp, self.helpers, args, logger)

        self.port = args.nbs_port
        credentials = None
        self.token = None
        if args.use_auth:
            if args.service_account_id:
                self.token = self.ycp.create_iam_token_for_service_account(
                    args.service_account_id)
            else:
                self.token = self.ycp.create_iam_token()

            credentials = ClientCredentials(auth_token=self.token.iam_token)

        try:
            self.logger.info(f'Creating connection to compute_node=<{self.test_config.instance.compute_node}>')
            self.client = make_client(
                args.dry_run,
                endpoint='{}:{}'.format(self.test_config.instance.compute_node, self.port),
                credentials=credentials)
        except ClientError as e:
            raise Error('Failed to create grpc client' + e.message)

    def create_file(self, out: str):
        tmp_file = self.helpers.create_tmp_file()
        tmp_file.write(bytes(out, 'utf8'))
        tmp_file.flush()
        return tmp_file

    def create_checkpoint(self):
        self.logger.info(f'Creating checkpoint <id={self.test_config.checkpoint_id}>'
                         f' on disk <id={self.test_config.disk.id}>')
        self.client.create_checkpoint(self.test_config.disk.id, self.test_config.checkpoint_id)
        self.logger.info('Checkpoint successfully created')

    def delete_checkpoint(self, checkpoint_id: str):
        self.logger.info(f'Deleting checkpoint <id={checkpoint_id}>'
                         f' on disk <id={self.test_config.disk.id}>')
        self.client.delete_checkpoint(self.test_config.disk.id, checkpoint_id)
        self.logger.info('Checkpoint successfully deleted')

    def validate_checkpoint(self, eternal_config: str, nbs_client_config: str, throttler_config: str):
        self.logger.info('Validating checkpoint')
        cmd = self._VALIDATE_CHECKPOINT_CMD % (
            self.token.iam_token if self.token is not None else '',
            self.args.validator_path,
            eternal_config,
            self.test_config.disk.id,
            self.test_config.checkpoint_id,
            nbs_client_config,
            throttler_config
        )
        result = self.helpers.make_subprocess_run(cmd)
        if result.returncode:
            raise Error(f'Validator failed with exit_code=<{result.returncode}>.\n'
                        f'Stderr: {result.stderr}')
        self.logger.info('Validation finished')

    def find_checkpoints(self) -> [str]:
        stat = self.client.stat_volume(disk_id=self.test_config.disk.id)
        return stat['Checkpoints']

    def run_test(self):
        checkpoints = self.find_checkpoints()
        self.logger.info(f'Found {len(checkpoints)} checkpoints')
        self.logger.info('Deleting existing checkpoints')
        for c in checkpoints:
            self.delete_checkpoint(c)

        eternal_config = self.create_file(self.test_config.config_json)
        nbs_client_config = self.create_file(get_config_template(self._CLIENT_CONFIG_FILE).render(
            host=self.test_config.instance.compute_node,
            port=self.port,
            securePort=self.port if self.args.use_auth else None,
        ))
        throttler_config = self.create_file(get_config_template(self._THROTTLER_CONFIG_FILE).render(
            maxReadBandwidth=self.args.max_read_bandwidth * 1024 ** 2
        ))

        self.logger.info(f'Running checkpoint validation test on cluster <{self.args.cluster}>')
        try:
            self.create_checkpoint()
            self.validate_checkpoint(eternal_config.name, nbs_client_config.name, throttler_config.name)
        finally:
            self.delete_checkpoint(self.test_config.checkpoint_id)


def parse_args():
    parser = argparse.ArgumentParser()

    verbose_quite_group = parser.add_mutually_exclusive_group()
    verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
    verbose_quite_group.add_argument('-q', '--quite', action='store_true')

    parser.add_argument('--teamcity', action='store_true', help='use teamcity logging format')

    test_arguments_group = parser.add_argument_group('test arguments')
    common.add_common_parser_arguments(test_arguments_group)
    test_arguments_group.add_argument(
        '--validator-path',
        type=str,
        required=True,
        help='specify path to checkpoint validator')
    test_arguments_group.add_argument(
        '--max-read-bandwidth',
        type=int,
        default=300,
        help='specify max read bandwidth in MB/s')
    test_arguments_group.add_argument(
        '--service-account-id',
        type=str,
        help='id of service account, which runs checkpoint validator')
    test_arguments_group.add_argument(
        '--folder-id',
        type=str,
        help='folder where to find the disk')
    test_arguments_group.add_argument(
        '--nbs-port',
        type=str,
        required=True,
        help='specify nbs server port'
    )
    test_arguments_group.add_argument(
        '--use-auth',
        action='store_true',
        default=False,
        help='make requests to nbs with authorization')
    test_arguments_group.add_argument(
        '--vm-name',
        type=str,
        default=DEFAULT_VM_NAME,
        help='VM name')
    test_arguments_group.add_argument(
        '--disk-name',
        type=str,
        default=DEFAULT_DISK_NAME,
        help='disk name')
    test_arguments_group.add_argument(
        '--file-name',
        type=str,
        default=DEFAULT_FILE_NAME,
        help='file name')

    args = parser.parse_args()

    if args.profile_name is None:
        args.profile_name = args.cluster

    return args
