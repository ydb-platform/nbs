import argparse
import json
import random
import sys
import time

from cloud.blockstore.public.sdk.python.client import ClientError, ClientCredentials
import cloud.blockstore.public.sdk.python.protos as protos
from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config
from cloud.blockstore.pylibs.ycp import YcpWrapper, make_ycp_engine
from cloud.blockstore.pylibs.sdk import make_client


class Error(Exception):
    pass


_DEFAULT_KILL_PERIOD = 600
_DEFAULT_MIGRATION_TIMEOUT = 28800  # 8h


class Migration:

    def __init__(self, runner, volume):
        self.__runner = runner
        self.__volume = volume
        self.__agent_id = None

    def __enter__(self):
        self.__agent_id = self.__runner.start_migration(self.__volume)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        message = 'finish migration test'
        if exc_type is not None:
            message = 'halt migration test'
        self.__runner.change_agent_state(self.__agent_id, 0, message)


class TestRunner:
    __DEFAULT_POSTPONED_WEIGHT = 128 * 1024 ** 2  # Bytes

    def __init__(self, module_factories, args, logger):
        self.module_factories = module_factories
        self.args = args
        self.logger = logger
        self.helpers = module_factories.make_helpers(self.args.dry_run)
        self.port = args.nbs_port
        self.need_kill_tablet = args.kill_tablet
        self.sleep_timeout = args.kill_period
        self.kills = 0
        self.max_kills = -1  # inf
        self.tablet_id = None
        self.instance = None
        self.disk_id = None
        self.credentials = None
        self.endpoint = None

        assert bool(args.disk_name) != bool(args.disk_id), \
            'Either disk name or id should be provided'

        if args.dry_run:
            self.sleep_timeout = 0
            self.max_kills = 6

        if args.no_ycp:
            assert args.disk_id, 'Disk id should be provided'
            assert args.nbs_host, 'Nbs endpoint host should be provided'
            assert not args.use_auth, 'Can\'t use auth without ycp'
            assert not args.check_load, 'Can\'t check load without ycp'
            assert not args.disk_name, 'Can\'t find disk by name without ycp'

            self.disk_id = args.disk_id
            self.endpoint = f'{args.nbs_host}:{args.nbs_port}'
        else:
            assert args.cluster, 'Cluster should be provided'
            assert args.zone, 'Zone should be provided'
            cluster = get_cluster_test_config(args.cluster, args.zone, args.cluster_config_path)
            folder_desc = cluster.ipc_type_to_folder_desc(args.ipc_type)

            self.logger.info(f'Create ycp wrapper: {args.profile_name} {folder_desc.folder_id}')
            ycp = YcpWrapper(
                args.profile_name,
                folder_desc,
                logger,
                make_ycp_engine(args.dry_run),
                self.module_factories.make_config_generator(args.dry_run),
                self.helpers,
                args.generate_ycp_config,
                args.ycp_requests_template_path)

            for disk in ycp.list_disks():
                if disk.name == args.disk_name or disk.id == args.disk_id:
                    self.disk_id = disk.id
                    self.instance = ycp.get_instance(disk.instance_ids[0])
                    break

            if self.disk_id is None:
                description = f'id={args.disk_id}' if args.disk_id else f'name={args.disk_name}'
                raise Error(f"Can't find disk with {description}")

            if args.use_auth:
                if args.service_account_id:
                    token = ycp.create_iam_token_for_service_account(args.service_account_id)
                else:
                    token = ycp.create_iam_token()
                self.credentials = ClientCredentials(auth_token=token.iam_token)

            host = args.nbs_host if args.nbs_host else self.instance.compute_node
            self.endpoint = '{}:{}'.format(host, self.port)

        try:
            self.client = make_client(
                self.args.dry_run,
                self.endpoint,
                credentials=self.credentials,
                log=logger
            )
        except ClientError as e:
            raise Error(f'Failed to create grpc client:\n{e}')

        if self.need_kill_tablet:
            self.tablet_id = self.get_volume_tablet_id()

    @common.retry(tries=5, delay=10, exception=Error)
    def change_agent_state(self, agent_id: str, state: int, message):
        self.logger.info(f'Changing state of agent_id=<{agent_id}> to state=<{state}>')
        disk_description = f'{self.args.disk_name}' if self.args.disk_name \
            else f'id={self.args.disk_id}'
        try:
            data = json.dumps({
                'Message': message + f' ({disk_description})',
                'ChangeAgentState': {
                    'AgentId': agent_id,
                    'State': state
                }
            })

            self.client.execute_action(
                action='diskregistrychangestate',
                input_bytes=str.encode(data))
        except ClientError as e:
            raise Error(f'Failed to change agent state <agent_id={agent_id}> <state={state}>:\n{e}')

    @common.retry(tries=5, delay=10, exception=Error)
    def get_volume_tablet_id(self):
        try:
            d = self.client.execute_action(
                action='DescribeVolume',
                input_bytes=str.encode('{"DiskId":"%s"}' % self.disk_id))
            return json.loads(d)['VolumeTabletId']
        except ClientError as e:
            raise Error(f'Failed to execute DescribeVolume action for disk <disk_id={self.disk_id}>:\n{e}')

    @common.retry(tries=5, delay=10, exception=Error)
    def get_volume_description(self):
        try:
            volume = self.client.describe_volume(disk_id=self.disk_id)
            return volume
        except ClientError as e:
            raise Error(f'Failed to execute describe volume for disk <disk_id={self.disk_id}>:\n{e}')

    @common.retry(tries=5, delay=10, exception=Error)
    def check_load(self):
        self.logger.info(f'check if eternal load running on instance with ip=<{self.instance.ip}>')
        with self.module_factories.make_ssh_client(self.args.dry_run, self.instance.ip, ssh_key_path=self.args.ssh_key_path) as ssh:
            _, stdout, _ = ssh.exec_command('pgrep eternal-load')
            stdout.channel.exit_status_ready()
            out = ''.join(stdout.readlines())
            if not out:
                raise Error(f'Eternal load not running on instance with ip=<{self.instance.ip}>')

    def start_migration(self, volume) -> str:
        devices = volume.Devices
        agent_id = random.choice(devices).AgentId
        self.change_agent_state(agent_id, 1, 'start migration test')
        return agent_id

    @common.retry(tries=5, delay=10, exception=Error)
    def check_migration(self):
        try:
            volume = self.client.describe_volume(disk_id=self.disk_id)
        except ClientError as e:
            raise Error(f'Failed to check migration status for disk:\n{e}')
        return getattr(volume, 'Migrations', False)

    @common.retry(tries=5, delay=10, exception=Error)
    def check_migration_started(self):
        self.logger.info('Check if migration started')
        if not self.check_migration():
            raise Error('Migration not started')

    @common.retry(tries=5, delay=10, exception=Error)
    def kill_tablet(self):
        if self.need_kill_tablet:
            self.logger.info(f'Killing tablet <tablet_id={self.tablet_id}>')
            try:
                self.client.execute_action(
                    action='killtablet',
                    input_bytes=str.encode('{"TabletId": %s}' % self.tablet_id))
            except ClientError as e:
                raise Error(f'Failed to kill tablet <tablet_id={self.tablet_id}>:\n{e}>')

        self.kills += 1
        time.sleep(self.sleep_timeout)

    @common.retry(tries=5, delay=10, exception=Error)
    def change_postponed_weight(self, weight: int, blocks: int):
        self.logger.info(f'Changing max postponed weight for disk <disk_id={self.disk_id}> to <weight={weight}>')
        try:
            self.client.resize_volume(
                disk_id=self.disk_id,
                blocks_count=blocks,
                channels_count=0,
                config_version=None,
                performance_profile=protos.TVolumePerformanceProfile(MaxPostponedWeight=weight))
        except ClientError as e:
            self.logger(e)
            raise Error(f'Failed to change postponed weight to <weight={weight}>:\n{e}')

    def check_timeout(self, start) -> bool:
        if self.args.timeout == -1:
            return True
        return time.time() - start < self.args.timeout

    def run(self):
        self.logger.info('Test started')

        if self.args.check_load:
            self.check_load()

        volume = self.get_volume_description()

        postponed_weight = volume.PerformanceProfile.MaxPostponedWeight
        blocks_count = volume.BlocksCount

        with Migration(self, volume):
            self.check_migration_started()
            self.logger.info('Migration started, waiting until it finished')

            start = time.time()
            while self.check_migration() and (self.max_kills == -1 or self.kills < self.max_kills):
                if not self.check_timeout(start):
                    raise Error('Timeout. Stopping test')

                self.kill_tablet()
                if self.kills == 5:
                    self.change_postponed_weight(postponed_weight + 1, blocks_count)

            self.change_postponed_weight(self.__DEFAULT_POSTPONED_WEIGHT, blocks_count)

        if self.args.check_load:
            self.check_load()


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--teamcity', action='store_true', help='use teamcity logging format')
    parser.add_argument('--verbose', action='store_true', help='enables debug logging level')
    parser.add_argument('--quite', action='store_true', help='log only critical events')

    test_arguments_group = parser.add_argument_group('test arguments')
    common.add_common_parser_arguments(test_arguments_group, cluster_required=False)
    test_arguments_group.add_argument(
        '--ipc-type',
        type=str,
        default='grpc',
        help='run migration test with specified ipc-type')
    test_arguments_group.add_argument(
        '--disk-name',
        type=str,
        required=False,
        help='run migration test for specified non-repl disk')
    test_arguments_group.add_argument(
        '--disk-id',
        type=str,
        required=False,
        help='run migration test for specified non-repl disk')
    test_arguments_group.add_argument(
        '--check-load',
        action='store_true',
        help='if set test will check that eternal load on specified instance not fails after migration'
    )
    test_arguments_group.add_argument(
        '--kill-tablet',
        action='store_true',
        help='if set, test will kill disk tablet during migration'
    )
    test_arguments_group.add_argument(
        '--timeout',
        type=int,
        default=_DEFAULT_MIGRATION_TIMEOUT,
        help='specify test timeout'
    )
    test_arguments_group.add_argument(
        '--kill-period',
        type=int,
        required='--kill-tablet' in sys.argv,
        default=_DEFAULT_KILL_PERIOD,
        help='specify sleep time between kills'
    )
    test_arguments_group.add_argument(
        '--service-account-id',
        type=str,
        help='specify service-account-id for authorization to nbs server'
    )
    test_arguments_group.add_argument(
        '--nbs-host',
        required=False,
        help='specify nbs server host'
    )
    test_arguments_group.add_argument(
        '--nbs-port',
        type=int,
        help='specify nbs server port'
    )
    test_arguments_group.add_argument(
        '--use-auth',
        action='store_true',
        default=False,
        help='make requests to nbs with authorization')
    test_arguments_group.add_argument(
        '--no-ycp',
        action='store_true',
        default=False,
        help='don\'t use ycp')
    test_arguments_group.add_argument(
        '--zone',
        type=str,
        help='zone id to render template config with')

    args = parser.parse_args()
    if args.profile_name is None:
        args.profile_name = args.cluster
    if args.nbs_port is None:
        args.nbs_port = 9768 if args.use_auth else 9766

    return args
