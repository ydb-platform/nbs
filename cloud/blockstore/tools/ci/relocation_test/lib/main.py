import argparse
import json

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config
from cloud.blockstore.pylibs.ycp import YcpWrapper, make_ycp_engine


class Error(RuntimeError):
    pass


_DEFAULT_TESTS_FOLDER_ID = "nbs.tests.folder"


class TestRunner:
    __DEFAULT_ZONE_ID = 'ru-central1-a'

    def __init__(self, module_factories, args, logger):
        self.module_factories = module_factories
        self.args = args
        self.logger = logger
        self.helpers = module_factories.make_helpers(self.args.dry_run)
        self.instance = None
        self.subnet = None

        cluster = get_cluster_test_config(args.cluster, self.__DEFAULT_ZONE_ID, self.args.cluster_config_path)
        folder_desc = cluster.folder_desc[args.folder_id]

        self.logger.info(f'Create ycp wrapper: {args.profile_name} {folder_desc.folder_id}')

        self.ycp = YcpWrapper(
            args.profile_name,
            folder_desc,
            logger,
            make_ycp_engine(args.dry_run),
            self.module_factories.make_config_generator(args.dry_run),
            self.helpers,
            args.generate_ycp_config,
            args.ycp_requests_template_path,
        )

        self.instance = self.find_instance(self.args.instance_name)
        self.logger.info(f'Current zone_id: {self.instance.zone_id}')

        zone_idx = self.args.zone_id_list.index(self.instance.zone_id)
        zone_idx = (zone_idx + 1) % len(self.args.zone_id_list)

        cluster = get_cluster_test_config(args.cluster, self.args.zone_id_list[zone_idx], self.args.cluster_config_path)
        folder_desc = cluster.folder_desc[args.folder_id]

        self.subnet = self.find_subnet(folder_desc.subnet_name)
        self.logger.info(f'Target zone_id is {self.subnet.zone_id}, subnet <id={self.subnet.id}>')
        self.logger.info(f'Target subnet: {vars(self.subnet)}')

    @common.retry(tries=5, delay=10, exception=Error)
    def check_load(self):
        self.logger.info(f'Check if eternal load running on instance with ip=<{self.instance.ip}>')
        with self.module_factories.make_ssh_client(self.args.dry_run, self.instance.ip, None) as ssh:
            command = 'systemctl list-units --no-legend --output=json --full --all "eternalload_vd*.service"'
            _, stdout, _ = ssh.exec_command(command)

            systemctl_output = stdout.read().decode('utf-8')
            if not systemctl_output:
                if self.args.dry_run:
                    return
                raise Error('No eternal-load services on instance')

            for service in json.loads(systemctl_output):
                unit = service["unit"]
                if service['active'] != 'active':
                    raise Error(f'Eternal load not running on instance with ip=<{self.instance.ip}>, unit=<{unit}>')
                else:
                    self.logger.info(f'Service {unit} is active.')

    def find_instance(self, instance_name):
        instances = self.ycp.list_instances()
        for instance in instances:
            if instance.name == instance_name:
                return instance

        raise RuntimeError(f"instance {instance_name} not found out of {len(instances)}")

    def find_subnet(self, subnet_name):
        subnets = self.ycp.list_subnets()
        for subnet in subnets:
            if subnet.name == subnet_name:
                return subnet

        raise RuntimeError(f"subnet {subnet_name} not found out of {len(subnets)}")

    def run(self):
        self.logger.info('Test started.')

        if self.args.check_load:
            self.check_load()

        self.ycp.relocate_instance(self.instance, self.subnet)
        self.instance = self.find_instance(self.args.instance_name)

        if self.args.check_load:
            self.check_load()

        self.logger.info('Vm relocated, eternal-load works.')


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--teamcity', action='store_true', help='use teamcity logging format')
    parser.add_argument('--verbose', action='store_true', help='enables debug logging level')
    parser.add_argument('--quite', action='store_true', help='log only critical events')

    test_arguments_group = parser.add_argument_group('test arguments')
    common.add_common_parser_arguments(test_arguments_group)
    test_arguments_group.add_argument(
        '--folder-id',
        type=str,
        default=_DEFAULT_TESTS_FOLDER_ID,
        help='folder id')
    test_arguments_group.add_argument(
        '--instance-name',
        type=str,
        required=True,
        help='run relocation test for specified instance')
    test_arguments_group.add_argument(
        '--zone-id-list',
        type=str,
        required=True,
        help='relocate to next zone-id in this comma-separated circular list')
    test_arguments_group.add_argument(
        '--check-load',
        action='store_true',
        help='if set test will check that eternal load on specified instance not fails after relocation')

    args = parser.parse_args()
    if args.profile_name is None:
        args.profile_name = args.cluster

    args.zone_id_list = args.zone_id_list.split(',')

    return args
