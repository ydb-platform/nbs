import argparse
from datetime import datetime
import socket

from .test_cases import generate_test_cases, TestCase
from .errors import Error

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config, translate_disk_type
from cloud.blockstore.pylibs.ycp import Ycp, YcpWrapper, make_ycp_engine


_VERIFY_TEST_REMOTE_PATH = '/usr/bin/verify-test'
_TEST_INSTANCE_CORES = 4
_TEST_INSTANCE_MEMORY = 16  # 64MB-bs test suite requires at least 8GB RAM
_DEFAULT_ZONE_ID = 'ru-central1-b'
_NFS_DEVICE = 'nfs'
_NFS_MOUNT_PATH = '/test'
_NFS_TEST_FILE = '/test/test.txt'
_NBS_MOUNT_PATH = '/dev/vdb'


def parse_args():
    parser = argparse.ArgumentParser()

    verbose_quite_group = parser.add_mutually_exclusive_group()
    verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
    verbose_quite_group.add_argument('-q', '--quite', action='store_true')

    parser.add_argument('--teamcity', action='store_true', help='use teamcity logging format')

    test_arguments_group = parser.add_argument_group('test arguments')
    common.add_common_parser_arguments(test_arguments_group)
    test_arguments_group.add_argument(
        '--test-suite',
        type=str,
        required=True,
        help='run the specified test suite')
    test_arguments_group.add_argument(
        '--ipc-type',
        type=str,
        default='grpc',
        help='use the specified ipc type')
    test_arguments_group.add_argument(
        '--verify-test-path',
        type=str,
        required=True,
        help='path to verify-test tool, built from arcadia cloud/blockstore/tools/testing/verify-test')
    test_arguments_group.add_argument(
        '--compute-node',
        type=str,
        default=None,
        help='run test at the specified compute node')
    test_arguments_group.add_argument(
        '--compute-nodes-list-path',
        type=str,
        default=None,
        help='Path to the file containing the list of compute nodes. '
             'One of these node will be chosen to run the test on. '
             'File format: newline separated list of compute nodes FQDNs.')
    test_arguments_group.add_argument(
        '--zone-id',
        type=str,
        default=_DEFAULT_ZONE_ID,
        help=f'specify zone id, default is {_DEFAULT_ZONE_ID}'
    )
    test_arguments_group.add_argument(
        '--service',
        choices=['nfs', 'nbs'],
        default='nbs',
        help='specify to use nbs or nfs for test'
    )
    test_arguments_group.add_argument(
        '--ttl-instance-days',
        type=int,
        default=7,
        help='ttl for tmp instances'
    )
    test_arguments_group.add_argument(
        '--debug',
        action='store_true',
        default=False,
        help='do not delete instance and disk, if fail')
    test_arguments_group.add_argument(
        '--results-path',
        type=str,
        help='specify path to test results')

    return parser.parse_args()


def _verify_cmd(test_case: TestCase, service: str) -> str:
    file: str = _NBS_MOUNT_PATH if service == 'nbs' else _NFS_TEST_FILE
    return f'{_VERIFY_TEST_REMOTE_PATH} {test_case.verify_test_cmd_args} --file {file} 2>&1'


def _run_test_case(
    module_factories: common.ModuleFactories,
    ycp: YcpWrapper,
    test_case: TestCase,
    instance: Ycp.Instance,
    disk_type: str,
    args,
    logger,
    results_processor
):

    logger.info(f'Running verify-test at instance <id={instance.id}>')
    exception = None
    with module_factories.make_ssh_client(args.dry_run, instance.ip, ssh_key_path=args.ssh_key_path) as ssh:
        _, stdout, _ = ssh.exec_command(_verify_cmd(test_case, args.service))
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            logger.error(f'Failed to execute verify-test with exit code {exit_code}:\n'
                         f'{"".join(stdout.readlines())}')
            if args.debug:
                ycp.turn_off_auto_deletion()
            exception = Error(f'verify-test execution failed with exit code {exit_code}')
        else:
            logger.info(f'{"".join(stdout.readlines())}')

    if results_processor is not None:
        if exception is not None:
            logger.error(exception)
        results_processor.publish_test_report_base(
            instance.compute_node,
            instance.id,
            test_case.size,
            disk_type,
            4096,  # disk_bs
            {},  # extra_params
            test_case.name,
            exception)
    elif exception is not None:
        raise exception


def _mount_fs(
    module_factories: common.ModuleFactories,
    ycp: YcpWrapper,
    instance_ip: str,
    dry_run: bool,
    args,
    logger
):
    logger.info('Mounting fs')
    with (module_factories.make_ssh_client(dry_run, instance_ip, ssh_key_path=args.ssh_key_path) as ssh,
          module_factories.make_sftp_client(dry_run, instance_ip, ssh_key_path=args.ssh_key_path) as sftp):
        sftp.mkdir(_NFS_MOUNT_PATH)
        _, _, stderr = ssh.exec_command(f'mount -t virtiofs {_NFS_DEVICE} {_NFS_MOUNT_PATH} && '
                                        f'touch {_NFS_TEST_FILE}')
        exit_code = stderr.channel.recv_exit_status()
        if exit_code != 0:
            logger.error(f'Failed to mount fs\n'
                         f'{"".join(stderr.readlines())}')
            if args.debug:
                ycp.turn_off_auto_deletion()
            raise Error(f'failed to mount fs with exit code {exit_code}')


def _read_compute_nodes_list(args, logger):
    if args.compute_nodes_list_path is None:
        return None

    if args.compute_node is not None:
        raise Error('--compute-node and --compute-nodes-list-path parameters '
                    'should not be specified simultaneously')

    with open(args.compute_nodes_list_path, 'r') as f:
        return [line.rstrip('\n') for line in f]


def run_corruption_test(module_factories: common.ModuleFactories, args, logger):
    cluster = get_cluster_test_config(args.cluster, args.zone_id, args.cluster_config_path)
    logger.info(f'Running corruption test suite at cluster <{cluster.name}>')

    test_cases = generate_test_cases(args.test_suite, args.cluster, args.service)
    logger.info(f'Generated {len(test_cases)} test cases for test suite <{args.test_suite}>')

    folder = cluster.ipc_type_to_folder_desc(args.ipc_type)

    compute_nodes_list = _read_compute_nodes_list(args, logger)

    helpers = module_factories.make_helpers(args.dry_run)
    ycp_config_generator = None
    if args.generate_ycp_config:
        ycp_config_generator = module_factories.make_config_generator(args.dry_run)
    ycp = YcpWrapper(
        args.profile_name or cluster.name,
        folder,
        logger,
        make_ycp_engine(args.dry_run),
        ycp_config_generator,
        helpers,
        args.generate_ycp_config,
        args.ycp_requests_template_path)

    results_processor = None
    if args.results_path is not None:
        results_processor = common.ResultsProcessorFsBase(
            args.service,
            args.test_suite,
            cluster.name,
            datetime.today().strftime('%Y-%m-%d'),
            args.results_path)

    ycp.delete_tmp_instances(args.ttl_instance_days)

    with ycp.create_instance(
            cores=_TEST_INSTANCE_CORES,
            memory=_TEST_INSTANCE_MEMORY,
            compute_node=args.compute_node,
            compute_nodes_list=compute_nodes_list,
            image_name=folder.image_name,
            image_folder_id=folder.image_folder_id,
            description="Corruption test") as instance:

        server_version = module_factories.fetch_server_version(
            args.dry_run,
            f'get_current_{args.service}_version',
            instance.compute_node,
            cluster,
            logger)
        logger.info(f'Compute node: {instance.compute_node}; server version: {server_version}')

        logger.info(f'Waiting until instance <id={instance.id}> becomes available via ssh')
        try:
            helpers.wait_until_instance_becomes_available_via_ssh(instance.ip, ssh_key_path=args.ssh_key_path)
        except (common.SshException, socket.error) as e:
            if args.debug:
                ycp.turn_off_auto_deletion()
            raise Error(f'failed to start test, problem with'
                        f' ssh connection: {e}')

        logger.info(f'Copying verify-test to instance <id={instance.id}>')
        with module_factories.make_sftp_client(args.dry_run, instance.ip, ssh_key_path=args.ssh_key_path) as sftp:
            sftp.put(args.verify_test_path, _VERIFY_TEST_REMOTE_PATH)
            sftp.chmod(_VERIFY_TEST_REMOTE_PATH, 0o755)

        for test_case in test_cases:
            logger.info(f'Executing test case <{test_case.name}>')

            disk_type = test_case.type

            if args.service == 'nbs':
                disk_type = translate_disk_type(args.cluster, test_case.type)

                with ycp.create_disk(
                        size=test_case.size,
                        type_id=disk_type,
                        description=f"Corruption test: {test_case.name}") as disk:

                    with ycp.attach_disk(instance, disk):
                        logger.info(f'Waiting until secondary disk appears'
                                    f' as a block device at instance <id={instance.id}>')
                        helpers.wait_for_block_device_to_appear(instance.ip, _NBS_MOUNT_PATH, ssh_key_path=args.ssh_key_path)
                        _run_test_case(
                            module_factories,
                            ycp,
                            test_case,
                            instance,
                            disk_type,
                            args,
                            logger,
                            results_processor)
            else:
                with ycp.create_fs(size=test_case.size, type_id=disk_type) as fs:
                    with ycp.attach_fs(instance, fs, _NFS_DEVICE):
                        _mount_fs(
                            module_factories,
                            ycp,
                            instance.ip,
                            args.dry_run,
                            args, logger)
                        _run_test_case(
                            module_factories,
                            ycp,
                            test_case,
                            instance,
                            disk_type,
                            args,
                            logger,
                            results_processor)
