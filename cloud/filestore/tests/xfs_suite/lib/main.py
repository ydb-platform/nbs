import argparse
import enum
import string

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config
from cloud.blockstore.pylibs.ycp import Ycp, YcpWrapper, make_ycp_engine

from contextlib import contextmanager
from library.python import resource

_DEFAULT_IMAGE_NAME = 'ubuntu-2204-eternal'
_DEFAULT_INSTANCE_CORES = 4
_DEFAULT_INSTANCE_MEMORY = 16  # GiB

_DEFAULT_TEST_DEVICE_SIZE = 50  # GiB
_DEFAULT_TEST_DEVICE_TYPE = 'network-ssd'

_DEFAULT_SCRATCH_DEVICE_SIZE = 50  # GiB
_DEFAULT_SCRATCH_DEVICE_TYPE = 'network-ssd'

_DEFAULT_SCRIPT_NAME = 'default.sh'


class Error(Exception):
    pass


class DeviceType(enum.Enum):
    virtiofs = 'virtiofs'
    # TODO: block

    def __str__(self):
        return self.value

    def test_name(self):
        return f'{self.value}-test'

    def scratch_name(self):
        return f'{self.value}-scratch'


def parse_args():
    parser = argparse.ArgumentParser()

    verbose_quite_group = parser.add_mutually_exclusive_group()
    verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
    verbose_quite_group.add_argument('-q', '--quite', action='store_true')

    parser.add_argument(
        '--teamcity',
        action='store_true',
        help='use teamcity logging format')

    test_arguments_group = parser.add_argument_group('test arguments')

    # common test parameters
    common.add_common_parser_arguments(test_arguments_group)
    test_arguments_group.add_argument(
        '--compute-node',
        type=str,
        default=None,
        help='run test at the specified compute node')
    test_arguments_group.add_argument(
        '--zone-id',
        type=str,
        required=True)
    test_arguments_group.add_argument(
        '--debug',
        action='store_true',
        help='dont delete instance if test fail')

    # instance parameters
    test_arguments_group.add_argument(
        '--image-name',
        type=str,
        default=_DEFAULT_IMAGE_NAME)
    test_arguments_group.add_argument(
        '--image-folder-id',
        type=str,
        default=None,
        help='use image from the specified folder to create disks')
    test_arguments_group.add_argument(
        '--cores',
        type=int,
        default=_DEFAULT_INSTANCE_CORES)
    test_arguments_group.add_argument(
        '--memory',
        type=int,
        default=_DEFAULT_INSTANCE_MEMORY)

    # test device and directory parameters
    test_arguments_group.add_argument(
        '--test-type',
        type=DeviceType,
        choices=list(DeviceType),
        required=True)
    test_arguments_group.add_argument(
        '--test-device',
        type=str,
        required=True)
    test_arguments_group.add_argument(
        '--test-dir',
        type=str,
        required=True)
    test_arguments_group.add_argument(
        '--test-device-size',
        type=int,
        default=_DEFAULT_TEST_DEVICE_SIZE)
    test_arguments_group.add_argument(
        '--test-device-type',
        type=str,
        default=_DEFAULT_TEST_DEVICE_TYPE)

    # scratch device and directory parameters
    test_arguments_group.add_argument(
        '--scratch-type',
        type=DeviceType,
        choices=list(DeviceType),
        required=True)
    test_arguments_group.add_argument(
        '--scratch-device',
        type=str,
        required=True)
    test_arguments_group.add_argument(
        '--scratch-dir',
        type=str,
        required=True)
    test_arguments_group.add_argument(
        '--scratch-device-size',
        type=int,
        default=_DEFAULT_SCRATCH_DEVICE_SIZE)
    test_arguments_group.add_argument(
        '--scratch-device-type',
        type=str,
        default=_DEFAULT_SCRATCH_DEVICE_TYPE)

    # script parameters
    test_arguments_group.add_argument(
        '--script-name',
        type=str,
        default=_DEFAULT_SCRIPT_NAME)

    return parser


@contextmanager
def create_device(
    ycp: YcpWrapper,
    instance: Ycp.Instance,
    device_type: str,
    device_size: int,
    device_name: str,
    type_id: DeviceType,
    logger
):
    if str(type_id) == 'virtiofs':
        logger.info(f'creating fs {device_name}')
        with ycp.create_fs(size=device_size, type_id=device_type) as fs:
            logger.info(f'attaching fs {device_name} to instance')
            with ycp.attach_fs(instance, fs, device_name):
                yield fs
    else:
        raise Error('Wrong device type')


def _run_script(
    module_factories,
    args,
    instance_ip: str,
    script_name: str,
    script_path: str,
    logger
):
    logger.info(f'Add variables to {script_name} template')
    template = string.Template(resource.find(f'{script_name}').decode('utf8'))
    script = template.substitute(
        testPath=args.test_dir,
        scratchPath=args.scratch_dir,
        testDevice=args.test_type.test_name(),
        scratchDevice=args.scratch_type.scratch_name(),
        fsType=str(args.test_type)
    )

    logger.info(f'Copying {script_name} to {script_path} on instance')
    with module_factories.make_sftp_client(args.dry_run, instance_ip) as sftp:
        file = sftp.file(script_path, 'w')
        file.write(script)
        file.flush()
        sftp.chmod(script_path, 0o755)

    logger.info(f'Running {script_name} on instance')
    with module_factories.make_ssh_client(args.dry_run, instance_ip) as ssh:
        _, stdout, stderr = ssh.exec_command(f'{script_path}')
        if not args.dry_run:
            for line in iter(lambda: stdout.readline(2048), ''):
                logger.info(line.rstrip())
        exit_code = stderr.channel.recv_exit_status()
        if exit_code != 0:
            logger.error(f'Failed to execute script {script_path} on remote host'
                         f' {instance_ip}: {"".join(stderr.readlines())}')
            raise Error('Failed to run command')


def run(module_factories, parser, args, logger):
    cluster = get_cluster_test_config(args.cluster, args.zone_id, args.cluster_config_path)
    logger.info(f'Running xfs test suite at cluster <{cluster.name}>')

    helpers = module_factories.make_helpers(args.dry_run)
    ycp = YcpWrapper(
        args.profile_name or cluster.name,
        cluster.ipc_type_to_folder_desc('grpc'),
        logger,
        make_ycp_engine(args.dry_run),
        module_factories.make_config_generator(args.dry_run),
        helpers,
        args.generate_ycp_config,
        args.ycp_requests_template_path)
    with ycp.create_instance(
            cores=args.cores,
            memory=args.memory,
            compute_node=args.compute_node,
            image_name=args.image_name,
            image_folder_id=args.image_folder_id) as instance:
        logger.info(
            f'Waiting until instance <id={instance.id}> becomes available via ssh')
        helpers.wait_until_instance_becomes_available_via_ssh(instance.ip)

        with create_device(
            ycp,
            instance,
            args.test_device_type,
            args.test_device_size,
            args.test_type.test_name(),
            args.test_type,
            logger
        ):
            with create_device(
                ycp,
                instance,
                args.scratch_device_type,
                args.scratch_device_size,
                args.scratch_type.scratch_name(),
                args.scratch_type,
                logger
            ):
                _run_script(
                    module_factories,
                    args,
                    instance.ip,
                    args.script_name,
                    f'/root/{args.script_name}',
                    logger)
