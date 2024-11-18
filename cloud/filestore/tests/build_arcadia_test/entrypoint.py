import argparse
import sys
import typing as tp
from datetime import datetime

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import \
    get_cluster_test_config
from cloud.blockstore.pylibs.ycp import YcpWrapper, make_ycp_engine
from cloud.filestore.tests.build_arcadia_test.common import (
    Error, ResourceExhaustedError, create_instance)

DEFAULT_PLATFORM_ID = 'standard-v2'
DEFAULT_ZONE_ID = 'ru-central1-b'

_IMAGE_NAME = 'ubuntu-2204-eternal'
_TEST_INSTANCE_CORES = 16
_TEST_INSTANCE_MEMORY = 64


def _parse_args():
    parser = argparse.ArgumentParser()

    verbose_quite_group = parser.add_mutually_exclusive_group()
    verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
    verbose_quite_group.add_argument('-q', '--quite', action='store_true')

    parser.add_argument(
        '--teamcity',
        action='store_true',
        help='use teamcity logging format')

    test_arguments_group = parser.add_argument_group('test arguments')
    test_arguments_group.add_argument(
        '--compute-node',
        type=str,
        default=None,
        help='run test at the specified compute node')
    test_arguments_group.add_argument(
        '--image-name',
        type=str,
        default=_IMAGE_NAME,
        help='use image with the specified name to create disks')
    test_arguments_group.add_argument(
        '--image-folder-id',
        type=str,
        default=None,
        help='use image from the specified folder to create disks')
    test_arguments_group.add_argument(
        '--test-case',
        type=str,
        required=True,
        choices=['nbs', 'nfs-build', 'nfs-test', 'nfs-coreutils'])
    test_arguments_group.add_argument(
        '--platform-ids',
        nargs='*',
        type=str,
        default=[DEFAULT_PLATFORM_ID],
        help=f'list of possible platforms, default is {DEFAULT_PLATFORM_ID}')
    test_arguments_group.add_argument(
        '--zone-ids',
        nargs='*',
        type=str,
        default=[DEFAULT_ZONE_ID],
        help=f'list of possible zones, default is {DEFAULT_ZONE_ID}')
    test_arguments_group.add_argument(
        '--debug',
        action='store_true',
        help='dont delete instance if test fail')
    test_arguments_group.add_argument(
        '--reuse-fs-id',
        type=str,
        required=False,
        default=None,
        help='An existing FS id to reuse. If not set, a new one is created')
    common.add_common_parser_arguments(test_arguments_group)

    return parser


def _run_build_test(parser, args, logger, modes: tp.Dict[tp.Callable[..., tp.Any], None], module_factories):
    logger.info(
        f'Running build test suite with mode {args.test_case}, supported modes: {modes.keys()}')

    cluster = get_cluster_test_config(
        args.cluster, args.zone_id, args.cluster_config_path)
    logger.info(f'Running build test suite at cluster <{cluster.name}>')

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
    with create_instance(
            ycp,
            _TEST_INSTANCE_CORES,
            _TEST_INSTANCE_MEMORY,
            args.compute_node,
            args.image_name,
            args.image_folder_id,
            args.platform_ids,
            None if args.dry_run else f'build arcadia test at {datetime.now()}',
            logger) as instance:
        logger.info(
            f'Waiting until instance <id={instance.id}> becomes available via ssh')
        helpers.wait_until_instance_becomes_available_via_ssh(
            instance.ip, ssh_key_path=args.ssh_key_path)

        if args.test_case in modes:
            modes[args.test_case](ycp, parser, instance, args, logger, module_factories=module_factories)
        else:
            raise Error('Unknown test-case')


def main(modes: tp.Dict[tp.Callable[..., tp.Any], None], module_factories):
    parser = _parse_args()
    args = parser.parse_args()
    logger = common.create_logger('yc-nfs-ci-build-arcadia-test', args)

    for zone_id in args.zone_ids:
        args.zone_id = zone_id

        try:
            _run_build_test(parser, args, logger, modes, module_factories)
            return
        except ResourceExhaustedError as e:
            # If ResourceExhaustedError occurred try another zone.
            logger.fatal(f'Failed to run build trunk test: {e}')
        except (Error, YcpWrapper.Error) as e:
            # If Error occurred finish test with non-zero code.
            logger.fatal(f'Failed to run build trunk test: {e}')
            break

    sys.exit(1)
