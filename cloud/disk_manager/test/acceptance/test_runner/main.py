import argparse
import logging
import sys

from .acceptance_test_runner import AcceptanceTestRunner
from .base_acceptance_test_runner import BaseAcceptanceTestRunner
from .eternal_acceptance_test_runner import EternalAcceptanceTestRunner
from .lib import (
    Error,
    ResourceExhaustedError,
)
from .sync_acceptance_test_runner import SyncAcceptanceTestRunner

from cloud.blockstore.pylibs import common


_DEFAULT_DISK_BLOCKSIZE = 4096
_DEFAULT_DISK_TYPE = 'network-ssd'
_DEFAULT_WRITE_SIZE_PERCENTAGE = 100
_DEFAULT_INSTANCE_CORES = 8
_DEFAULT_INSTANCE_RAM = 8
_DEFAULT_CHUNK_STORAGE_TYPE = 'ydb'
_DEFAULT_PLATFORM_ID = 'standard-v2'
_DEFAULT_ZONE_ID = 'ru-central1-a'


_logger = logging.getLogger(__file__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser('PROG')

    verbose_quite_group = parser.add_mutually_exclusive_group()
    verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
    verbose_quite_group.add_argument('-q', '--quite', action='store_true')

    parser.add_argument(
        '--teamcity',
        action='store_true',
        help='use teamcity logging format')

    common.add_common_parser_arguments(parser)

    subparsers = parser.add_subparsers(dest='test_type', help='test types')

    # acceptance test type stuff
    acceptance_test_type_parser = subparsers.add_parser(
        'acceptance',
        help='will create acceptance test suite for single iteration')
    acceptance_test_type_parser.add_argument(
        '--test-suite',
        type=str,
        required=True,
        help='run specified test suite from {arc_root}/cloud/disk_manager/test'
             '/acceptance/test_runner/lib/test_cases.py')
    acceptance_test_type_parser.add_argument(
        '--verify-test',
        type=str,
        required=True,
        help='path to verify-test binary')
    acceptance_test_type_parser.add_argument(
        '--s3-host',
        type=str,
        dest='s3_host',
        default=None,
        help='url for s3 to store images',
    )
    acceptance_test_type_parser.add_argument(
        '--bucket-location',
        type=str,
        dest='bucket_location',
        default=None,
        help='s3 bucket location',
    )

    # eternal test type stuff
    eternal_test_type_parser = subparsers.add_parser(
        'eternal',
        help='will perform acceptance test on single disk'
             ' with complete content checking')
    eternal_test_type_parser.add_argument(
        '--disk-size',
        type=int,
        required=True,
        help='disk size in GiB')
    eternal_test_type_parser.add_argument(
        '--disk-blocksize',
        type=int,
        default=_DEFAULT_DISK_BLOCKSIZE,
        help=f'disk blocksize in bytes (default is {_DEFAULT_DISK_BLOCKSIZE}')
    eternal_test_type_parser.add_argument(
        '--disk-type',
        type=str,
        default=_DEFAULT_DISK_TYPE,
        help=f'disk type (default is {_DEFAULT_DISK_TYPE}')
    eternal_test_type_parser.add_argument(
        '--disk-write-size-percentage',
        type=int,
        default=_DEFAULT_WRITE_SIZE_PERCENTAGE,
        help=f'verification write size percentage (default is'
             f' {_DEFAULT_WRITE_SIZE_PERCENTAGE}')
    eternal_test_type_parser.add_argument(
        '--cmp-util',
        type=str,
        required=True,
        help='path to cmp-util binary')

    # sync test type stuff
    sync_test_type_parser = subparsers.add_parser(
        'sync',
        help='will create an instance, network-ssd disk with ext4'
             ' then write some files, sync them, create a snapshot,'
             ' restore from this snapshot, and check that file contents are'
             ' the same')
    sync_test_type_parser.add_argument(
        '--disk-size',
        type=int,
        required=True,
        help='disk size in GiB')
    sync_test_type_parser.add_argument(
        '--disk-blocksize',
        type=int,
        default=_DEFAULT_DISK_BLOCKSIZE,
        help=f'disk blocksize in bytes (default is {_DEFAULT_DISK_BLOCKSIZE}')
    sync_test_type_parser.add_argument(
        '--disk-type',
        type=str,
        default=_DEFAULT_DISK_TYPE,
        help=f'disk type (default is {_DEFAULT_DISK_TYPE}')
    sync_test_type_parser.add_argument(
        '--disk-write-size-percentage',
        type=int,
        default=_DEFAULT_WRITE_SIZE_PERCENTAGE,
        help=f'verification write size percentage (default is'
             f' {_DEFAULT_WRITE_SIZE_PERCENTAGE}')

    # common stuff
    test_arguments_group = parser.add_argument_group('common arguments')
    test_arguments_group.add_argument(
        '--acceptance-test',
        type=str,
        required=True,
        help='path to acceptance-test binary')
    test_arguments_group.add_argument(
        '--results-path',
        type=str,
        help='specify path to test results')
    test_arguments_group.add_argument(
        '--conserve-snapshots',
        action='store_true',
        default=False,
        help='do not delete snapshot after acceptance test')
    test_arguments_group.add_argument(
        '--chunk-storage-type',
        type=str,
        default=_DEFAULT_CHUNK_STORAGE_TYPE,
        help=f'use specified chunk storage type (default is'
             f'{_DEFAULT_CHUNK_STORAGE_TYPE})')
    test_arguments_group.add_argument(
        '--placement-group-name',
        type=str,
        default=None,
        help='create vm with specified placement group')
    test_arguments_group.add_argument(
        '--compute-node',
        type=str,
        default=None,
        help='run acceptance test on specified compute node')
    test_arguments_group.add_argument(
        '--zone-ids',
        nargs='*',
        type=str,
        default=[_DEFAULT_ZONE_ID],
        help=f'list of possible zones (default is {_DEFAULT_ZONE_ID})')
    test_arguments_group.add_argument(
        '--instance-cores',
        type=int,
        default=_DEFAULT_INSTANCE_CORES,
        help=f'cores for created vm (default is {_DEFAULT_INSTANCE_CORES})')
    test_arguments_group.add_argument(
        '--instance-ram',
        type=int,
        default=_DEFAULT_INSTANCE_RAM,
        help=f'RAM for created vm (in GiB)'
             f' (default is {_DEFAULT_INSTANCE_RAM})')
    test_arguments_group.add_argument(
        '--instance-platform-ids',
        nargs='*',
        type=str,
        default=[_DEFAULT_PLATFORM_ID],
        help=f'List of possible platforms (default is {_DEFAULT_PLATFORM_ID})')
    test_arguments_group.add_argument(
        '--debug',
        action='store_true',
        default=False,
        help='do not delete instance and disk, if fail')
    test_arguments_group.add_argument(
        '--cleanup-before-tests',
        dest='cleanup_before_tests',
        action='store_true',
        default=False,
        help='Clean up outdated resources in place')
    test_arguments_group.add_argument(
        '--skip-images',
        action='store_true',
        help='will skip creation of images and creation of disks from images')
    args = parser.parse_args()

    if args.profile_name is None:
        args.profile_name = args.cluster

    return args


def fetch_test_runner(
    args: argparse.Namespace,
    module_factory: common.ModuleFactories,
) -> BaseAcceptanceTestRunner | None:
    if args.test_type == 'acceptance':
        return AcceptanceTestRunner(args, module_factory)
    elif args.test_type == 'eternal':
        return EternalAcceptanceTestRunner(args, module_factory)
    elif args.test_type == 'sync':
        return SyncAcceptanceTestRunner(args, module_factory)
    _logger.fatal(f'Failed to run test suite {args.test_type}:'
                  f' No such test type')
    return None


def run_acceptance_test(
    zone_id: str,
    args: argparse.Namespace,
    module_factory: common.ModuleFactories,
):
    args.zone_id = zone_id
    test_runner = fetch_test_runner(args, module_factory)
    if test_runner is None:
        raise RuntimeError("Test runner not found")
    with common.make_profiler(_logger, not args.debug) as profiler:
        try:
            test_runner.run(profiler)
            return
        except ResourceExhaustedError as e:
            # If ResourceExhaustedError occurred try another zone.
            _logger.fatal(f'Failed to run test suite: {e}')
            raise
        except Error as e:
            # If Error occurred finish test with non-zero code.
            _logger.fatal(f'Failed to run test suite: {e}')
            raise RuntimeError("Test suite run failed")


def run_acceptance_tests(module_factory: common.ModuleFactories):
    args = parse_args()
    common.setup_global_logger(args)

    for zone_id in args.zone_ids:
        try:
            run_acceptance_test(zone_id, args, module_factory)
            return
        except RuntimeError:
            sys.exit(1)
        except ResourceExhaustedError:
            continue
    sys.exit(1)
