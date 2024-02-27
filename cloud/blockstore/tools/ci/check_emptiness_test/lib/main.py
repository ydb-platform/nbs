from concurrent.futures.thread import ThreadPoolExecutor

import argparse
from datetime import datetime
import os
import socket
import threading

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config, translate_disk_type
from cloud.blockstore.pylibs.ycp import YcpWrapper, make_ycp_engine


_VERIFY_TEST_REMOTE_PATH = '/usr/bin/verify-test'


class Error(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser()

    verbose_quite_group = parser.add_mutually_exclusive_group()
    verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
    verbose_quite_group.add_argument('-q', '--quite', action='store_true')

    parser.add_argument('--teamcity', action='store_true', help='use teamcity logging format')

    test_arguments_group = parser.add_argument_group('test arguments')
    common.add_common_parser_arguments(test_arguments_group)
    test_arguments_group.add_argument(
        '--zones',
        nargs='+',
        required=True,
        help='run test in specified zones of cluster')
    test_arguments_group.add_argument(
        '--verify-test-path',
        type=str,
        required=True,
        help='path to verify-test tool, built from arcadia cloud/blockstore/tools/testing/verify-test',
    )
    test_arguments_group.add_argument(
        '--disk-size',
        type=int,
        default=1023,
        help='specify disk size in GB',
    )
    test_arguments_group.add_argument(
        '--io-depth',
        type=int,
        default=128,
        help='specify io depth',
    )
    test_arguments_group.add_argument(
        '--compute-node',
        type=str,
        default=None,
        help='run fio test on specified node',
    )
    test_arguments_group.add_argument(
        '--host-group',
        type=str,
        default=None,
        help='specify host group for instance creation',
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
        help='do not delete instance and disk, if this test fails',
    )
    test_arguments_group.add_argument(
        '--results-path',
        type=str,
        help='specify path to test results',
    )

    return parser.parse_args()


def _scan_disk(
        module_factories: common.ModuleFactories,
        task_id: int,
        args,
        zone_id: str,
        type_id: str,
        should_stop) -> str:
    logger = common.create_logger(f"#{task_id}/{zone_id}", args)

    cluster = get_cluster_test_config(args.cluster, zone_id, args.cluster_config_path)
    # creating a small vm by default so that our disk access gets throttled
    # at the client level - otherwise this test might create too much stress
    # on blockstore-server
    cores = int(os.getenv("TEST_VM_CORES", "2"))
    memory = int(os.getenv("TEST_VM_RAM", "8"))

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

    results_processor = None
    if args.results_path is not None:
        results_processor = common.ResultsProcessorFsBase(
            "nbs",  # service
            "emptiness_%s" % zone_id,
            cluster.name,
            datetime.today().strftime('%Y-%m-%d'),
            args.results_path)

    ycp.delete_tmp_instances(args.ttl_instance_days)

    with ycp.create_instance(
            cores,
            memory,
            compute_node=args.compute_node,
            host_group=args.host_group,
            description="Check emptiness") as instance:

        with ycp.create_disk(
                size=args.disk_size,
                type_id=type_id,
                description="Check emptiness") as disk:

            logger.info(
                f'Waiting until instance <id={instance.id}> becomes available'
                ' via ssh')

            try:
                helpers.wait_until_instance_becomes_available_via_ssh(instance.ip, ssh_key_path=args.ssh_key_path)
            except (common.SshException, socket.error) as e:
                if args.debug:
                    ycp.turn_off_auto_deletion()
                raise Error(f'failed to start test, instance not reachable'
                            f' via ssh: {e}')

            with ycp.attach_disk(instance, disk):
                logger.info(f'Copying verify-test to instance <id={instance.id}>')
                with module_factories.make_sftp_client(args.dry_run, instance.ip, ssh_key_path=args.ssh_key_path) as sftp:
                    sftp.put(args.verify_test_path, _VERIFY_TEST_REMOTE_PATH)
                    sftp.chmod(_VERIFY_TEST_REMOTE_PATH, 0o755)

                device_name = f'/dev/disk/by-id/virtio-{disk.id}'

                logger.info(
                    f'Waiting until secondary disk <id={disk.id}> appears as '
                    f'block device "{device_name}" on instance <id={instance.id}>')

                helpers.wait_for_block_device_to_appear(instance.ip, device_name, ssh_key_path=args.ssh_key_path)

                logger.info(f'Scanning disk with <id={disk.id}>')
                with module_factories.make_ssh_client(args.dry_run, instance.ip, ssh_key_path=args.ssh_key_path) as ssh:

                    _, stdout, _ = ssh.exec_command(
                        f'{_VERIFY_TEST_REMOTE_PATH}'
                        f' --filesize {args.disk_size * 1024 ** 3}'
                        f' --file {device_name}'
                        f' --iodepth {args.io_depth}'
                        ' --zero-check --blocksize 4194304 2>&1')

                    while not should_stop.is_set() and not stdout.channel.exit_status_ready():
                        pass

                    exception = None
                    if stdout.channel.recv_exit_status():
                        stdout_str = ''.join(stdout.readlines())
                        should_stop.set()
                        ycp.turn_off_auto_deletion()
                        exception = Error(f'failed to scan disk on instance <id={instance.id} with'
                                          f' disk <id={disk.id}>\n{stdout_str}>')

                    if results_processor is not None:
                        results_processor.publish_test_report_base(
                            instance.compute_node,
                            instance.id,
                            args.disk_size,
                            type_id,
                            4096,  # disk_bs
                            {},  # extra_params
                            "default",  # test_case_name
                            exception)
                    elif exception is not None:
                        raise exception

                    return (f'Successfully finished in zone <id={zone_id}>.\n'
                            f'instance <id={instance.id}>, disk <id={disk.id}>')


def run_test_suite(module_factories, args, logger):
    types = [
        translate_disk_type(args.cluster, 'network-ssd-nonreplicated'),
        # clusters other than prod are too small for m3
        translate_disk_type(args.cluster, 'network-ssd-io-m3' if args.cluster == 'prod' else 'network-ssd-io-m2'),
    ]

    task_num = 0
    with ThreadPoolExecutor(max_workers=len(args.zones)) as executor:
        futures = []
        should_stop = threading.Event()
        try:
            for type_id in types:
                for zone_id in args.zones:
                    logger.info(f'Run test #{task_num} in {zone_id} for {type_id}')

            for type_id in types:
                for zone_id in args.zones:
                    futures.append(executor.submit(
                        _scan_disk,
                        module_factories,
                        task_num,
                        args,
                        zone_id,
                        type_id,
                        should_stop))
                    task_num += 1
        finally:
            should_stop.set()
            logger.info(f'Waiting for {len(futures)} tests...')
            results = []
            # collecting all results first to avoid races in output between the
            # code in _scan_disk and this code
            for future in futures:
                results.append(future.result())

            for result in results:
                logger.info(result)
