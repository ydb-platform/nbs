import argparse
import contextlib
import logging
import math
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.ycp import YcpWrapper
from .base_acceptance_test_runner import BaseAcceptanceTestRunner, \
    BaseTestBinaryExecutor
from .cleanup import BaseResourceCleaner
from .lib import (
    check_ssh_connection,
    Error,
)

_logger = logging.getLogger(__file__)


def page_align(size, page_size=4096):
    return (int(size) // page_size) * page_size


class EternalAcceptanceTestBinaryExecutor(BaseTestBinaryExecutor):
    _entity_suffix = 'eternal'


class EternalTestCleaner(BaseResourceCleaner):
    def __init__(self, ycp: YcpWrapper, args: argparse.Namespace):
        super(EternalTestCleaner, self).__init__(ycp, args)
        test_type = args.test_type

        disk_name_pattern = re.compile(
            fr'^acc-{test_type}-'
            fr'{self._disk_parameters_string}-[0-9]+$',
        )
        old_disk_name_pattern = re.compile(
            fr'^acceptance-test-{test_type}-'
            fr'{self._disk_size}-'
            fr'{self._disk_blocksize}-[0-9]+$',
        )

        self._entity_ttls = self._entity_ttls | {
            'disk': timedelta(days=5),
        }
        self._patterns = {
            **self._patterns,
            'disk': [disk_name_pattern, old_disk_name_pattern],
        }


class EternalAcceptanceTestRunner(BaseAcceptanceTestRunner):
    _test_binary_executor_type = EternalAcceptanceTestBinaryExecutor
    _cleaner_type = EternalTestCleaner
    _single_disk_test_ttl = timedelta(days=5)

    @property
    def _remote_cmp_path(self) -> str:
        return '/usr/bin/acceptance-cmp'

    def _get_test_suite(self):
        return (
            f'{self._args.zone_id}_eternal_'
            f'{self._make_disk_parameters_string(delim="_")}'.lower()
        )

    def _report_compute_failure(self, error):
        if self._results_processor is not None:
            self._results_processor.publish_test_report_base(
                compute_node="FailedToGetInstance",
                id="FailedToGetInstanceId",
                disk_size=self._args.disk_size * (1024 ** 3),
                disk_type=self._args.disk_type,
                disk_bs=self._args.disk_blocksize,
                extra_params={},
                test_case_name=self._get_test_suite(),
                error=error,
            )

    def run(self, profiler: common.Profiler) -> None:
        self._initialize_run(
            profiler,
            f'acc-{self._args.test_type}-{self._timestamp}',
            'eternal',
        )

        with contextlib.ExitStack() as stack:
            instance = stack.enter_context(
                self.instance_policy_obtained(
                    self._report_compute_failure,
                ),
            )
            stack.enter_context(
                self._recording_result(
                    instance.compute_node,
                    instance.id,
                    self._args.disk_size * (1024 ** 3),
                    self._args.disk_type,
                    self._args.disk_blocksize,
                    {},
                    self._get_test_suite(),
                    )
            )
            # Copy cmp binary to instance
            _logger.info(f'Copying <path={self._args.cmp_util}> to'
                         f' <path={instance.ip}:'
                         f'{self._remote_cmp_path}>')
            self._scp_file(self._args.cmp_util,
                           self._remote_cmp_path,
                           instance.ip)

            disk_name_prefix = (
                f'acc-{self._args.test_type}-'
                f'{self._make_disk_parameters_string()}').lower()
            disk = self._find_or_create_eternal_disk(disk_name_prefix)

            _logger.info(
                f'Waiting until disk <id={disk.id}> will be attached'
                f' to instance <id={instance.id}> and secondary disk'
                f' appears as block device')

            primary_disk_path = stack.enter_context(
                self._instance_policy.attach_disk(disk)
            )
            runtime = math.ceil(
                (math.sqrt(0.476+1.24*self._args.disk_size)-0.69)/1.24)*60
            _logger.info(f'Approximate time <sec={runtime}>')

            percentage = min(
                100,
                max(0, self._args.disk_write_size_percentage))
            _logger.info(f'Disk filling <percent={percentage}>')

            self._perform_verification_write(
                f'fio --name=fill-disk --filename={primary_disk_path}'
                f' --rw=write --bsrange=1-64M --bs_unaligned'
                f' --iodepth={self._iodepth} --ioengine=libaio'
                f' --size={percentage}% --runtime={runtime}'
                f' --sync=1',
                disk,
                instance)

            # Perform acceptance test on current disk (detached after fio)
            disk_ids = self._perform_acceptance_test_on_single_disk(disk)

            _logger.info(
                f'Waiting until disk <id={disk.id}> will be attached'
                f' to instance <id={instance.id}> and secondary disk'
                f' appears as block device')

            for disk_id in disk_ids:
                output_disk = self._ycp.get_disk(disk_id)

                # Attach output disk
                _logger.info(
                    f'Waiting until disk <id={output_disk.id}> will be'
                    f' attached to instance <id={instance.id}> and'
                    f' secondary disk appears as block device')

                with contextlib.ExitStack() as inner_disk_exit_stack:
                    secondary_disk_path = inner_disk_exit_stack.enter_context(
                        self._instance_policy.attach_disk(output_disk),
                    )
                    disk_bytes_count = self._args.disk_size * (1024 ** 3)
                    chunk_size = 4 << 20
                    byte_count = page_align(disk_bytes_count / self._iodepth, page_size=chunk_size)
                    cmds = []
                    futures = []
                    executor = ThreadPoolExecutor(max_workers=self._iodepth)
                    for i in range(self._iodepth + 1):
                        offset = int(i * byte_count)
                        if offset >= disk_bytes_count:
                            break
                        count = min(byte_count, disk_bytes_count - offset)
                        check_ssh_connection(instance.ip, self._profiler, self._module_factory, self._args.ssh_key_path)
                        ssh = inner_disk_exit_stack.enter_context(
                            self._module_factory.make_ssh_client(
                                False,
                                instance.ip,
                                ssh_key_path=self._args.ssh_key_path,
                            ),
                        )

                        # non 4MB chunk size aligned remainder of disk
                        if count != byte_count:
                            chunk_size = 4096

                        cmd = (f'{self._remote_cmp_path} --verbose'
                               f' --bytes={count}'
                               f' --ignore-initial={offset}'
                               f' --chunk-size={chunk_size}'
                               f' {primary_disk_path}'
                               f' {secondary_disk_path}')
                        future = executor.submit(ssh.exec_command, cmd)
                        futures.append(future)
                        cmds.append(cmd)
                        _logger.info(f'Verifying data'
                                     f' <index={i}, offset={offset},'
                                     f' bytes={count}> on disk'
                                     f' <id={output_disk.id}> on'
                                     f' instance <id={instance.id}>')

                    error_message = ""
                    executor.shutdown(wait=True)
                    for i, future in enumerate(futures):
                        _, stdout, stderr = future.result()
                        stdout_str = stdout.read().decode('utf-8')
                        stderr_str = stderr.read().decode('utf-8')
                        _logger.info(f'Verifying finished <index={i},'
                                     f' command="{cmds[i]}", stdout='
                                     f'{stdout_str}, stderr='
                                     f'{stderr_str}>')
                        if stderr.channel.recv_exit_status():
                            error_message += (
                                f'Error <command="{cmds[i]}", stderr='
                                f'{stderr_str}>\n')

                    if len(error_message) != 0:
                        raise Error(error_message)

            # Delete all output disks
            self._delete_output_disks(disk_ids)
