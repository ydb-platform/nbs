import argparse
import logging
import re

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.ycp import YcpWrapper, Ycp
from .base_acceptance_test_runner import BaseAcceptanceTestRunner, \
    BaseTestBinaryExecutor, BaseResourceCleaner
from .lib import (
    generate_test_cases,
    YcpNewDiskPolicy,
    TestCase,
)

_logger = logging.getLogger(__file__)


class AcceptanceTestBinaryExecutor(BaseTestBinaryExecutor):
    _entity_suffix = 'acceptance'

    def __init__(self, args, *arguments, **kwargs):
        super(AcceptanceTestBinaryExecutor, self).__init__(args, *arguments, **kwargs)
        location = args.bucket_location or args.profile_name or args.cluster
        if not args.skip_images:
            self._acceptance_test_cmd.extend([
                '--url-for-create-image-from-url-test',
                f"https://{self._s3_host}/{location}.disk-manager/acceptance-tests/ubuntu-1604-ci-stable"])


class AcceptanceTestCleaner(BaseResourceCleaner):
    def __init__(self, ycp: YcpWrapper, args: argparse.Namespace):
        super(AcceptanceTestCleaner, self).__init__(ycp, args)
        _instance_name_pattern = re.compile(
            rf'^acc-{args.test_type}-'
            rf'{args.test_suite}-[0-9]+$',
        )
        _old_instance_name_pattern = re.compile(
            rf'^acceptance-test-{args.test_type}-'
            rf'{args.test_suite}-[0-9]+$',
        )
        self._patterns = {
            'instance': [_instance_name_pattern, _old_instance_name_pattern],
            'disk': [_instance_name_pattern, _old_instance_name_pattern],
        }


class AcceptanceTestRunner(BaseAcceptanceTestRunner):

    _test_binary_executor_type = AcceptanceTestBinaryExecutor
    _cleaner_type = AcceptanceTestCleaner

    @property
    def _remote_verify_test_path(self) -> str:
        return '/usr/bin/verify-test'

    def _get_test_suite(self):
        return self._args.test_suite

    def _run_single_testcase(self, instance, test_case):
        with self._recording_result(
            instance.compute_node,
            instance.id,
            test_case.disk_size,
            test_case.disk_type,
            test_case.disk_blocksize,
            {},
            f'{self._args.zone_id}_{test_case.test_case_name}',
        ):
            _logger.info(f'Executing test case'
                         f' <name={test_case.name % self._iodepth}>')
            # Create disk
            disk_policy = YcpNewDiskPolicy(
                self._create_ycp(self._args.cluster),
                zone_id=self._args.zone_id,
                name=f'acc-{self._args.test_type}-'
                     f'{self._args.test_suite}-{self._timestamp}',
                size=test_case.disk_size,
                blocksize=test_case.disk_blocksize,
                type_id=test_case.disk_type,
                auto_delete=not self._args.debug)

            with disk_policy.obtain() as disk:
                _logger.info(
                    f'Waiting until disk <id={disk.id}> will be attached'
                    f' to instance <id={instance.id}> and secondary disk'
                    f' appears as block device')

                with self._instance_policy.attach_disk(disk) as disk_path:
                    self._perform_verification_write(
                        test_case.verify_write_cmd % (
                            self._remote_verify_test_path,
                            disk_path,
                            self._iodepth),
                        disk,
                        instance)

                # Perform acceptance test on current disk
                disk_ids = self._perform_acceptance_test_on_single_disk(disk)

                for disk_id in disk_ids:
                    output_disk = self._ycp.get_disk(disk_id)

                    # Attach output disk
                    _logger.info(
                        f'Waiting until disk <id={output_disk.id}> will be'
                        f' attached to instance <id={instance.id}> and'
                        f' secondary disk appears as block device')

                    with self._instance_policy.attach_disk(output_disk) as disk_path:
                        self._perform_verification_read(
                            test_case.verify_read_cmd % (
                                self._remote_verify_test_path,
                                disk_path,
                                self._iodepth),
                            output_disk,
                            instance)

                # Delete all output disks
                self._delete_output_disks(disk_ids)

    def report_failed_test_cases(
        self,
        test_cases: list[TestCase],
        exception: Exception,
        instance: Ycp.Instance | None = None
    ):
        if self._results_processor is None:
            raise exception
        for test_case in test_cases:
            compute_node = instance and instance.compute_node or "FailedToGetInstance"
            compute_node_id = instance and instance.id or "FailedToGetInstanceId"
            self._results_processor.publish_test_report_base(
                compute_node,
                compute_node_id,
                disk_size=test_case.disk_size,
                disk_type=test_case.disk_type,
                disk_bs=test_case.disk_blocksize,
                extra_params={},
                test_case_name=f'{self._args.zone_id}_{test_case.test_case_name}',
                error=exception,
            )
        raise exception

    def run(self, profiler: common.Profiler) -> None:
        self._initialize_run(
            profiler,
            f'acc-{self._args.test_type}-{self._args.test_suite}-'
            f'{self._timestamp}',
            'acceptance',
        )
        # Generate test cases from test_suite name
        test_cases = generate_test_cases(self._args.test_suite,
                                         self._cluster.name)

        with self.instance_policy_obtained(
            lambda err: self.report_failed_test_cases(
                test_cases,
                err,
            ),
        ) as instance:
            try:
                # Copy verify-test binary to instance
                _logger.info(f'Copying <path={self._args.verify_test}> to'
                             f' <path={instance.ip}:'
                             f'{self._remote_verify_test_path}>')
                self._scp_file(self._args.verify_test,
                               self._remote_verify_test_path,
                               instance.ip)

                _logger.info(f'<path={self._args.verify_test}> copied to'
                             f' <path={instance.ip}:'
                             f'{self._remote_verify_test_path}>')

                _logger.info(f'Generated <len={len(test_cases)}> test cases'
                             f' for test suite <name={self._args.test_suite}>')
            except Exception as e:
                self.report_failed_test_cases(test_cases, e, instance)
            for test_case in test_cases:
                self._run_single_testcase(instance, test_case)
