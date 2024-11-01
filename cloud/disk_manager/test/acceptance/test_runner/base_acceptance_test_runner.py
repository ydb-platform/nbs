import argparse
import calendar
import contextlib
import logging
import re
import socket
import subprocess
import sys
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Type

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config
from cloud.blockstore.pylibs.ycp import Ycp, YcpWrapper
from cloud.blockstore.pylibs.ydb.tests.library.harness.kikimr_runner import (
    get_unique_path_for_current_test,
    ensure_path_exists
)
from .cleanup import (
    cleanup_previous_acceptance_tests_results,
    BaseResourceCleaner,
)
from .lib import (
    check_ssh_connection,
    create_ycp,
    make_disk_parameters_string,
    Error,
    YcpNewInstancePolicy,
    YcpFindDiskPolicy,
    YcpNewDiskPolicy
)

_logger = logging.getLogger(__file__)


class BaseTestBinaryExecutor:
    _entity_suffix: str

    def __init__(
        self,
        args: argparse.Namespace,
        ycp: YcpWrapper,
    ):
        cached_ids_folder = Path(
            get_unique_path_for_current_test(
                output_path='/tmp',
                sub_folder=f'{args.test_type}_acceptance_{str(uuid.uuid4())}'
            ),
        )

        ensure_path_exists(cached_ids_folder)
        self._output_disk_ids_file = cached_ids_folder / 'disks.txt'
        self._output_snapshot_ids_file = cached_ids_folder / 'snapshots.txt'
        self._acceptance_test_cmd = [
            f'{args.acceptance_test}',
            '--profile', f'{args.profile_name}',
            '--folder-id', f'{ycp._folder_desc.folder_id}',
            '--zone-id', f'{args.zone_id}',
            '--output-disk-ids', str(self._output_disk_ids_file),
        ]
        if ycp.ycp_config_path is not None:
            self._acceptance_test_cmd.extend(['--ycp-config-path', f'{ycp.ycp_config_path}'])
        if args.conserve_snapshots:
            self._acceptance_test_cmd.extend([
                '--output-snapshot-ids',
                self._output_snapshot_ids_file])

        if args.verbose:
            self._acceptance_test_cmd.append('--verbose')
        if args.skip_images:
            self._acceptance_test_cmd.append('--skip-images')

        self._s3_host = getattr(args, 's3_host', None)

    def run(self, disk_type: str, disk_id: str, disk_size: str, disk_blocksize: str) -> None:
        self._acceptance_test_cmd.extend(
            [
                '--suffix',
                (
                    f'{self._entity_suffix}-'
                    f'{make_disk_parameters_string(disk_type, int(disk_size), int(disk_blocksize))}'.lower()
                ),
            ]
        )
        self._acceptance_test_cmd.extend(['--src-disk-ids', disk_id])
        with subprocess.Popen(self._acceptance_test_cmd,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE) as proc:
            for line in iter(proc.stdout.readline, ''):
                if proc.poll() is not None:
                    break
                _logger.info(line.rstrip().decode('utf-8'))
            if proc.returncode != 0:
                stderr_str = b''.join(proc.stderr.readlines()).decode('utf-8')
                raise Error(
                    f'failed to execute {self._acceptance_test_cmd}:'
                    f' {stderr_str}')

    def log_snapshot_ids(self) -> None:
        _logger.info(f'Reading file with snapshot ids'
                     f' <path={self._output_snapshot_ids_file}>')
        snapshot_ids = [
            line.rstrip()
            for line in self._output_snapshot_ids_file.read_text().splitlines()
        ]
        _logger.info(f'Generated snapshots: {snapshot_ids}')

    def get_disk_ids(self) -> list[str]:
        _logger.info(f'Reading file with disk ids'
                     f' <path={self._output_disk_ids_file}>')
        disk_ids = [
            line.rstrip()
            for line in self._output_disk_ids_file.read_text().splitlines()
        ]
        _logger.info(f'Generated disks: {disk_ids}')
        return disk_ids


class BaseAcceptanceTestRunner(ABC):

    _test_binary_executor_type: Type[BaseTestBinaryExecutor]
    _cleaner_type: Type[BaseResourceCleaner]
    _single_disk_test_ttl = timedelta(days=1)

    @abstractmethod
    def run(self, profiler: common.Profiler):
        """Test run method"""

    def __init__(self, args: argparse.Namespace, module_factory: common.ModuleFactories) -> None:
        self._module_factory = module_factory
        self._args = args
        self._cluster = get_cluster_test_config(args.cluster,
                                                args.zone_id,
                                                args.cluster_config_path)
        self._iodepth = self._args.instance_cores * 4
        self._results_processor = None
        self._cleanup_before_tests = self._args.cleanup_before_tests
        if self._args.results_path is not None:
            self._results_processor = common.ResultsProcessorFsBase(
                service='disk-manager',
                cluster=self._cluster.name,
                test_suite=self._get_test_suite(),
                date=datetime.today().strftime('%Y-%m-%d'),
                results_path=self._args.results_path,
            )

    @abstractmethod
    def _get_test_suite(self):
        pass

    def _setup_binary_executor(self):
        self._test_binary_executor = self._test_binary_executor_type(
            self._args,
            self._ycp,
        )

    @property
    def _timestamp(self) -> int:
        return calendar.timegm(datetime.utcnow().utctimetuple())

    def _create_ycp(self, cluster_name: str) -> YcpWrapper:
        return create_ycp(
            cluster_name,
            zone_id=self._args.zone_id,
            cluster_test_config_path=self._args.cluster_config_path,
            chunk_storage_type=self._args.chunk_storage_type,
            make_ycp_config_generator=self._module_factory.make_config_generator,
            module_factory=self._module_factory,
            use_generated_config=self._args.generate_ycp_config,
            profile_name=self._args.profile_name,
            ycp_requests_template_path=self._args.ycp_requests_template_path,
        )

    def _initialize_run(self,
                        profiler: common.Profiler,
                        instance_name: str,
                        suffix: str) -> None:
        self._profiler = profiler
        self._ycp = self._create_ycp(self._cluster.name)
        self._ycp_config_path = self._ycp.ycp_config_path
        self._entity_suffix = suffix

        # Create instance
        self._instance_policy = YcpNewInstancePolicy(
            self._create_ycp(self._args.cluster),
            zone_id=self._args.zone_id,
            name=instance_name,
            core_count=self._args.instance_cores,
            ram_count=self._args.instance_ram,
            compute_node=self._args.compute_node,
            placement_group=self._args.placement_group_name,
            platform_ids=self._args.instance_platform_ids,
            do_not_delete_on_error=True,
            auto_delete=not self._args.debug,
            module_factory=self._module_factory,
            ssh_key_path=self._args.ssh_key_path)
        if self._cleanup_before_tests:
            self._cleaner_type(self._ycp, self._args).cleanup()

    def _perform_acceptance_test_on_single_disk(self, disk: Ycp.Disk) -> List[str]:
        # Execute acceptance test on test disk
        if self._cleanup_before_tests:
            cleanup_previous_acceptance_tests_results(
                self._ycp,
                self._args.test_type,
                disk.type_id,
                int(disk.size),
                int(disk.block_size),
                self._single_disk_test_ttl,
            )
        _logger.info(f'Executing acceptance test on disk <id={disk.id}>')
        self._setup_binary_executor()
        self._test_binary_executor.run(disk.type_id, disk.id, disk.size, disk.block_size)
        disk_ids = self._test_binary_executor.get_disk_ids()

        if self._args.conserve_snapshots:
            self._test_binary_executor.log_snapshot_ids()

        return disk_ids

    def _scp_file(self,
                  src: str,
                  dst: str,
                  ip: str) -> None:
        check_ssh_connection(ip, self._profiler, self._module_factory, self._args.ssh_key_path)
        try:
            with self._module_factory.make_sftp_client(False, ip, ssh_key_path=self._args.ssh_key_path) as sftp:
                sftp.put(src, dst)
                sftp.chmod(dst, 0o755)
        except (common.SshException, socket.error) as e:
            raise Error(f'Failed to copy file {src}'
                        f' from local to remote host {ip}:'
                        f'{dst} via sftp: {e}')

    def _execute_ssh_cmd(self,
                         cmd: str,
                         ip: str) -> str:
        check_ssh_connection(ip, self._profiler, self._module_factory, self._args.ssh_key_path)
        with self._module_factory.make_ssh_client(False, ip, ssh_key_path=self._args.ssh_key_path) as ssh:
            _, stdout, stderr = ssh.exec_command(cmd)
            output = ''
            for line in iter(lambda: stdout.readline(2048), ''):
                output += line.rstrip() + " "
                _logger.info("stdout: %s", output)
            if stderr.channel.recv_exit_status():
                stderr_lines = stderr.readlines()
                stderr_str = ''.join(stderr_lines)
                for stderr_line in stderr_lines:
                    _logger.info("stderr: %s ", stderr_line.rstrip())
                raise Error(f'failed to execute command {cmd} on remote host'
                            f' {ip}: {stderr_str}')
            return output

    def _perform_verification_write(self,
                                    cmd: str,
                                    disk: Ycp.Disk,
                                    instance: Ycp.Instance) -> None:
        _logger.info(f'Filling disk <id={disk.id}> with'
                     f' verification data on instance'
                     f' <id={instance.id}>')
        self._execute_ssh_cmd(cmd, instance.ip)

    def _perform_verification_read(self,
                                   cmd: str,
                                   disk: Ycp.Disk,
                                   instance: Ycp.Instance) -> None:
        _logger.info(f'Verifying data on disk'
                     f' <id={disk.id}> on'
                     f' instance <id={instance.id}>')
        self._execute_ssh_cmd(cmd, instance.ip)

    def _delete_output_disks(self, disk_ids: List[str]) -> None:
        error_str = ''
        for disk_id in disk_ids:
            try:
                self._ycp.delete_disk(self._ycp.get_disk(disk_id))
            except YcpWrapper.Error as e:
                _logger.error(f'failed to delete disk <id={disk_id}>')
                error_str += f'\n{e}'
                continue
        if len(error_str) > 0:
            raise Error('failed to delete disks: {error_str}')

    def _find_or_create_eternal_disk(self, disk_name_prefix) -> Ycp.Disk:
        disk_name_regex = disk_name_prefix+'-[0-9]+'

        _logger.info(f'Try to find disk'
                     f' <name_regex={disk_name_regex}>')

        # Try to find disk with test name regex
        # Or create it if nothing was found
        disk_policy = YcpFindDiskPolicy(
            self._create_ycp(self._args.cluster),
            zone_id=self._args.zone_id,
            name_regex=disk_name_regex)

        try:
            disk = disk_policy.obtain()
            _logger.info(f'Disk <id={disk.id}, name={disk.name}> was found')
            for instance_id in disk.instance_ids:
                attached_instance = self._ycp.get_instance(instance_id)
                _logger.info(f'Disk <id={disk.id}> is attached to'
                             f' instance <id={attached_instance.id}>.'
                             f' Try to detach.')
                self._ycp.detach_disk(attached_instance, disk)
            disk.instance_ids.clear()
        except Error as e:
            if re.match('^Failed to find disk', f'{e}'):
                _logger.info(f'Failed to find disk'
                             f' <name_regex={disk_name_regex}>')
                disk_policy = YcpNewDiskPolicy(
                    self._create_ycp(self._args.cluster),
                    zone_id=self._args.zone_id,
                    name=disk_name_prefix + f'-{self._timestamp}',
                    size=self._args.disk_size,
                    blocksize=self._args.disk_blocksize,
                    type_id=self._args.disk_type,
                    auto_delete=False)
                with disk_policy.obtain() as ycp_disk:
                    disk = ycp_disk

        return disk

    @contextlib.contextmanager
    def instance_policy_obtained(self, handler):
        ctx = self._instance_policy.obtain()
        try:
            instance = ctx.__enter__()
        except Exception as e:
            handler(e)
            raise e
        try:
            yield instance
        finally:
            exc_type, exc_value, exc_tb = sys.exc_info()
            ctx.__exit__(exc_type, exc_value, exc_tb)

    @contextlib.contextmanager
    def _recording_result(
        self,
        compute_node: str,
        _id: str,
        disk_size: int,
        disk_type: str,
        disk_bs: int,
        extra_params: dict,
        test_case_name: str,
    ) -> None:
        error = None
        try:
            yield
        except Exception as e:
            error = e
            raise e
        finally:
            if self._results_processor is not None:
                self._results_processor.publish_test_report_base(
                    compute_node=compute_node,
                    id=_id,
                    disk_size=disk_size,
                    disk_type=disk_type,
                    disk_bs=disk_bs,
                    extra_params=extra_params,
                    test_case_name=test_case_name,
                    error=error,
                )

    def _make_disk_parameters_string(self, delim: str = "-"):
        return make_disk_parameters_string(
            self._args.disk_type,
            self._args.disk_size * (1024 ** 3),
            self._args.disk_blocksize,
            delim,
        )
