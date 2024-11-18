import argparse
from datetime import datetime
import json
import logging
import socket


from cloud.blockstore.tools.ci.fio_performance_test_suite.lib.test_cases import (
    generate_test_cases,
    IO_SIZE,
    NBS,
    NFS,
    TestCase
)
from cloud.blockstore.tools.ci.fio_performance_test_suite.lib.errors import Error

from cloud.blockstore.pylibs import common
from cloud.blockstore.pylibs.clusters.test_config import get_cluster_test_config, translate_disk_type
from cloud.blockstore.pylibs.ycp import Ycp, YcpWrapper, make_ycp_engine

_DEFAULT_ZONE_ID = 'ru-central1-b'
_DEFAULT_INSTANCE_CORES = 8
_DEFAULT_INSTANCE_RAM = 8


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    verbose_quite_group = parser.add_mutually_exclusive_group()
    verbose_quite_group.add_argument('-v', '--verbose', action='store_true')
    verbose_quite_group.add_argument('-q', '--quite', action='store_true')

    parser.add_argument(
        '--teamcity',
        action='store_true',
        default=False,
        help='use teamcity logging format')

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
        '-f',
        '--force',
        action='store_true',
        default=False,
        help='run all test cases and override YT results table')
    test_arguments_group.add_argument(
        '--placement-group-name',
        type=str,
        default=None,
        help='create vm in the specified placement group')
    test_arguments_group.add_argument(
        '--platform-id',
        type=str,
        default="standard-v2",
        help='create vm on the specified platform')
    test_arguments_group.add_argument(
        '--compute-node',
        type=str,
        default=None,
        help='run fio test on the specified compute node')
    test_arguments_group.add_argument(
        '--no-store-test-results',
        action='store_true',
        default=False,
        help='do not store test results')
    test_arguments_group.add_argument(
        '--zone-id',
        type=str,
        default=_DEFAULT_ZONE_ID,
        help=f'specify zone id, default is {_DEFAULT_ZONE_ID}')
    test_arguments_group.add_argument(
        '--service',
        choices=[NBS, NFS],
        default=NBS,
        help='specify to use nbs or nfs for test')
    test_arguments_group.add_argument(
        '--ttl-instance-days',
        type=int,
        default=7,
        help='ttl for tmp instances')
    test_arguments_group.add_argument(
        '--in-parallel',
        action='store_true',
        default=False,
        help='run all test cases in parallel')
    test_arguments_group.add_argument(
        '--image-name',
        type=str,
        default=None,
        help='use image with the specified name to create disks')
    test_arguments_group.add_argument(
        '--image-folder-id',
        type=str,
        default=None,
        help='use image from the specified folder to create disks')
    test_arguments_group.add_argument(
        '--instance-cores',
        type=int,
        default=_DEFAULT_INSTANCE_CORES,
        help=f'specify instance core count, default is {_DEFAULT_INSTANCE_CORES}')
    test_arguments_group.add_argument(
        '--instance-ram',
        type=int,
        default=_DEFAULT_INSTANCE_RAM,
        help=f'specify instance RAM in GiB, default is {_DEFAULT_INSTANCE_RAM}')
    test_arguments_group.add_argument(
        '--debug',
        action='store_true',
        default=False,
        help='do not delete instance and disk, if fail')
    test_arguments_group.add_argument(
        '--results-path',
        type=str,
        default='',
        help='specify path to test results')

    args = parser.parse_args()

    if args.service == NFS:
        args.in_parallel = False

    return args


class FioTestsRunner:

    def __init__(
        self,
        module_factories: common.ModuleFactories,
        args: argparse.Namespace,
        profiler: common.Profiler,
        logger: logging.Logger,
    ) -> None:
        self._args = args
        self._profiler = profiler
        self._logger = logger
        self._module_factories = module_factories

    def _version_method_name(self) -> str:
        raise NotImplementedError()

    def _run_tests(
        self,
        test_cases: list[TestCase],
        completed_test_cases: set[TestCase],
        helpers,
        ycp: YcpWrapper,
        instance: Ycp.Instance,
    ) -> None:
        pass

    def _load_fio_results(self, fio_results):
        return fio_results if self._args.dry_run else json.load(fio_results)

    def _teardown(
        self,
        ycp: YcpWrapper,
        is_fail: bool = True,
    ) -> None:
        if is_fail and self._args.debug:
            ycp.turn_off_auto_deletion()

    def run_test_suite(self) -> None:
        cluster = get_cluster_test_config(self._args.cluster, self._args.zone_id, self._args.cluster_config_path)

        today = datetime.today().strftime('%Y-%m-%d')

        helpers = self._module_factories.make_helpers(self._args.dry_run)
        ycp_config_generator = self._module_factories.make_config_generator(self._args.dry_run)
        ycp = YcpWrapper(
            self._args.profile_name or cluster.name,
            cluster.ipc_type_to_folder_desc(self._args.ipc_type),
            self._logger,
            make_ycp_engine(self._args.dry_run),
            ycp_config_generator,
            helpers,
            self._args.generate_ycp_config,
            self._args.ycp_requests_template_path)
        cores, memory = (self._args.instance_cores, self._args.instance_ram)

        ycp.delete_tmp_instances(self._args.ttl_instance_days)

        try:
            with ycp.create_instance(
                    cores=cores,
                    memory=memory,
                    platform_id=self._args.platform_id,
                    compute_node=self._args.compute_node,
                    placement_group_name=self._args.placement_group_name,
                    image_name=self._args.image_name,
                    image_folder_id=self._args.image_folder_id,
                    description='fio performance test') as instance:
                version = self._module_factories.fetch_server_version(
                    self._args.dry_run,
                    self._version_method_name(),
                    instance.compute_node,
                    cluster,
                    self._logger)
                self._logger.info(f'Running fio performance test suite'
                                  f' on cluster <{cluster.name}>'
                                  f' for service {self._args.service}'
                                  f' of version <{version}>')

                test_cases = generate_test_cases(
                    self._args.test_suite,
                    cluster.name,
                    self._args.service)
                self._logger.info(f'Generated {len(test_cases)} test cases'
                                  f' for test suite <{self._args.test_suite}>'
                                  f' for service <{self._args.service}>')

                self._results_processor = self._module_factories.make_test_result_processor(
                    self._args.service,
                    self._args.test_suite,
                    cluster.name,
                    version,
                    today,
                    self._logger,
                    self._args.results_path,
                    self._args.dry_run or self._args.no_store_test_results)

                if not self._args.force:
                    completed_test_cases: set[TestCase] = \
                        self._results_processor.fetch_completed_test_cases()
                    self._logger.info(f'Found {len(completed_test_cases)} completed test'
                                      f' cases for today`s date <{today}>')
                    if len(completed_test_cases) == len(test_cases):
                        self._results_processor.announce_test_run()
                        return
                else:
                    completed_test_cases: set[TestCase] = set()

                self._logger.info(f'Waiting until instance <id={instance.id}> becomes'
                                  f' available via ssh')
                try:
                    helpers.wait_until_instance_becomes_available_via_ssh(instance.ip,
                                                                          ssh_key_path=self._args.ssh_key_path)
                except (common.SshException, socket.error) as e:
                    self._profiler.add_ip(instance.ip)
                    self._teardown(ycp)
                    raise Error(f'failed to start test, instance not reachable'
                                f' via ssh: {e}')

                #  Skip completed test cases
                test_cases_to_run = []
                for test_case in test_cases:
                    if not self._args.force and test_case in completed_test_cases:
                        self._logger.info(f'Test case <name={test_case.name}>'
                                          f' will be skipped')
                        continue
                    test_cases_to_run.append(test_case)

                self._run_tests(
                    test_cases_to_run,
                    completed_test_cases,
                    helpers,
                    ycp,
                    instance)
        except YcpWrapper.Error as e:
            raise Error(f'failed to run test, problem with ycp:\n{e}')

        # Announce test run for dashboard if at least half of test cases have passed
        if len(completed_test_cases) >= len(test_cases) // 2:
            self._results_processor.announce_test_run()

        if len(completed_test_cases) != len(test_cases):
            raise Error('failed to execute all the test cases; watch log for more info')

    @staticmethod
    def _get_fio_cmd_to_fill_device(target_path: str, disk_type: str) -> str:
        iodepth = 128
        if disk_type.find('hdd') >= 0:
            iodepth = 8
        return (f'fio --name=fill-secondary-disk --filename={target_path}'
                f' --rw=write --bs=4M --iodepth={iodepth} --direct=1 --sync=1'
                f' --ioengine=libaio --size={IO_SIZE}')

    def _execute_fio_commands(
        self,
        test_case: TestCase,
        ycp: YcpWrapper,
        instance: Ycp.Instance
    ):
        with self._module_factories.make_ssh_client(self._args.dry_run, instance.ip, ssh_key_path=self._args.ssh_key_path) as ssh:
            try:
                if test_case.type in ['network-ssd-io-m2', 'network-ssd-io-m3']:
                    minutes_to_wait = 10
                    self._logger.info(f'Waiting for {minutes_to_wait} min'
                                      f' for blocks resync')  # NBS-3907
                    _, _, stderr = ssh.exec_command(f'sleep {60 * minutes_to_wait}')
                    if stderr.channel.recv_exit_status():
                        stderr_str = ''.join(stderr.readlines())
                        self._teardown(ycp)
                        raise Error(f'failed to execute sleep command on instance'
                                    f' <id={instance.id}>:\n{stderr_str}')

                self._logger.info(f'Filling device with random data on instance'
                                  f' <id={instance.id}>')
                _, _, stderr = ssh.exec_command(
                    self._get_fio_cmd_to_fill_device(test_case.target_path, test_case.type))
                if stderr.channel.recv_exit_status():
                    stderr_str = ''.join(stderr.readlines())
                    self._teardown(ycp)
                    raise Error(f'failed to fill device on instance'
                                f' <id={instance.id}>:\n{stderr_str}')

                self._logger.info(f'Running fio on instance <id={instance.id}>')
                _, stdout, stderr = ssh.exec_command(test_case.fio_cmd)
                if stdout.channel.recv_exit_status():
                    stderr_str = ''.join(stderr.readlines())
                    self._teardown(ycp)
                    raise Error(f'failed to run fio on instance'
                                f' <id={instance.id}>:\n{stderr_str}')
            except (common.SshException, socket.error) as e:
                self._profiler.add_ip(instance.ip)
                self._teardown(ycp)
                raise Error(f'failed to finish test, problem with ssh'
                            f' connection: {e}')

            return stdout


class FioTestsRunnerNbs(FioTestsRunner):

    def __init__(
        self,
        module_factories: common.ModuleFactories,
        args: argparse.Namespace,
        profiler: common.Profiler,
        logger: logging.Logger,
    ) -> None:
        super().__init__(module_factories, args, profiler, logger)

    def _version_method_name(self) -> str:
        return 'get_current_nbs_version'

    def _teardown_parallel_run(
        self,
        ycp: YcpWrapper,
        instance: Ycp.Instance,
        disks: list[Ycp.Disk],
        is_fail: bool = True,
    ) -> None:
        if not self._args.in_parallel:
            self._logger.error('must not get here when in_parallel option is disabled')
        self._teardown(ycp, is_fail=is_fail)
        if not self._args.debug:
            self._detach_disks(instance, disks, ycp)
            self._delete_disks(disks, ycp)

    def _detach_disks(
        self,
        instance: Ycp.Instance,
        disks: list[Ycp.Disk],
        ycp: YcpWrapper,
    ) -> None:
        error_str = ''
        for disk in disks:
            try:
                ycp.detach_disk(instance, disk)
            except YcpWrapper.Error as e:
                self._logger.error(f'failed to detach disk <id={disk.id}>')
                error_str += f'\n{e}'
                continue
        if len(error_str) > 0:
            raise Error(f'failed to detach disks: {error_str}')

    def _delete_disks(
        self,
        disks: list[Ycp.Disk],
        ycp: YcpWrapper,
    ) -> None:
        error_str = ''
        for disk in disks:
            try:
                ycp.delete_disk(disk)
            except YcpWrapper.Error as e:
                self._logger.error(f'failed to delete disk <id={disk.id}>')
                error_str += f'\n{e}'
                continue
        if len(error_str) > 0:
            raise Error(f'failed to delete disks: {error_str}')

    def _run_tests(
        self,
        test_cases: list[TestCase],
        completed_test_cases: set[TestCase],
        helpers,
        ycp: YcpWrapper,
        instance: Ycp.Instance,
    ) -> None:
        if self._args.in_parallel:
            for test_index in range(len(test_cases)):
                test_cases[test_index].target_path = \
                    f'/dev/vd{chr(ord("a") + test_index + 1)}'
            self._run_parallel_tests(
                test_cases,
                completed_test_cases,
                helpers,
                ycp,
                instance)
        else:
            self._run_sequential_tests(
                test_cases,
                completed_test_cases,
                helpers,
                ycp,
                instance)

    def _run_sequential_tests(
        self,
        test_cases: list[TestCase],
        completed_test_cases: set[TestCase],
        helpers,
        ycp: YcpWrapper,
        instance: Ycp.Instance,
    ) -> None:
        for test_case in test_cases:
            self._logger.info(f'Executing test case <{test_case.name}>')
            with ycp.create_disk(
                    size=test_case.size,
                    type_id=translate_disk_type(self._args.cluster, test_case.type),
                    bs=test_case.device_bs,
                    image_name=self._args.image_name,
                    image_folder_id=self._args.image_folder_id,
                    description=f'fio performance test: {test_case.name}') as disk:
                try:
                    with ycp.attach_disk(
                            instance=instance,
                            disk=disk):
                        self._logger.info(f'Waiting until secondary disk appears as block'
                                          f' device on instance <id={instance.id}>')
                        helpers.wait_for_block_device_to_appear(
                            instance.ip,
                            test_case.target_path,
                            ssh_key_path=self._args.ssh_key_path)

                        fio_results = self._execute_fio_commands(
                            test_case,
                            ycp,
                            instance)
                except Exception as e:
                    self._logger.error(e)
                    self._results_processor.publish_test_report(
                        instance.compute_node,
                        disk.id,
                        test_case,
                        {},
                        e)
                    continue

            self._results_processor.publish_test_report(
                instance.compute_node,
                disk.id,
                test_case,
                self._load_fio_results(fio_results))
            completed_test_cases.add(test_case)

    def _run_parallel_tests(
        self,
        test_cases: list[TestCase],
        completed_test_cases: set[TestCase],
        helpers,
        ycp: YcpWrapper,
        instance: Ycp.Instance,
    ) -> None:
        disks: list[Ycp.Disk] = list()
        for test_case in test_cases:
            with ycp.create_disk(
                    size=test_case.size,
                    type_id=translate_disk_type(self._args.cluster, test_case.type),
                    bs=test_case.device_bs,
                    image_name=self._args.image_name,
                    image_folder_id=self._args.image_folder_id,
                    auto_delete=False,
                    description=f'fio performance test: {test_case.name}') as disk:
                with ycp.attach_disk(
                        instance=instance,
                        disk=disk,
                        auto_detach=False):
                    helpers.wait_for_block_device_to_appear(
                        instance.ip,
                        test_case.target_path,
                        ssh_key_path=self._args.ssh_key_path)
                    disks.append(disk)

        with self._module_factories.make_ssh_client(self._args.dry_run, instance.ip, ssh_key_path=self._args.ssh_key_path) as ssh:
            stdouts = list()
            stderrs = list()
            for test_case in test_cases:
                try:
                    self._logger.info(f'Filling disk <name={test_case.target_path}> with'
                                      f' random data on instance <id={instance.id}>')
                    _, stdout, stderr = ssh.exec_command(
                        self._get_fio_cmd_to_fill_device(test_case.target_path, test_case.type))
                    stdouts.append(stdout)
                    stderrs.append(stderr)
                except (common.SshException, socket.error) as e:
                    self._profiler.add_ip(instance.ip)
                    self._teardown_parallel_run(ycp, instance, disks)
                    raise Error(f'failed to finish test, problem with'
                                f' ssh connection: {e}')

            for test_index in range(len(stderrs)):
                if stderrs[test_index].channel.recv_exit_status():
                    self._teardown_parallel_run(ycp, instance, disks)
                    stderr_str = ''.join(stderrs[test_index].readlines())
                    raise Error(f'failed to fill disk <id={disks[test_index].id},'
                                f' name={test_cases[test_index].target_path}>'
                                f' on instance <id={instance.id}>:\n{stderr_str}')

            stdouts.clear()
            stderrs.clear()
            for test_case in test_cases:
                try:
                    self._logger.info(f'Running fio on disk <name={test_case.target_path}'
                                      f'> on instance <id={instance.id}>')
                    _, stdout, stderr = ssh.exec_command(test_case.fio_cmd)
                    stdouts.append(stdout)
                    stderrs.append(stderr)
                    self._logger.info(f'Executing test case <{test_case.name}>')
                except (common.SshException, socket.error) as e:
                    self._profiler.add_ip(instance.ip)
                    self._teardown_parallel_run(ycp, instance, disks)
                    raise Error(f'failed to finish test, problem with'
                                f' ssh connection: {e}')

            for test_index in range(len(stdouts)):
                if stdouts[test_index].channel.recv_exit_status():
                    self._teardown_parallel_run(ycp, instance, disks)
                    stderr_str = ''.join(stderrs[test_index].readlines())
                    raise Error(f'failed to fill disk <id={disks[test_index].id},'
                                f' name={test_cases[test_index].target_path}>'
                                f' on instance <id={instance.id}>:\n{stderr_str}')

                self._results_processor.publish_test_report(
                    instance.compute_node,
                    disks[test_index].id,
                    test_cases[test_index],
                    self._load_fio_results(stdouts[test_index]))
                completed_test_cases.add(test_cases[test_index])

        self._teardown_parallel_run(ycp, instance, disks, is_fail=False)


class FioTestsRunnerNfs(FioTestsRunner):
    _NFS_DEVICE = 'nfs'
    _NFS_MOUNT_PATH = '/test'
    _NFS_TEST_FILE = '/test/test.txt'

    def __init__(
        self,
        module_factories: common.ModuleFactories,
        args: argparse.Namespace,
        profiler: common.Profiler,
        logger: logging.Logger,
    ) -> None:
        super().__init__(module_factories, args, profiler, logger)

    def _version_method_name(self) -> str:
        return 'get_current_nfs_version'

    def _mount_fs(
        self,
        ycp: YcpWrapper,
        instance_ip: str,
    ) -> None:
        self._logger.info('Mounting fs')
        with self._module_factories.make_ssh_client(self._args.dry_run, instance_ip, ssh_key_path=self._args.ssh_key_path) as ssh, \
                self._module_factories.make_sftp_client(self._args.dry_run, instance_ip, ssh_key_path=self._args.ssh_key_path) as sftp:
            sftp.mkdir(self._NFS_MOUNT_PATH)
            _, _, stderr = ssh.exec_command(f'mount -t virtiofs '
                                            f'{self._NFS_DEVICE} {self._NFS_MOUNT_PATH} && '
                                            f'touch {self._NFS_TEST_FILE}')
            exit_code = stderr.channel.recv_exit_status()
            if exit_code != 0:
                self._logger.error(f'Failed to mount fs\n'
                                   f'{"".join(stderr.readlines())}')
                self._teardown(ycp)
                raise Error(f'failed to mount fs with exit code {exit_code}')
            sftp.truncate(self._NFS_TEST_FILE, IO_SIZE)

    def _unmount_fs(
        self,
        ycp: YcpWrapper,
        instance_ip: str,
    ) -> None:
        self._logger.info('Unmounting fs')
        with self._module_factories.make_ssh_client(self._args.dry_run, instance_ip, ssh_key_path=self._args.ssh_key_path) as ssh, \
                self._module_factories.make_sftp_client(self._args.dry_run, instance_ip, ssh_key_path=self._args.ssh_key_path) as sftp:
            _, _, stderr = ssh.exec_command(f'umount {self._NFS_MOUNT_PATH}')
            exit_code = stderr.channel.recv_exit_status()
            if exit_code != 0:
                self._logger.error(f'Failed to unmount fs\n'
                                   f'{"".join(stderr.readlines())}')
                self._teardown(ycp)
                raise Error(f'failed to unmount fs with exit code {exit_code}')
            sftp.rmdir(self._NFS_MOUNT_PATH)

    def _run_tests(
        self,
        test_cases: list[TestCase],
        completed_test_cases: set[TestCase],
        helpers,
        ycp: YcpWrapper,
        instance: Ycp.Instance,
    ) -> None:
        for test_case in test_cases:
            test_case.target_path = self._NFS_TEST_FILE
            self._logger.info(f'Executing test case <{test_case.name}>')
            with ycp.create_fs(
                    size=test_case.size,
                    type_id=test_case.type,
                    bs=test_case.device_bs,
                    description=f'fio performance test: {test_case.name}') as fs:
                try:
                    with ycp.attach_fs(
                            instance=instance,
                            fs=fs,
                            device_name=self._NFS_DEVICE):
                        self._mount_fs(ycp, instance.ip)

                        fio_results = self._execute_fio_commands(
                            test_case,
                            ycp,
                            instance)

                        self._unmount_fs(ycp, instance.ip)
                except Exception as e:
                    self._logger.error(e)
                    self._results_processor.publish_test_report(
                        instance.compute_node,
                        fs.id,
                        test_case,
                        e
                    )
                    continue
            self._results_processor.publish_test_report(
                instance.compute_node,
                fs.id,
                test_case,
                self._load_fio_results(fio_results))
            completed_test_cases.add(test_case)
