import argparse
import datetime
import logging
import re

from cloud.blockstore.pylibs.ycp import YcpWrapper
from .base_acceptance_test_runner import BaseAcceptanceTestRunner, \
    BaseTestBinaryExecutor, BaseResourceCleaner
from .lib import (
    size_prettifier,
    Error,
)

from cloud.blockstore.pylibs import common


_logger = logging.getLogger(__file__)


class SyncTestBinaryExecutor(BaseTestBinaryExecutor):
    _entity_suffix = 'sync'


class SyncTestCleaner(BaseResourceCleaner):
    def __init__(self, ycp: YcpWrapper, args: argparse.Namespace):
        super(SyncTestCleaner, self).__init__(ycp, args)
        test_type = args.test_type
        disk_name_string = (
            fr'^acceptance-test-{test_type}-'
            fr'{self._disk_size}-'
            fr'{self._disk_blocksize}-[0-9]+'
        )
        disk_name_pattern = re.compile(fr'{disk_name_string}$')
        secondary_disk_name_pattern = re.compile(
            fr'{disk_name_string}-from-snapshot$',
        )
        self._entity_ttls = self._entity_ttls | {
            'disk': datetime.timedelta(days=5),
            'snapshot': datetime.timedelta(days=5),
        }
        self._patterns = {
            **self._patterns,
            'disk': [disk_name_pattern, secondary_disk_name_pattern],
            'snapshot': [re.compile(r'^sync-acceptance-test-snapshot-.*$')]
        }


class SyncAcceptanceTestRunner(BaseAcceptanceTestRunner):

    _cleaner_type = SyncTestCleaner
    _single_disk_test_ttl = datetime.timedelta(days=5)

    def _get_test_suite(self):
        return (
            f'{self._args.zone_id}_sync_'
            f'{size_prettifier(self._args.disk_size * (1024 ** 3))}_'
            f'{size_prettifier(self._args.disk_blocksize)}'.lower()
        )

    def run(self, profiler: common.Profiler) -> None:
        self._initialize_run(
            profiler,
            f'acceptance-test-{self._args.test_type}-{self._timestamp}',
            'sync',
        )

        with self._instance_policy.obtain() as instance:
            with self._recording_result(
                instance.compute_node,
                instance.id,
                self._args.disk_size * (1024 ** 3),
                self._args.disk_type,
                self._args.disk_blocksize,
                {},
                (
                    f'{self._args.zone_id}_sync_'
                    f'{size_prettifier(self._args.disk_size * (1024 ** 3))}_'
                    f'{size_prettifier(self._args.disk_blocksize)}'.lower()
                ),
            ):
                disk_name_prefix = (
                    f'acceptance-test-{self._args.test_type}-'
                    f'{size_prettifier(self._args.disk_size * (1024 ** 3))}'
                    f'-{size_prettifier(self._args.disk_blocksize)}').lower()
                disk = self._find_or_create_eternal_disk(disk_name_prefix)

                _logger.info(
                    f'Waiting until disk <id={disk.id}> will be attached '
                    f' to instance <id={instance.id}> and appears as block device')

                with self._instance_policy.attach_disk(disk) as block_device_1:
                    self._create_ext4_filesystem(block_device_1, instance)
                    folder_1 = '/tmp/sync_acceptance_test'
                    self._mount_block_device(folder_1, block_device_1, instance)
                    checksums_1 = self._create_random_files_and_get_checksums(folder_1, instance)
                    snapshot_name = self._create_disk_snapshot(disk)

                    _logger.info('Creating disk from snapshot')
                    with self._ycp.create_disk(
                        name=disk_name_prefix+f'-{self._timestamp}-from-snapshot',
                        bs=self._args.disk_blocksize,
                        size=self._args.disk_size,
                        type_id=self._args.disk_type,
                        snapshot_name=snapshot_name,
                        auto_delete=False
                    ) as disk_copy:
                        _logger.info('Created disk from snapshot')

                        _logger.info(
                            f'Waiting until disk copy <id={disk_copy.id}> will be '
                            f'attached to instance <id={instance.id}> and appears as a block device')

                        with self._instance_policy.attach_disk(disk_copy) as block_device_2:
                            _logger.info('Attached disk copy to device')

                            folder_2 = '/tmp/sync_acceptance_test_copy'
                            self._mount_block_device(folder_2, block_device_2, instance)
                            checksums_2 = self._get_checksums(folder_2, instance)
                            self._check_checksums_equality(checksums_1, checksums_2)

    def _create_ext4_filesystem(self, block_device, instance) -> None:
        _logger.info(f'Creating ext4 filesystem on {block_device}')
        self._execute_ssh_cmd(f"mkfs.ext4 {block_device}", instance.ip)
        _logger.info(f'Created ext4 filesystem on {block_device}')

    def _create_random_files_and_get_checksums(self, folder, instance) -> list[str]:
        _logger.info('Writing some files and syncing them')

        md5sum_output = self._execute_ssh_cmd(
            f"dd if=/dev/urandom of={folder}/file1 bs=1M count=10 && md5sum {folder}/file1 && "
            f"dd if=/dev/urandom of={folder}/file2 bs=1M count=10 && md5sum {folder}/file2 && "
            f"dd if=/dev/urandom of={folder}/file3 bs=1M count=10 && md5sum {folder}/file3 && "
            f"sync",
            instance.ip)
        _logger.info('Wrote some files and synced them')

        checksums = re.findall(rf"(\S+) {{2}}{folder}/file\d", md5sum_output)
        _logger.info(f"file checksums are: {checksums}")

        return checksums

    def _create_disk_snapshot(self, disk) -> str:
        _logger.info('Creating snapshot')

        suffix = datetime.datetime.now().strftime("%y-%m-%d-%H-%M-%S")
        snapshot_name = f"sync-acceptance-test-snapshot-{suffix}"
        self._ycp.create_snapshot(disk.id, snapshot_name)

        _logger.info('Created snapshot')
        return snapshot_name

    def _mount_block_device(self, folder_to_mount, block_device, instance) -> None:
        _logger.info(f'Mounting {block_device} to {folder_to_mount}')
        self._execute_ssh_cmd(
            f"mkdir {folder_to_mount} && "
            f"mount {block_device} {folder_to_mount}",
            instance.ip)
        _logger.info(f'Mounted {block_device} to {folder_to_mount}')

    def _get_checksums(self, folder, instance) -> list[str]:
        _logger.info('Getting file checksums from the copy folder')
        md5sum_output = self._execute_ssh_cmd(
            f"md5sum {folder}/file1 && "
            f"md5sum {folder}/file2 && "
            f"md5sum {folder}/file3",
            instance.ip)
        checksums = re.findall(rf"(\S+) {{2}}{folder}/file\d", md5sum_output)
        _logger.info(f"file checksums are: {checksums}")

        return checksums

    def _check_checksums_equality(self, checksums_1, checksums_2) -> None:
        _logger.info('Checking that file checksums are equal')

        if len(checksums_1) != 3 or len(checksums_1) != len(checksums_2):
            raise Error("Unable to retrieve all file checksums")

        for i in range(len(checksums_1)):
            if checksums_1[i] != checksums_2[i]:
                raise Error(
                    f"Checksum mismatch: '{checksums_1[i]}', '{checksums_2[i]}")

        _logger.info('File checksums are equal, test is complete')
