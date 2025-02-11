import hashlib
import json
import logging
import subprocess
import time

from contextlib import contextmanager
from pathlib import Path
from typing import NamedTuple

import pytest

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists

from cloud.disk_manager.test.recipe.disk_manager_launcher import DiskManagerLauncher
from cloud.disk_manager.test.recipe.metadata_service_launcher import MetadataServiceLauncher
from cloud.disk_manager.test.recipe.nbs_launcher import NbsLauncher
from cloud.disk_manager.test.recipe.s3_launcher import S3Launcher
from cloud.disk_manager.test.recipe.ydb_launcher import YDBLauncher


_logger = logging.getLogger(__file__)


def compute_checksum(file_path: str) -> str:
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


@contextmanager
def handle_process_lookup_error():
    try:
        yield
    except ProcessLookupError:
        pass


class _MigrationTestSetup:

    class _Disk(NamedTuple):
        block_size: int
        blocks_count: int
        id: str

    def __init__(
        self,
        use_s3_source: bool = False,
        use_s3_destination: bool = False,
        use_source_s3_for_destination: bool = False,
    ):
        self.use_s3_source = use_s3_source
        self.use_s3_destination = use_s3_destination
        self.use_source_s3_for_destination = use_source_s3_for_destination

        certs_dir = Path(yatest_common.source_path("cloud/blockstore/tests/certs"))
        self._root_certs_file = certs_dir / "server.crt"
        _logger.info(certs_dir.exists())
        self._cert_file = certs_dir / "server.crt"
        _logger.info(self._cert_file.exists())
        self._cert_key_file = certs_dir / "server.key"
        _logger.info(self._cert_key_file.exists())

        ydb_binary_path = yatest_common.binary_path("cloud/storage/core/tools/testing/ydb/bin/ydbd")
        if ydb_binary_path is None:
            ydb_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")
        nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")
        disk_agent_binary_path = yatest_common.binary_path("cloud/blockstore/apps/disk_agent/diskagentd")
        self.disk_manager_binary_path = yatest_common.binary_path("cloud/disk_manager/cmd/disk-manager/disk-manager")
        self.blockstore_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
        self.disk_manager_admin_binary_path = yatest_common.binary_path("cloud/disk_manager/cmd/disk-manager-admin/disk-manager-admin")

        self.default_cloud_id = "cloud_id"
        self.default_folder_id = "folder_id"
        self.default_zone = "zone-a"

        self.ydb = YDBLauncher(ydb_binary_path=ydb_binary_path)
        self.ydb.start()
        self.secondary_ydb = YDBLauncher(ydb_binary_path=ydb_binary_path)
        self.secondary_ydb.start()
        self.nbs = NbsLauncher(
            self.ydb.port,
            self.ydb.domains_txt,
            self.ydb.dynamic_storage_pools,
            str(self._root_certs_file),
            str(self._cert_file),
            str(self._cert_key_file),
            ydb_binary_path=ydb_binary_path,
            nbs_binary_path=nbs_binary_path,
            disk_agent_binary_path=disk_agent_binary_path,
            ydb_client=self.ydb.client,
            disk_agent_count=1)
        self.nbs.start()

        self.metadata_service = MetadataServiceLauncher()
        self.metadata_service.start()
        self.base_disk_id_prefix = "base-"

        self.source_s3 = None
        self.destination_s3 = None
        self.s3_credentials_file = None

        if self.use_s3_source or self.use_s3_destination:
            working_dir = Path(get_unique_path_for_current_test(
                output_path=yatest_common.output_path(),
                sub_folder=""
            ))
            working_dir.mkdir(parents=True, exist_ok=True)
            self.s3_credentials_file = (working_dir / 's3_credentials.json')
            self.s3_credentials_file.write_text(json.dumps({"id": "test", "secret": "test"}))

        if self.use_s3_source:
            self.source_s3 = S3Launcher()
            self.source_s3.start()

        if self.use_s3_destination:
            if not self.use_source_s3_for_destination:
                self.destination_s3 = S3Launcher()
                self.destination_s3.start()
            else:
                self.destination_s3 = self.source_s3

        self.initial_cpl_disk_manager = DiskManagerLauncher(
            hostname="localhost0",
            ydb_port=self.ydb.port,
            nbs_port=self.nbs.port,
            nbs2_port=self.nbs.port,
            nbs3_port=self.nbs.port,
            root_certs_file=str(self._root_certs_file),
            idx=0,
            is_dataplane=False,
            disk_manager_binary_path=self.disk_manager_binary_path,
            cert_file=str(self._cert_file),
            cert_key_file=str(self._cert_key_file),
            base_disk_id_prefix=self.base_disk_id_prefix,
            creation_and_deletion_allowed_only_for_disks_with_id_prefix="",
            disable_disk_registry_based_disks=True,
            with_nemesis=False,
            metadata_url=self.metadata_service.url,
            s3_port=self.source_s3.port if self.source_s3 is not None else None,
        )
        self.initial_dpl_disk_manager = DiskManagerLauncher(
            hostname="localhost0",
            ydb_port=self.ydb.port,
            nbs_port=self.nbs.port,
            nbs2_port=self.nbs.port,
            nbs3_port=self.nbs.port,
            root_certs_file=str(self._root_certs_file),
            idx=0,
            is_dataplane=True,
            disk_manager_binary_path=self.disk_manager_binary_path,
            cert_file=str(self._cert_file),
            cert_key_file=str(self._cert_key_file),
            base_disk_id_prefix=self.base_disk_id_prefix,
            creation_and_deletion_allowed_only_for_disks_with_id_prefix="",
            disable_disk_registry_based_disks=True,
            migration_dst_ydb_port=self.secondary_ydb.port,
            dataplane_ydb_port=self.ydb.port,
            with_nemesis=False,
            metadata_url=self.metadata_service.url,
            s3_port=self.source_s3.port if self.source_s3 is not None else None,
            s3_credentials_file=str(self.s3_credentials_file) if self.s3_credentials_file is not None else None,
            migration_dst_s3_port=self.destination_s3.port if self.destination_s3 is not None else None,
            migration_dst_s3_credentials_file=str(self.s3_credentials_file) if self.s3_credentials_file is not None else None,
        )

        self.initial_cpl_disk_manager.start()
        self.initial_dpl_disk_manager.start()
        self.client_config_path = self.initial_cpl_disk_manager.client_config_file
        self.server_config_path = self.initial_cpl_disk_manager.config_file
        self.secondary_dpl_disk_manager = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with handle_process_lookup_error():
            self.initial_cpl_disk_manager.stop()
        with handle_process_lookup_error():
            self.initial_dpl_disk_manager.stop()
        if self.secondary_dpl_disk_manager is not None:
            with handle_process_lookup_error():
                self.secondary_dpl_disk_manager.stop()
        with handle_process_lookup_error():
            self.metadata_service.stop()
        with handle_process_lookup_error():
            self.nbs.stop()
        with handle_process_lookup_error():
            self.secondary_ydb.stop()
        with handle_process_lookup_error():
            self.ydb.stop()
        with handle_process_lookup_error():
            if self.source_s3 is not None:
                self.source_s3.stop()
        with handle_process_lookup_error():
            if self.destination_s3 is not None:
                self.destination_s3.stop()

    def admin(self, *args: str):
        return subprocess.check_output(
            [
                self.disk_manager_admin_binary_path,
                "--config", self.client_config_path,
                "--server-config", self.server_config_path,
                *args,
            ],
        ).decode()

    def blockstore_client(self, *args: str):
        return subprocess.check_output(
            [
                self.blockstore_client_binary_path,
                *args,
                "--secure-port", str(self.nbs.port), "--skip-cert-verification",
            ],
        ).decode()

    def create_new_disk(self, disk_id: str, size: int):
        _logger.info("Creating new disk with id %s, size %d", disk_id, size)
        self.admin(
            "disks",
            "create",
            "--cloud-id",
            self.default_cloud_id,
            "--folder-id",
            self.default_folder_id,
            "--zone-id", self.default_zone,
            "--size", str(size),
            "--id", disk_id
        )

    def get_disk(self, disk_id: str) -> _Disk:
        output = self.admin("disks", "get", "--id", disk_id)
        disk_info = json.loads(output)
        return self._Disk(
            block_size=disk_info["block_size"],
            blocks_count=disk_info["blocks_count"],
            id=disk_info["id"]
        )

    def fill_disk(self, disk_id: str) -> str:
        unique_test_dir = Path(get_unique_path_for_current_test(yatest_common.output_path(), ""))
        ensure_path_exists(str(unique_test_dir))
        data_file = unique_test_dir / "disk_data.bin"
        try:
            disk = self.get_disk(disk_id)
            subprocess.check_call([
                "dd",
                "if=/dev/urandom",
                f"of={data_file}",
                f"bs={disk.block_size}",
                f"count={disk.blocks_count}"
            ])

            self.blockstore_client(
                "writeblocks",
                "--disk-id", disk_id,
                "--start-index", "0",
                "--input", str(data_file),
            )

            return compute_checksum(data_file.as_posix())
        finally:
            data_file.unlink(missing_ok=True)

    def create_snapshot(self, source_disk_id: str, snapshot_id: str):
        self.admin(
            "snapshots",
            "create",
            "--id", snapshot_id,
            "--zone-id", self.default_zone,
            "--src-disk-id", source_disk_id,
            "--folder-id", "test",
        )

    def migrate_snapshot(self, snapshot_id: str, timeout_sec=360):
        stdout = self.admin(
            "snapshots",
            "schedule_migrate_snapshot_task",
            "--id",
            snapshot_id,
        )
        task_id = stdout.replace("Task: ", "").replace("\n", "")
        started_at = time.monotonic()
        while True:
            if time.monotonic() - started_at > timeout_sec:
                raise TimeoutError("Timed out snapshot migration")
            output = self.admin("tasks", "get", "--id", task_id)
            status = json.loads(output)["status"]
            if status == "finished":
                break

            time.sleep(0.1)

    def checksum_disk(self, disk_id: str):
        unique_test_dir = Path(get_unique_path_for_current_test(yatest_common.output_path(), ""))
        ensure_path_exists(str(unique_test_dir))
        data_file = unique_test_dir / "disk_data.bin"
        try:
            self.blockstore_client(
                "readblocks",
                "--disk-id", disk_id,
                "--start-index", "0",
                "--output",
                str(data_file),
                "--io-depth",
                "32",
                "--read-all"
            )
            return compute_checksum(str(data_file))
        finally:
            data_file.unlink(missing_ok=True)

    def create_disk_from_snapshot(self, snapshot_id: str, disk_id: str, size: int):
        self.admin(
            "disks",
            "create",
            "--folder-id",
            self.default_folder_id,
            "--cloud-id",
            self.default_cloud_id,
            "--zone-id",
            self.default_zone,
            "--size",
            str(size),
            "--src-snapshot-id",
            snapshot_id,
            "--id",
            disk_id,
        )

    def switch_dataplane_to_new_db(self):
        self.initial_dpl_disk_manager.stop_daemon()
        self.secondary_dpl_disk_manager = DiskManagerLauncher(
            hostname="localhost0",
            ydb_port=self.ydb.port,
            nbs_port=self.nbs.port,
            nbs2_port=self.nbs.port,
            nbs3_port=self.nbs.port,
            root_certs_file=str(self._root_certs_file),
            idx=0,
            is_dataplane=True,
            disk_manager_binary_path=self.disk_manager_binary_path,
            cert_file=str(self._cert_file),
            cert_key_file=str(self._cert_key_file),
            base_disk_id_prefix=self.base_disk_id_prefix,
            creation_and_deletion_allowed_only_for_disks_with_id_prefix="",
            disable_disk_registry_based_disks=True,
            dataplane_ydb_port=self.secondary_ydb.port,
            with_nemesis=False,
            metadata_url=self.metadata_service.url,
            s3_port=self.destination_s3.port if self.destination_s3 is not None else None,
            s3_credentials_file=str(self.s3_credentials_file) if self.s3_credentials_file is not None else None,
        )
        self.secondary_dpl_disk_manager.start()


@pytest.mark.parametrize(
    "use_s3_source, use_s3_destination, use_source_s3_for_destination",
    [
        (True, False, False),
        (False, True, False),
        (True, True, False),
        (True, True, True),
        (False, False, False),
    ]
)
def test_disk_manager_backup_restore(use_s3_source, use_s3_destination, use_source_s3_for_destination):
    with _MigrationTestSetup() as setup:
        disk_size = 1024 * 1024 * 1024
        initial_disk_id = "example"
        snapshot_id = "snapshot1"
        setup.create_new_disk(initial_disk_id, disk_size)
        checksum = setup.fill_disk("example")
        setup.create_snapshot(source_disk_id=initial_disk_id, snapshot_id=snapshot_id)
        setup.migrate_snapshot(snapshot_id)
        setup.switch_dataplane_to_new_db()
        new_disk = "new_example"
        setup.create_disk_from_snapshot(snapshot_id=snapshot_id, disk_id=new_disk, size=disk_size)
        setup.checksum_disk(new_disk)
        new_checksum = setup.checksum_disk(new_disk)
        assert new_checksum == checksum
