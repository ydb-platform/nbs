import hashlib
import json
import logging
import subprocess
import time

from pathlib import Path
from typing import NamedTuple

import pytest

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists

from cloud.disk_manager.test.recipe.common import get_ydb_binary_path
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


class _MigrationTestSetup:

    class _Disk(NamedTuple):
        block_size: int
        blocks_count: int
        id: str

    def __init__(self, use_s3_as_src: bool = False, use_s3_as_dst: bool = False):
        self.use_s3_as_src = use_s3_as_src
        self.use_s3_as_dst = use_s3_as_dst

        certs_dir = Path(yatest_common.source_path("cloud/blockstore/tests/certs"))
        self._root_certs_file = certs_dir / "server.crt"
        _logger.info(certs_dir.exists())
        self._cert_file = certs_dir / "server.crt"
        _logger.info(self._cert_file.exists())
        self._cert_key_file = certs_dir / "server.key"
        _logger.info(self._cert_key_file.exists())

        ydb_binary_path = get_ydb_binary_path()
        nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")
        disk_agent_binary_path = yatest_common.binary_path("cloud/blockstore/apps/disk_agent/diskagentd")
        self.disk_manager_binary_path = yatest_common.binary_path("cloud/disk_manager/cmd/disk-manager/disk-manager")
        self.blockstore_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
        self.disk_manager_admin_binary_path = yatest_common.binary_path("cloud/disk_manager/cmd/disk-manager-admin/disk-manager-admin")

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

        self.src_s3 = None
        self.dst_s3 = None
        self.s3_credentials_file = None

        if self.use_s3_as_src or self.use_s3_as_dst:
            working_dir = Path(get_unique_path_for_current_test(
                output_path=yatest_common.output_path(),
                sub_folder=""
            ))
            working_dir.mkdir(parents=True, exist_ok=True)
            self.s3_credentials_file = (working_dir / 's3_credentials.json')
            self.s3_credentials_file.write_text(json.dumps({"id": "test", "secret": "test"}))

        if self.use_s3_as_src:
            self.src_s3 = S3Launcher()
            self.src_s3.start()

        if self.use_s3_as_dst:
            self.dst_s3 = S3Launcher()
            self.dst_s3.start()

        self.common_parameters = dict(
            hostname="localhost0",
            ydb_port=self.ydb.port,
            nbs_port=self.nbs.port,
            nbs2_port=self.nbs.port,
            nbs3_port=self.nbs.port,
            root_certs_file=str(self._root_certs_file),
            cert_file=str(self._cert_file),
            cert_key_file=str(self._cert_key_file),
            idx=0,
            disk_manager_binary_path=self.disk_manager_binary_path,
            base_disk_id_prefix=self.base_disk_id_prefix,
            creation_and_deletion_allowed_only_for_disks_with_id_prefix="",
            disable_disk_registry_based_disks=True,
            with_nemesis=False,
            metadata_url=self.metadata_service.url,
        )

        self.initial_cpl_disk_manager = DiskManagerLauncher(
            **self.common_parameters,  # type: ignore
            is_dataplane=False,
            s3_port=self.src_s3.port if self.src_s3 is not None else None,
        )
        self.initial_dpl_disk_manager = DiskManagerLauncher(
            **self.common_parameters,  # type: ignore
            is_dataplane=True,
            migration_dst_ydb_port=self.secondary_ydb.port,
            dataplane_ydb_port=self.ydb.port,
            s3_port=self.src_s3.port if self.src_s3 is not None else None,
            s3_credentials_file=str(self.s3_credentials_file) if self.s3_credentials_file is not None else None,
            migration_dst_s3_port=self.dst_s3.port if self.dst_s3 is not None else None,
            migration_dst_s3_credentials_file=str(self.s3_credentials_file) if self.s3_credentials_file is not None else None,
        )

        self.initial_cpl_disk_manager.start()
        self.initial_dpl_disk_manager.start()
        self.client_config_path = self.initial_cpl_disk_manager.client_config_file
        self.server_config_path = self.initial_cpl_disk_manager.config_file
        self.secondary_dpl_disk_manager = None
        self.initial_dpl_pid = self.initial_dpl_disk_manager.pid

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            DiskManagerLauncher.stop()
        except ProcessLookupError as e:
            # stop() tries to terminate all disk-manager processes created with DiskManagerLauncher
            # and raises ProcessLookupError, since one of processes is already terminated.
            # Compare pids list for terminated process pid to ensure we do not miss process crashes.
            pids = getattr(e, "pids", [])
            if len(pids) != 1:
                _logger.error(
                    "Unexpected %s occurred while stopping Disk-Manager pids: '%s'",
                    e.__class__.__name__,
                    ",".join(pids)
                )
                raise e
            [pid] = pids
            if self.initial_dpl_pid != pid:
                raise e

        MetadataServiceLauncher.stop()
        NbsLauncher.stop()
        YDBLauncher.stop()
        if self.src_s3 is not None or self.dst_s3 is not None:
            S3Launcher.stop()

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
            "--cloud-id", "cloud",
            "--folder-id", "folder",
            "--zone-id", "zone-a",
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

    def create_snapshot(self, src_disk_id: str, snapshot_id: str):
        self.admin(
            "snapshots",
            "create",
            "--id", snapshot_id,
            "--zone-id", "zone-a",
            "--src-disk-id", src_disk_id,
            "--folder-id", "folder",
        )

    def migrate_snapshot(self, snapshot_id: str, timeout_sec=360):
        stdout = self.admin(
            "snapshots",
            "schedule_migrate_snapshot_task",
            "--id", snapshot_id,
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

            time.sleep(1)

    def start_database_migration(self):
        stdout = self.admin(
            "snapshots",
            "schedule_migrate_snapshot_database_task",
        )
        task_id = stdout.replace("Task: ", "").replace("\n", "")
        return task_id

    def finish_database_migration(self, task_id: str):
        self.admin("tasks", "cancel", task_id)

    def dependencies_finished(self, task_id: str) -> bool:
        stdout = self.admin("tasks", "get", task_id)
        dependencies = json.loads(stdout)["dependencies"]
        return len(dependencies) == 0

    def list_snapshots(self) -> list[str]:
        stdout = self.admin("snapshots", "list")
        return stdout.splitlines()

    def checksum_disk(self, disk_id: str) -> str:
        unique_test_dir = Path(get_unique_path_for_current_test(yatest_common.output_path(), ""))
        ensure_path_exists(str(unique_test_dir))
        data_file = unique_test_dir / "disk_data.bin"
        try:
            self.blockstore_client(
                "readblocks",
                "--disk-id", disk_id,
                "--start-index", "0",
                "--output", str(data_file),
                "--io-depth", "32",
                "--read-all"
            )
            return compute_checksum(str(data_file))
        finally:
            data_file.unlink(missing_ok=True)

    def create_disk_from_snapshot(self, snapshot_id: str, disk_id: str, size: int):
        self.admin(
            "disks",
            "create",
            "--folder-id", "folder",
            "--cloud-id", "cloud",
            "--zone-id", "zone-a",
            "--size", str(size),
            "--src-snapshot-id", snapshot_id,
            "--id", disk_id,
        )

    def switch_dataplane_to_new_db(self):
        self.initial_dpl_disk_manager.stop_daemon()
        self.secondary_dpl_disk_manager = DiskManagerLauncher(
            **self.common_parameters,  # type: ignore
            is_dataplane=True,
            dataplane_ydb_port=self.secondary_ydb.port,
            s3_port=self.dst_s3.port if self.dst_s3 is not None else None,
            s3_credentials_file=str(self.s3_credentials_file) if self.s3_credentials_file is not None else None,
        )
        self.secondary_dpl_disk_manager.start()


@pytest.mark.parametrize(
    "use_s3_as_src, use_s3_as_dst",
    [
        (True, False),
        (False, True),
        (True, True),
        (False, False),
    ]
)
def test_disk_manager_single_snapshot_migration(use_s3_as_src, use_s3_as_dst):
    with _MigrationTestSetup(
        use_s3_as_src=use_s3_as_src,
        use_s3_as_dst=use_s3_as_dst,
    ) as setup:
        disk_size = 16 * 1024 * 1024
        initial_disk_id = "example"
        snapshot_id = "snapshot1"
        setup.create_new_disk(initial_disk_id, disk_size)
        checksum = setup.fill_disk("example")
        setup.create_snapshot(src_disk_id=initial_disk_id, snapshot_id=snapshot_id)
        setup.migrate_snapshot(snapshot_id)
        setup.switch_dataplane_to_new_db()
        new_disk = "new_example"
        setup.create_disk_from_snapshot(snapshot_id=snapshot_id, disk_id=new_disk, size=disk_size)
        new_checksum = setup.checksum_disk(new_disk)
        assert new_checksum == checksum

@pytest.mark.parametrize(
    "use_s3_as_src, use_s3_as_dst",
    [
        (True, False),
        (False, True),
        (True, True),
        (False, False),
    ]
)
def test_disk_manager_dataplane_database_migration(use_s3_as_src, use_s3_as_dst):
    with _MigrationTestSetup(
        use_s3_as_src=use_s3_as_src,
        use_s3_as_dst=use_s3_as_dst,
    ) as setup:
        initial_data_count = 10
        disks = [f"disk_{i}" for i in range(initial_data_count)]
        snapshots = [f"snapshot_{i}" for i in range(initial_data_count)]
        new_disks_for_initial = [f"new_disk_{i}" for i in range(initial_data_count)]
        checksums = []
        size = 16 * 1024 * 1024

        # Create disks and snapshots before migration
        for snapshot, disk in list(zip(snapshots, disks))[:initial_data_count // 2]:
            setup.create_new_disk(disk, size)
            checksums.append(setup.fill_disk(disk))
            setup.create_snapshot(src_disk_id=disk, snapshot_id=snapshot)

        # Wait for snapshots to be created
        while len(setup.list_snapshots()) != initial_data_count // 2:
            time.sleep(1)

        # Prepare disks to create snapshots from during migration
        for disk in disks[initial_data_count // 2:]:
            setup.create_new_disk(disk, size)
            checksums.append(setup.fill_disk(disk))

        task_id = setup.start_database_migration()

        # Create snapshots during migration
        for snapshot, disk in list(zip(snapshots, disks))[initial_data_count // 2:]:
            setup.create_snapshot(src_disk_id=disk, snapshot_id=snapshot)

        # Wait for snapshots to be created
        while len(setup.list_snapshots()) != initial_data_count:
            time.sleep(1)

        while not setup.dependencies_finished(task_id):
            time.sleep(1)

        time.sleep(10)
        setup.finish_database_migration(task_id)
        setup.switch_dataplane_to_new_db()
        for new_disk, checksum in zip(new_disks_for_initial, checksums):
            setup.create_disk_from_snapshot(snapshot_id=snapshot, disk_id=new_disk, size=size)
            new_checksum = setup.checksum_disk(new_disk)
            assert new_checksum == checksum
