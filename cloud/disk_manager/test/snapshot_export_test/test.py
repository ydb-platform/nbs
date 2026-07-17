import hashlib
import json
import logging
import subprocess

from pathlib import Path
from typing import NamedTuple

import pytest

import contrib.ydb.tests.library.common.yatest_common as yatest_common

from contrib.ydb.tests.library.harness.kikimr_runner import (
    ensure_path_exists,
    get_unique_path_for_current_test,
)

from cloud.disk_manager.test.recipe.common import get_ydb_binary_path
from cloud.disk_manager.test.recipe.disk_manager_launcher import DiskManagerLauncher
from cloud.disk_manager.test.recipe.metadata_service_launcher import (
    MetadataServiceLauncher,
)
from cloud.disk_manager.test.recipe.nbs_launcher import NbsLauncher
from cloud.disk_manager.test.recipe.s3_launcher import S3Launcher
from cloud.disk_manager.test.recipe.ydb_launcher import YDBLauncher


_logger = logging.getLogger(__file__)


def compute_checksum(file_path: Path) -> str:
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_sha256.update(chunk)

    return hash_sha256.hexdigest()


class _SnapshotExportTestSetup:

    class _Disk(NamedTuple):
        block_size: int
        blocks_count: int
        id: str

    def __init__(self, use_s3: bool):
        self.use_s3 = use_s3

        certs_dir = Path(yatest_common.source_path("cloud/blockstore/tests/certs"))
        self._root_certs_file = certs_dir / "server.crt"
        self._cert_file = certs_dir / "server.crt"
        self._cert_key_file = certs_dir / "server.key"

        ydb_binary_path = get_ydb_binary_path()
        nbs_binary_path = yatest_common.binary_path(
            "cloud/blockstore/apps/server/nbsd"
        )
        disk_agent_binary_path = yatest_common.binary_path(
            "cloud/blockstore/apps/disk_agent/diskagentd"
        )
        self.disk_manager_binary_path = yatest_common.binary_path(
            "cloud/disk_manager/cmd/disk-manager/disk-manager"
        )
        self.blockstore_client_binary_path = yatest_common.binary_path(
            "cloud/blockstore/apps/client/blockstore-client"
        )
        self.disk_manager_admin_binary_path = yatest_common.binary_path(
            "cloud/disk_manager/cmd/disk-manager-admin/disk-manager-admin"
        )

        self.working_dir = Path(
            get_unique_path_for_current_test(
                output_path=yatest_common.output_path(),
                sub_folder="snapshot_export",
            )
        )
        ensure_path_exists(str(self.working_dir))

        self.ydb = YDBLauncher(ydb_binary_path=ydb_binary_path)
        self.ydb.start()

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
            disk_agent_count=1,
        )
        self.nbs.start()

        self.metadata_service = MetadataServiceLauncher()
        self.metadata_service.start()

        self.s3 = None
        self.s3_credentials_file = None
        if self.use_s3:
            self.s3_credentials_file = self.working_dir / "s3_credentials.json"
            self.s3_credentials_file.write_text(
                json.dumps({"id": "test", "secret": "test"})
            )
            self.s3 = S3Launcher()
            self.s3.start()

        common_parameters = dict(
            hostname="localhost0",
            ydb_port=self.ydb.port,
            nbs_port=self.nbs.port,
            nbs2_port=self.nbs.port,
            nbs3_port=self.nbs.port,
            nbs4_port=self.nbs.port,
            nbs5_port=self.nbs.port,
            root_certs_file=str(self._root_certs_file),
            cert_file=str(self._cert_file),
            cert_key_file=str(self._cert_key_file),
            idx=0,
            disk_manager_binary_path=self.disk_manager_binary_path,
            base_disk_id_prefix="base-",
            creation_and_deletion_allowed_only_for_disks_with_id_prefix="",
            disable_disk_registry_based_disks=True,
            with_nemesis=False,
            metadata_url=self.metadata_service.url,
        )

        self.controlplane_disk_manager = DiskManagerLauncher(
            **common_parameters,  # type: ignore
            is_dataplane=False,
            s3_port=self.s3.port if self.s3 is not None else None,
        )
        self.dataplane_disk_manager = DiskManagerLauncher(
            **common_parameters,  # type: ignore
            is_dataplane=True,
            dataplane_ydb_port=self.ydb.port,
            s3_port=self.s3.port if self.s3 is not None else None,
            s3_credentials_file=(
                str(self.s3_credentials_file)
                if self.s3_credentials_file is not None
                else None
            ),
        )

        self.controlplane_disk_manager.start()
        self.dataplane_disk_manager.start()
        self.client_config_path = self.controlplane_disk_manager.client_config_file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.controlplane_disk_manager.stop_daemon()
        self.dataplane_disk_manager.stop_daemon()
        self.metadata_service.stop_service()
        self.nbs.stop_service()
        self.ydb.stop_service()
        if self.s3 is not None:
            self.s3.stop_service()

    def _admin_command(self, server_config_path: str, *args: str) -> list[str]:
        return [
            self.disk_manager_admin_binary_path,
            "--config",
            self.client_config_path,
            "--server-config",
            server_config_path,
            *args,
        ]

    def admin(self, *args: str) -> str:
        return subprocess.check_output(
            self._admin_command(
                self.controlplane_disk_manager.config_file,
                *args,
            ),
        ).decode()

    def export_snapshot(self, snapshot_id: str, output_file: Path, *args: str):
        with open(output_file, "wb") as stdout:
            subprocess.check_call(
                self._admin_command(
                    self.dataplane_disk_manager.config_file,
                    "snapshots",
                    "export",
                    "--id",
                    snapshot_id,
                    *args,
                ),
                stdout=stdout,
            )

    def blockstore_client(self, *args: str) -> str:
        return subprocess.check_output(
            [
                self.blockstore_client_binary_path,
                *args,
                "--secure-port",
                str(self.nbs.port),
                "--skip-cert-verification",
            ],
        ).decode()

    def create_new_disk(self, disk_id: str, size: int) -> _Disk:
        _logger.info("Creating new disk with id %s, size %d", disk_id, size)
        self.admin(
            "disks",
            "create",
            "--cloud-id",
            "cloud",
            "--folder-id",
            "folder",
            "--zone-id",
            "zone-a",
            "--size",
            str(size),
            "--id",
            disk_id,
        )
        return self.get_disk(disk_id)

    def get_disk(self, disk_id: str) -> _Disk:
        output = self.admin("disks", "get", "--id", disk_id)
        disk_info = json.loads(output)
        return self._Disk(
            block_size=disk_info["block_size"],
            blocks_count=disk_info["blocks_count"],
            id=disk_info["id"],
        )

    def write_random_blocks(
        self,
        disk_id: str,
        start_block_index: int,
        blocks_count: int,
    ):
        disk = self.get_disk(disk_id)
        data_file = self.working_dir / (
            f"{disk_id}_{start_block_index}_{blocks_count}.bin"
        )
        try:
            subprocess.check_call(
                [
                    "dd",
                    "if=/dev/urandom",
                    f"of={data_file}",
                    f"bs={disk.block_size}",
                    f"count={blocks_count}",
                    "status=none",
                ]
            )

            self.blockstore_client(
                "writeblocks",
                "--disk-id",
                disk_id,
                "--start-index",
                str(start_block_index),
                "--input",
                str(data_file),
            )
        finally:
            data_file.unlink(missing_ok=True)

    def create_snapshot(self, src_disk_id: str, snapshot_id: str):
        self.admin(
            "snapshots",
            "create",
            "--id",
            snapshot_id,
            "--zone-id",
            "zone-a",
            "--src-disk-id",
            src_disk_id,
            "--folder-id",
            "folder",
        )

    def checksum_disk(self, disk_id: str) -> str:
        data_file = self.working_dir / f"{disk_id}.raw"
        try:
            self.blockstore_client(
                "readblocks",
                "--disk-id",
                disk_id,
                "--start-index",
                "0",
                "--output",
                str(data_file),
                "--io-depth",
                "32",
                "--read-all",
            )
            return compute_checksum(data_file)
        finally:
            data_file.unlink(missing_ok=True)

    def export_snapshot_checksum(self, snapshot_id: str, file_name: str) -> str:
        output_file = self.working_dir / file_name
        self.export_snapshot(snapshot_id, output_file, "--read-workers", "4")
        return compute_checksum(output_file)

    def export_snapshot_partitions_checksum(
        self,
        snapshot_id: str,
        file_name: str,
        partition_count: int,
    ) -> str:
        output_file = self.working_dir / file_name
        output_file.unlink(missing_ok=True)

        with open(output_file, "wb") as output:
            for partition in range(1, partition_count + 1):
                partition_file = self.working_dir / (
                    f"{file_name}.partition-{partition}"
                )
                self.export_snapshot(
                    snapshot_id,
                    partition_file,
                    "--partition",
                    str(partition),
                    "--partition-count",
                    str(partition_count),
                    "--read-workers",
                    "4",
                )
                with open(partition_file, "rb") as partition_data:
                    for chunk in iter(lambda: partition_data.read(1024 * 1024), b""):
                        output.write(chunk)

        return compute_checksum(output_file)


@pytest.mark.parametrize("use_s3", [False, True], ids=["ydb", "s3"])
def test_snapshot_export_downloads_snapshot_and_preserves_data(use_s3):
    with _SnapshotExportTestSetup(use_s3=use_s3) as setup:
        disk_size = 10 * 1024 * 1024
        disk_id = "export-source"
        base_snapshot_id = "export-base-snapshot"
        incremental_snapshot_id = "export-incremental-snapshot"

        disk = setup.create_new_disk(disk_id, disk_size)
        one_mib_blocks = 1024 * 1024 // disk.block_size

        setup.write_random_blocks(disk_id, 0, one_mib_blocks)
        setup.write_random_blocks(
            disk_id,
            disk.blocks_count - one_mib_blocks,
            one_mib_blocks,
        )
        base_checksum = setup.checksum_disk(disk_id)
        setup.create_snapshot(disk_id, base_snapshot_id)

        setup.write_random_blocks(disk_id, one_mib_blocks, one_mib_blocks)
        setup.write_random_blocks(
            disk_id,
            disk.blocks_count // 2,
            one_mib_blocks,
        )
        incremental_checksum = setup.checksum_disk(disk_id)
        assert incremental_checksum != base_checksum
        setup.create_snapshot(disk_id, incremental_snapshot_id)

        assert setup.export_snapshot_checksum(
            base_snapshot_id,
            "base_snapshot.raw",
        ) == base_checksum
        assert (setup.working_dir / "base_snapshot.raw").stat().st_size == disk_size

        assert setup.export_snapshot_checksum(
            incremental_snapshot_id,
            "incremental_snapshot.raw",
        ) == incremental_checksum
        assert (
            setup.working_dir / "incremental_snapshot.raw"
        ).stat().st_size == disk_size

        assert setup.export_snapshot_partitions_checksum(
            incremental_snapshot_id,
            "incremental_snapshot_from_partitions.raw",
            partition_count=2,
        ) == incremental_checksum
        assert (
            setup.working_dir / "incremental_snapshot_from_partitions.raw"
        ).stat().st_size == disk_size
