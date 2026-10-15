import hashlib
import json
import logging
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pathlib import Path
from typing import NamedTuple

import pytest
import boto3

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

    def __init__(self, use_s3: bool, enable_backup: bool = False):
        self.use_s3 = use_s3
        self.services_stopped = False

        certs_dir = Path(yatest_common.source_path("cloud/blockstore/tests/certs"))
        self._root_certs_file = certs_dir / "server.crt"
        self._cert_file = certs_dir / "server.crt"
        self._cert_key_file = certs_dir / "server.key"

        ydb_binary_path = get_ydb_binary_path()
        nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")
        disk_agent_binary_path = yatest_common.binary_path("cloud/blockstore/apps/disk_agent/diskagentd")
        self.disk_manager_binary_path = yatest_common.binary_path("cloud/disk_manager/cmd/disk-manager/disk-manager")
        self.blockstore_client_binary_path = yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
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
            self.s3_credentials_file.write_text(json.dumps({"id": "test", "secret": "test"}))
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
            s3_credentials_file=(str(self.s3_credentials_file) if self.s3_credentials_file is not None else None),
        )

        self.backup_s3 = None
        if enable_backup:
            self.backup_s3 = S3Launcher()
            self.backup_s3.start()
            self.backup_credentials = self.working_dir / "backup_credentials.json"
            self.backup_credentials.write_text(json.dumps({"id": "backup", "secret": "backup"}))
            self.backup_bucket = "snapshot-source-backups"
            self.backup_client = boto3.client(
                "s3",
                endpoint_url=f"http://localhost:{self.backup_s3.port}",
                region_name="ru-central1",
                aws_access_key_id="backup",
                aws_secret_access_key="backup",
            )
            self.backup_client.create_bucket(
                Bucket=self.backup_bucket,
                CreateBucketConfiguration={"LocationConstraint": "ru-central1"},
            )
            backup_config = f"""
SnapshotStorageBackupConfig {{
    S3Config {{
        Endpoint: "http://localhost:{self.backup_s3.port}"
        CredentialsFilePath: {json.dumps(str(self.backup_credentials))}
        Region: "ru-central1"
    }}
    S3Bucket: "{self.backup_bucket}"
    S3KeyPrefix: "source"
}}
"""
            for launcher in [self.controlplane_disk_manager, self.dataplane_disk_manager]:
                path = Path(launcher.config_file)
                config = path.read_text().replace(
                    "SnapshotsConfig: <",
                    'SnapshotsConfig: <\n    ScheduleBackupSnapshotTasksScheduleInterval: "1s"',
                )
                config = config.replace(
                    "ImagesConfig: <",
                    'ImagesConfig: <\n    ScheduleBackupImageTasksScheduleInterval: "1s"',
                )
                path.write_text(config + backup_config)
            # Required server protobuf fields remain inert. No snapshot
            # persistence config is present; service endpoints are unreachable.
            self.backup_export_config = self.working_dir / "backup_export_config.txt"
            self.backup_export_config.write_text(backup_config + '''
TasksConfig {}
LoggingConfig {}
MonitoringConfig { Revision: 1 Version: "test" }
AuthConfig { MetadataUrl: "http://localhost:1" }
PersistenceConfig { Endpoint: "localhost:1" Database: "/unavailable" }
''')

        self.controlplane_disk_manager.start()
        self.dataplane_disk_manager.start()
        self.client_config_path = self.controlplane_disk_manager.client_config_file

        # The destination bucket is independent of the source storage config.
        # YDB-only sources have no source S3 configuration at all.
        self.destination_s3 = self.s3
        if self.destination_s3 is None:
            self.destination_s3 = S3Launcher()
            self.destination_s3.start()
        self.destination_endpoint = f"http://localhost:{self.destination_s3.port}"
        self.destination_credentials = self.working_dir / "destination_credentials.json"
        self.destination_credentials.write_text(
            json.dumps({"id": "destination-id", "secret": "destination-secret", "session_token": "temporary-token"})
        )
        self.destination_client = boto3.client(
            "s3",
            endpoint_url=self.destination_endpoint,
            region_name="destination-region",
            aws_access_key_id="destination-id",
            aws_secret_access_key="destination-secret",
            aws_session_token="temporary-token",
        )
        self.destination_bucket = "snapshot-exports"
        self.destination_client.create_bucket(
            Bucket=self.destination_bucket,
            CreateBucketConfiguration={"LocationConstraint": "destination-region"},
        )

    def __enter__(self):
        return self

    def stop_disk_services(self):
        if self.services_stopped:
            return
        self.controlplane_disk_manager.stop_daemon()
        self.dataplane_disk_manager.stop_daemon()
        self.metadata_service.stop_service()
        self.nbs.stop_service()
        self.ydb.stop_service()
        self.services_stopped = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_disk_services()
        if self.backup_s3 is not None:
            self.backup_s3.stop_service()
        if self.s3 is not None:
            self.s3.stop_service()
        else:
            self.destination_s3.stop_service()

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
                [
                    self.disk_manager_admin_binary_path,
                    "--server-config",
                    self.dataplane_disk_manager.config_file,
                    "snapshots",
                    "export",
                    "--id",
                    snapshot_id,
                    *args,
                ],
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
        data_file = self.working_dir / (f"{disk_id}_{start_block_index}_{blocks_count}.bin")
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
                partition_file = self.working_dir / (f"{file_name}.partition-{partition}")
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

    def s3_export_command(
        self,
        snapshot_id: str,
        prefix: str,
        endpoint=None,
        server_config=None,
        source_args=(),
        resource_group="snapshots",
    ):
        return [
            self.disk_manager_admin_binary_path,
            "--server-config",
            str(server_config or self.dataplane_disk_manager.config_file),
            resource_group,
            "export",
            "--id",
            snapshot_id,
            "--s3-endpoint",
            endpoint or self.destination_endpoint,
            "--s3-bucket",
            self.destination_bucket,
            "--s3-credentials-file",
            str(self.destination_credentials),
            "--s3-prefix",
            prefix,
            "--s3-region",
            "destination-region",
            "--s3-part-size",
            "4MiB",
            "--s3-max-retries",
            "1",
            "--s3-request-timeout",
            "10s",
            "--read-workers",
            "4",
        ] + list(source_args)

    def export_snapshot_to_s3(
        self,
        snapshot_id: str,
        prefix: str,
        endpoint=None,
        check=True,
        server_config=None,
        source_args=(),
        resource_group="snapshots",
    ):
        return subprocess.run(
            self.s3_export_command(snapshot_id, prefix, endpoint, server_config, source_args, resource_group),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=check,
            timeout=120,
        )

    def backup_snapshot(self, snapshot_id: str):
        self.backup_resource("snapshots", snapshot_id)

    def backup_resource(self, resource_group: str, resource_id: str):
        self.admin(resource_group, "backup", "--id", resource_id)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if json.loads(self.admin(resource_group, "get", "--id", resource_id)).get("backup_completed"):
                return
            time.sleep(1)
        raise AssertionError(f"Backup of {resource_id} did not complete")

    def backup_fingerprint(self):
        objects = {}
        paginator = self.backup_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.backup_bucket):
            for item in page.get("Contents", []):
                key = item["Key"]
                response = self.backup_client.get_object(Bucket=self.backup_bucket, Key=key)
                with response["Body"] as body:
                    objects[key] = (hashlib.sha256(body.read()).hexdigest(), response["Metadata"])
        return objects

    def destination_keys(self, prefix: str):
        result = self.destination_client.list_objects_v2(Bucket=self.destination_bucket, Prefix=prefix)
        return [obj["Key"] for obj in result.get("Contents", [])]

    def verify_s3_backup(self, manifest_key: str, expected_checksum: str, disk_size: int):
        response = self.destination_client.get_object(Bucket=self.destination_bucket, Key=manifest_key)
        assert response["ContentType"] == "application/json"
        assert "ContentEncoding" not in response
        with response["Body"] as body:
            manifest = json.loads(body.read())
        assert manifest["format_version"] == 1
        assert manifest["format"] == "partitioned-raw-image"
        assert manifest["size"] == disk_size
        assert manifest["part_count"] == len(manifest["parts"]) == 3
        assert manifest["max_part_size"] == 4 * 1024 * 1024
        assert manifest["raw_data_interpretation"] == "plaintext"
        backup_prefix = manifest_key.removesuffix("manifest.json")
        assert backup_prefix.endswith(f"{manifest['snapshot_id']}-backup-{manifest['backup_uuid']}/")
        offset = 0
        checksum = hashlib.sha256()
        for number, part in enumerate(manifest["parts"], 1):
            assert part["number"] == number
            assert part["offset"] == offset
            response = self.destination_client.get_object(
                Bucket=self.destination_bucket, Key=backup_prefix + part["key"]
            )
            assert response["ContentType"] == "application/octet-stream"
            assert "ContentEncoding" not in response
            with response["Body"] as body:
                data = body.read()
            assert len(data) == part["length"] <= manifest["max_part_size"]
            assert hashlib.sha256(data).hexdigest() == part["sha256"]
            checksum.update(data)
            offset += len(data)
        assert offset == disk_size
        assert checksum.hexdigest() == expected_checksum
        return manifest


@contextmanager
def fail_second_part_proxy(destination_endpoint: str, started=None, release=None):
    """Forward to the S3 launcher but reject the second raw object."""

    class Handler(BaseHTTPRequestHandler):
        def forward(self):
            if self.command == "PUT" and self.path.endswith("part-00000002.raw"):
                if started is not None:
                    started.set()
                    release.wait(timeout=30)
                body = b"<Error><Code>AccessDenied</Code><Message>injected</Message></Error>"
                self.send_response(403)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            request = urllib.request.Request(
                destination_endpoint + self.path,
                data=data if self.command == "PUT" else None,
                headers={key: value for key, value in self.headers.items() if key.lower() != "host"},
                method=self.command,
            )
            try:
                response = urllib.request.urlopen(request, timeout=10)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                body = response.read()
                self.send_response(response.status)
                for key, value in response.headers.items():
                    if key.lower() not in {"connection", "transfer-encoding", "content-length"}:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        do_HEAD = forward
        do_PUT = forward

    port_manager = yatest_common.PortManager()
    server = ThreadingHTTPServer(("localhost", port_manager.get_port()), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://localhost:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


@pytest.mark.parametrize("use_s3", [False, True], ids=["ydb", "s3"])
def test_snapshot_export_downloads_snapshot_and_preserves_data(use_s3):
    with _SnapshotExportTestSetup(use_s3=use_s3) as setup:
        disk_size = 12 * 1024 * 1024
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

        assert (
            setup.export_snapshot_checksum(
                base_snapshot_id,
                "base_snapshot.raw",
            )
            == base_checksum
        )
        assert (setup.working_dir / "base_snapshot.raw").stat().st_size == disk_size

        assert (
            setup.export_snapshot_checksum(
                incremental_snapshot_id,
                "incremental_snapshot.raw",
            )
            == incremental_checksum
        )
        assert (setup.working_dir / "incremental_snapshot.raw").stat().st_size == disk_size

        assert (
            setup.export_snapshot_partitions_checksum(
                incremental_snapshot_id,
                "incremental_snapshot_from_partitions.raw",
                partition_count=2,
            )
            == incremental_checksum
        )
        assert (setup.working_dir / "incremental_snapshot_from_partitions.raw").stat().st_size == disk_size

        for snapshot_id, checksum in [
            (base_snapshot_id, base_checksum),
            (incremental_snapshot_id, incremental_checksum),
            (incremental_snapshot_id, incremental_checksum),
        ]:
            existing = set(setup.destination_keys("successful/"))
            result = setup.export_snapshot_to_s3(snapshot_id, "successful")
            assert result.stdout == b""
            new_keys = set(setup.destination_keys("successful/")) - existing
            assert len(new_keys) == 4
            manifest_keys = [key for key in new_keys if key.endswith("manifest.json")]
            assert len(manifest_keys) == 1
            assert manifest_keys[0].encode() in result.stderr
            manifest = setup.verify_s3_backup(manifest_keys[0], checksum, disk_size)
            assert manifest["snapshot_id"] == snapshot_id
        assert len(setup.destination_keys("successful/")) == 12

        with fail_second_part_proxy(setup.destination_endpoint) as endpoint:
            result = setup.export_snapshot_to_s3(incremental_snapshot_id, "failed", endpoint=endpoint, check=False)
        assert result.returncode != 0
        assert result.stdout == b""
        assert b"part 2/3" in result.stderr
        failed_keys = setup.destination_keys("failed/")
        assert len(failed_keys) == 1
        assert failed_keys[0].endswith("part-00000001.raw")
        assert not any(key.endswith("manifest.json") for key in failed_keys)


@pytest.mark.parametrize("use_s3", [False, True], ids=["ydb", "s3"])
def test_snapshot_export_reads_real_backup_without_disk_services(use_s3):
    with _SnapshotExportTestSetup(use_s3=use_s3, enable_backup=True) as setup:
        disk_size = 12 * 1024 * 1024
        disk_id = "backup-export-source"
        base_id = "backup-export-base"
        incremental_id = "backup-export-incremental"
        disk = setup.create_new_disk(disk_id, disk_size)
        mib_blocks = 1024 * 1024 // disk.block_size
        setup.write_random_blocks(disk_id, 0, mib_blocks)
        setup.write_random_blocks(disk_id, disk.blocks_count - mib_blocks, mib_blocks)
        base_checksum = setup.checksum_disk(disk_id)
        setup.create_snapshot(disk_id, base_id)
        setup.backup_snapshot(base_id)
        setup.write_random_blocks(disk_id, 2 * mib_blocks, mib_blocks)
        incremental_checksum = setup.checksum_disk(disk_id)
        assert incremental_checksum != base_checksum
        setup.create_snapshot(disk_id, incremental_id)
        setup.backup_snapshot(incremental_id)

        original_source = setup.backup_fingerprint()
        assert original_source
        # The backup bucket and destination remain available, disk services do not.
        setup.stop_disk_services()
        source_args = ("--source=backup", f"--src-disk-id={disk_id}")
        for snapshot_id, checksum in [(base_id, base_checksum), (incremental_id, incremental_checksum)]:
            prefix = f"backup-success/{snapshot_id}"
            result = setup.export_snapshot_to_s3(
                snapshot_id,
                prefix,
                server_config=setup.backup_export_config,
                source_args=source_args,
            )
            assert result.stdout == b""
            keys = setup.destination_keys(prefix)
            manifests = [key for key in keys if key.endswith("manifest.json")]
            assert len(keys) == 4 and len(manifests) == 1
            manifest = setup.verify_s3_backup(manifests[0], checksum, disk_size)
            assert manifest["snapshot_id"] == snapshot_id
            assert manifest["encryption_mode"] == "NO_ENCRYPTION"

        # The same backup source supports stdout and chunk-aligned partitions.
        output_file = setup.working_dir / "backup_partitioned.raw"
        with output_file.open("wb") as output:
            for partition in [1, 2]:
                subprocess.check_call(
                    [
                        setup.disk_manager_admin_binary_path,
                        "--server-config",
                        str(setup.backup_export_config),
                        "snapshots",
                        "export",
                        "--id",
                        incremental_id,
                        *source_args,
                        "--partition",
                        str(partition),
                        "--partition-count=2",
                    ],
                    stdout=output,
                )
        assert compute_checksum(output_file) == incremental_checksum

        for snapshot_id, args in [
            ("missing", source_args),
            (incremental_id, ("--source=backup", "--src-disk-id=wrong-disk")),
        ]:
            result = setup.export_snapshot_to_s3(
                snapshot_id,
                "backup-missing",
                check=False,
                server_config=setup.backup_export_config,
                source_args=args,
            )
            assert result.returncode != 0
        assert setup.destination_keys("backup-missing/") == []

        with fail_second_part_proxy(setup.destination_endpoint) as endpoint:
            result = setup.export_snapshot_to_s3(
                incremental_id,
                "backup-failed",
                endpoint=endpoint,
                check=False,
                server_config=setup.backup_export_config,
                source_args=source_args,
            )
        assert result.returncode != 0
        failed_keys = setup.destination_keys("backup-failed/")
        assert len(failed_keys) == 1
        assert failed_keys[0].endswith("part-00000001.raw")

        started, release = threading.Event(), threading.Event()
        with fail_second_part_proxy(setup.destination_endpoint, started, release) as endpoint:
            process = subprocess.Popen(
                setup.s3_export_command(
                    incremental_id,
                    "backup-cancelled",
                    endpoint,
                    setup.backup_export_config,
                    source_args,
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                assert started.wait(timeout=30), "Export did not reach the second part"
                process.send_signal(signal.SIGTERM)
                stdout, _ = process.communicate(timeout=30)
                assert process.returncode != 0
                assert stdout == b""
            finally:
                release.set()
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=10)
        cancelled_keys = setup.destination_keys("backup-cancelled/")
        assert len(cancelled_keys) == 1
        assert cancelled_keys[0].endswith("part-00000001.raw")
        assert setup.backup_fingerprint() == original_source


@pytest.mark.parametrize("use_s3", [False, True], ids=["ydb", "s3"])
def test_image_export_reads_own_data_and_real_backup_without_disk_services(use_s3):
    with _SnapshotExportTestSetup(use_s3=use_s3, enable_backup=True) as setup:
        disk_size = 12 * 1024 * 1024
        disk_id = "image-export-source"
        base_id = "image-export-base"
        incremental_id = "image-export-incremental"
        disk_image_id = "image-export-from-disk"
        snapshot_image_id = "image-export-from-incremental"
        disk = setup.create_new_disk(disk_id, disk_size)
        mib_blocks = 1024 * 1024 // disk.block_size
        setup.write_random_blocks(disk_id, 0, mib_blocks)
        setup.write_random_blocks(disk_id, disk.blocks_count - mib_blocks, mib_blocks)
        base_checksum = setup.checksum_disk(disk_id)
        setup.create_snapshot(disk_id, base_id)
        setup.admin(
            "images",
            "create",
            "--id",
            disk_image_id,
            "--src-disk-id",
            disk_id,
            "--src-disk-zone-id=zone-a",
            "--folder-id=folder",
        )

        setup.write_random_blocks(disk_id, 2 * mib_blocks, mib_blocks)
        incremental_checksum = setup.checksum_disk(disk_id)
        assert incremental_checksum != base_checksum
        setup.create_snapshot(disk_id, incremental_id)
        setup.admin(
            "images",
            "create",
            "--id",
            snapshot_image_id,
            "--src-snapshot-id",
            incremental_id,
            "--folder-id=folder",
        )
        images = [(disk_image_id, base_checksum), (snapshot_image_id, incremental_checksum)]
        for image_id, checksum in images:
            # Default source addresses the image's own dataplane snapshot.
            prefix = f"image-direct/{image_id}"
            result = setup.export_snapshot_to_s3(image_id, prefix, resource_group="images")
            assert result.stdout == b""
            manifests = [key for key in setup.destination_keys(prefix) if key.endswith("manifest.json")]
            assert len(manifests) == 1
            manifest = setup.verify_s3_backup(manifests[0], checksum, disk_size)
            assert manifest["snapshot_id"] == image_id
            setup.backup_resource("images", image_id)

        original_source = setup.backup_fingerprint()
        assert f"source/images/{snapshot_image_id}/meta.json" in original_source
        assert not any(key.startswith("source/snapshots/") for key in original_source)
        setup.stop_disk_services()
        source_args = ("--source=backup",)
        for image_id, checksum in images:
            prefix = f"image-backup/{image_id}"
            for _ in range(2):
                existing = set(setup.destination_keys(prefix))
                result = setup.export_snapshot_to_s3(
                    image_id,
                    prefix,
                    server_config=setup.backup_export_config,
                    source_args=source_args,
                    resource_group="images",
                )
                assert result.stdout == b""
                new_keys = set(setup.destination_keys(prefix)) - existing
                manifests = [key for key in new_keys if key.endswith("manifest.json")]
                assert len(new_keys) == 4 and len(manifests) == 1
                assert manifests[0].encode() in result.stderr
                manifest = setup.verify_s3_backup(manifests[0], checksum, disk_size)
                assert manifest["snapshot_id"] == image_id
                assert manifest["encryption_mode"] == "NO_ENCRYPTION"
            assert len(setup.destination_keys(prefix)) == 8

        result = setup.export_snapshot_to_s3(
            "missing-image",
            "image-missing",
            check=False,
            server_config=setup.backup_export_config,
            source_args=source_args,
            resource_group="images",
        )
        assert result.returncode != 0
        assert setup.destination_keys("image-missing/") == []

        with fail_second_part_proxy(setup.destination_endpoint) as endpoint:
            result = setup.export_snapshot_to_s3(
                snapshot_image_id,
                "image-failed",
                endpoint=endpoint,
                check=False,
                server_config=setup.backup_export_config,
                source_args=source_args,
                resource_group="images",
            )
        assert result.returncode != 0
        failed_keys = setup.destination_keys("image-failed/")
        assert len(failed_keys) == 1 and failed_keys[0].endswith("part-00000001.raw")
        assert not any(key.endswith("manifest.json") for key in failed_keys)

        started, release = threading.Event(), threading.Event()
        with fail_second_part_proxy(setup.destination_endpoint, started, release) as endpoint:
            process = subprocess.Popen(
                setup.s3_export_command(
                    snapshot_image_id,
                    "image-cancelled",
                    endpoint,
                    setup.backup_export_config,
                    source_args,
                    resource_group="images",
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                assert started.wait(timeout=30), "Image export did not reach the second part"
                process.send_signal(signal.SIGTERM)
                stdout, _ = process.communicate(timeout=30)
                assert process.returncode != 0 and stdout == b""
            finally:
                release.set()
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=10)
        cancelled_keys = setup.destination_keys("image-cancelled/")
        assert len(cancelled_keys) == 1
        assert cancelled_keys[0].endswith("part-00000001.raw")
        assert not any(key.endswith("manifest.json") for key in cancelled_keys)
        assert setup.backup_fingerprint() == original_source
