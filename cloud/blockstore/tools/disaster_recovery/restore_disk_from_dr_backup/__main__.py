#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys

from cloud.blockstore.libs.storage.protos import disk_pb2
from cloud.storage.core.protos.media_pb2 import (
    STORAGE_MEDIA_SSD_MIRROR2,
    STORAGE_MEDIA_SSD_MIRROR3,
)
from google.protobuf.json_format import MessageToJson, ParseDict


def quote(value):
    return json.dumps(str(value), ensure_ascii=False)


def load_backup(path):
    try:
        with open(path, "r", encoding="utf-8") as backup_file:
            payload = json.load(backup_file)
        response = ParseDict(payload, disk_pb2.TBackupDiskRegistryStateResponse())
    except Exception as exc:
        raise RuntimeError(
            f"Error loading backup from: {quote(path)} error: {exc}"
        ) from exc
    return response.LocalDBBackup


def build_device_index(backup):
    by_uuid = {}
    by_physical = {}

    for agent in backup.Agents:
        for device in agent.Devices:
            by_uuid[device.DeviceUUID] = device
            by_physical[(device.SerialNumber, device.PhysicalOffset)] = device

    return by_uuid, by_physical


def build_disk_id_to_config(backup):
    disk_id_to_config = {}
    for disk in backup.Disks:
        disk_id_to_config[disk.DiskId] = disk
    return disk_id_to_config


def validate_device(old_device, new_device):
    return (
        old_device.SerialNumber == new_device.SerialNumber
        and old_device.PhysicalOffset == new_device.PhysicalOffset
        and old_device.PoolKind == new_device.PoolKind
        and old_device.BlockSize == new_device.BlockSize
    )


def is_mirrored_disk(disk):
    return disk.StorageMediaKind in (
        STORAGE_MEDIA_SSD_MIRROR2,
        STORAGE_MEDIA_SSD_MIRROR3,
    ) or bool(disk.MasterDiskId)


def build_replica_id(disk_id, replica_index):
    return f"{disk_id}/{replica_index}"


def resolve_master_disk(disk, disk_id_to_old_config):
    if not disk.MasterDiskId:
        return disk

    master = disk_id_to_old_config.get(disk.MasterDiskId)
    if master is None:
        raise RuntimeError(
            f"master disk {quote(disk.MasterDiskId)} not found in old backup"
        )
    return master


def suspend_secure_erase():
    send_request(
        "DiskRegistrySuspendSecureErase",
        {"state": True},
        "disk registry",
    )


def send_request(request, disk_id):
    args = [
        "blockstore-client", "executeaction",
        "--action", "CreateDiskFromDevices",
        "--verbose", "error",
        "--input-bytes", MessageToJson(request),
    ]
    subprocess.run(args, check=True, text=True, stdout=sys.stdout, stderr=sys.stdout)


def validate_replicas(replicas):
    if len(replicas) == 0:
        return False

    devices_len = len(replicas[0])
    for replica in replicas:
        if len(replica) != devices_len:
            return False

    for d in range(devices_len):
        has_data = False

        for r in range(len(replicas)):
            if replicas[r][d] != "":
                has_data = True
                break
        if not has_data:
            return False

    return True


def get_available_devices(devices):
    available_devices = []
    for uuid in devices:
        if uuid != "":
            available_devices.append(uuid)
    return available_devices


def build_request(disk, devices=None, replicas=None):
    request = disk_pb2.TCreateVolumeFromDevicesRequest(
        DiskId=disk.DiskId,
        BlockSize=disk.BlockSize,
        StorageMediaKind=disk.StorageMediaKind,
        Force=True,
        KeepBlockCount=True,
        # DoNotCreateIfTabletExists=True,
    )

    if disk.ProjectId:
        request.ProjectId = disk.ProjectId
    if disk.FolderId:
        request.FolderId = disk.FolderId
    if disk.CloudId:
        request.CloudId = disk.CloudId

    if (replicas is None) == (devices is None):
        raise RuntimeError(
            f"Disk {quote(disk.DiskId)}: "
            "either devices or replicas must be set, but not both"
        )
    if replicas is not None:
        for replica_uuids in replicas:
            replica = request.Replicas.add()
            replica.DeviceUUIDs.extend(replica_uuids)
    else:
        request.DeviceUUIDs.extend(devices)

    return request


class IncompleteDisk:
    def __init__(self, disk_id, available_devices: list[str], error):
        self.disk_id = disk_id
        self.available_devices = available_devices

    def __str__(self):
        return f"Incomplete disk {quote(self.disk_id)}\n"
        "available devices uuids:\n{self.available_devices}"


class RestoreDiskFromDrBackup:
    def __init__(self, old_backup_path, current_backup_path, disk_ids, dry_run):
        self._old_backup_path = old_backup_path
        self._current_backup_path = current_backup_path
        self._disk_ids = disk_ids
        self._dry_run = dry_run

        old_backup = load_backup(self._old_backup_path)
        self.old_by_uuid, _ = build_device_index(old_backup)

        current_backup = load_backup(self._current_backup_path)
        _, self.current_by_physical = build_device_index(current_backup)

        self.disk_id_to_old_config = build_disk_id_to_config(old_backup)

        self.device_to_disk = {}
        self.requests = []
        self.incomplete_disks = []
        self.processed_master_disks = set()

    def resolve_disk_devices(self, old_disk):
        devices = []

        def handle_missing_device(msg):
            devices.append("")
            print(msg)

        for uuid in old_disk.DeviceUUIDs:

            if uuid in old_disk.DeviceReplacementUUIDs:
                devices.append("")
                continue

            old_device = self.old_by_uuid.get(uuid)
            if current_device is None:
                handle_missing_device(f"Device {quote(uuid)} not found in old backup")
                continue

            current_device = self.current_by_physical.get((old_device.SerialNumber, old_device.PhysicalOffset))
            if current_device is None:
                handle_missing_device(f"Device {quote(uuid)} with serial number {quote(old_device.SerialNumber)}"
                                        f" and offset {old_device.PhysicalOffset} not found in current backup")
                continue

            if not validate_device(old_device, current_device):
                handle_missing_device(f"Device {quote(uuid)} is inconsistent with device in previous backup")
                continue

            current_uuid = current_device.DeviceUUID
            if current_uuid in self.device_to_disk and self.device_to_disk[current_uuid] != old_disk.DiskId:
                handle_missing_device(f"device {quote(current_uuid)} already exists on"
                                        f"disk {quote(self.device_to_disk[current_uuid])}")
                continue

            self.device_to_disk[current_uuid] = old_disk.DiskId
            devices.append(current_uuid)

        return devices

    def process_disk(self, disk_id):
        old_disk = self.disk_id_to_old_config.get(disk_id)
        if old_disk is None:
            raise RuntimeError(f"Disk {quote(disk_id)} is not found in old backup")

        if not is_mirrored_disk(old_disk):
            devices = self.resolve_disk_devices(old_disk, allow_fresh=False)
            available_devices = get_available_devices(devices)

            if len(available_devices) != len(devices):
                self.incomplete_disks.append(IncompleteDisk(disk_id, available_devices))
            else:
                self.requests.append(build_request(old_disk, devices))
            return

        master = resolve_master_disk(old_disk, self.disk_id_to_old_config)
        if master.DiskId in self.processed_master_disks:
            return
        self.processed_master_disks.add(master.DiskId)

        replicas = []
        for replica_index in range(master.ReplicaCount + 1):
            replica_id = build_replica_id(master.DiskId, replica_index)

            replica = self.disk_id_to_old_config.get(replica_id)
            if replica is None:
                RuntimeError(f"Disk {quote(replica_id)} not found in old backup")
                continue

            devices = self.resolve_disk_devices(replica)
            replicas.append(devices)

        if validate_replicas(replicas):
            self.requests.append(build_request(master, replicas=replicas))
        else:
            devices = []
            for replica in replicas:
                devices.extend(get_available_devices(replica))
            self.incomplete_disks.append(IncompleteDisk(master.DiskId, get_available_devices(devices)))

    def run(self):
        for disk_id in self._disk_ids:
            try:
                self.process_disk(disk_id)
            except RuntimeError as exc:
                print(f"Disk {quote(disk_id)} error: {exc}")
                continue

            print(f"Disk {quote(disk_id)}: ready to restore")

        print(f"Disks to restore count: {len(self.requests)}")
        print(f"Disks with unavailable devices count: {len(self.incomplete_disks)}")

        for incomplete_disk in self.incomplete_disks:
            print(incomplete_disk)

        restore_failed = []
        if not self._dry_run:
            try:
                suspend_secure_erase()
            except Exception as exc:
                print(f"Failed to suspend secure erase: {exc}", file=sys.stderr)
                return 1
            print("Secure erase suspended")

            for disk_id, request in self.requests:
                try:
                    send_request(request, disk_id)
                except Exception as exc:
                    restore_failed.append(disk_id)
                    print(
                        f"Disk {quote(disk_id)}: restore failed: {exc}",
                        file=sys.stderr,
                    )
                    continue
                print(f"Disk {quote(disk_id)} restored")

            print(f"Disks failed to restore count: {len(restore_failed)}")

        if self.incomplete_disks or restore_failed:
            return 1

        return 0


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--old-backup-path",
        required=True,
        help="Path to the old backup file",
    )
    parser.add_argument(
        "--current-backup-path",
        required=True,
        help="Path to the current backup file",
    )
    parser.add_argument(
        "--disk-ids",
        required=True,
        help="Comma-separated list of disk IDs to restore",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check disks data and do not call blockstore-client",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    disk_ids = [disk_id for disk_id in args.disk_ids.split(",") if disk_id]
    try:
        return RestoreDiskFromDrBackup(
            args.old_backup_path,
            args.current_backup_path,
            disk_ids,
            args.dry_run,
        ).run()
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
