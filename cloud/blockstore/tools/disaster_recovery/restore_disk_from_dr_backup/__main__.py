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
from google.protobuf.json_format import MessageToDict, MessageToJson, ParseDict


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


def resolve_disk_devices(old_disk, old_by_uuid, current_by_physical, device_to_disk):
    if len(old_disk.DeviceUUIDs) == 0:
        raise RuntimeError(f"disk {quote(old_disk.DiskId)} has no devices in old backup")

    devices = []
    for uuid in old_disk.DeviceUUIDs:
        old_device = old_by_uuid.get(uuid)
        if old_device is None:
            raise RuntimeError(f"device {quote(uuid)} not found in old backup")

        current_device = current_by_physical.get(
            (old_device.SerialNumber, old_device.PhysicalOffset)
        )
        if current_device is None:
            raise RuntimeError(
                f"device {quote(uuid)} with serial number {quote(old_device.SerialNumber)}"
                f" and physical offset {old_device.PhysicalOffset} not found in current backup"
            )

        if not validate_device(old_device, current_device):
            raise RuntimeError(
                f"device {quote(uuid)} is inconsistent with device in previous backup"
            )

        current_uuid = current_device.DeviceUUID
        if current_uuid in device_to_disk and device_to_disk[current_uuid] != old_disk.DiskId:
            raise RuntimeError(f"device {quote(current_uuid)}"
                f"already exists on other disk {quote(device_to_disk[current_uuid])}")

        device_to_disk[current_uuid] = old_disk.DiskId
        devices.append(current_device)

    return devices


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


def build_nonreplicated_disk_request(old_disk, new_devices):
    device_uuids = []
    for device in new_devices:
        if not device.DeviceUUID:
            raise RuntimeError("device from current backup has no UUID")
        device_uuids.append(device.DeviceUUID)

    return disk_pb2.TCreateVolumeFromDevicesRequest(
        DiskId=old_disk.DiskId,
        BlockSize=old_disk.BlockSize,
        DeviceUUIDs=device_uuids,
        Force=True,
        KeepBlockCount=True,
    )


def resolve_fresh_device_uuids(master, old_by_uuid, current_by_physical):
    fresh_device_uuids = []
    for uuid in master.DeviceReplacementUUIDs:
        old_device = old_by_uuid.get(uuid)
        if old_device is None:
            raise RuntimeError(
                f"fresh device {quote(uuid)} not found in old backup"
            )

        current_device = current_by_physical.get(
            (old_device.SerialNumber, old_device.PhysicalOffset)
        )
        if current_device is None:
            raise RuntimeError(
                f"fresh device {quote(uuid)} with serial number "
                f"{quote(old_device.SerialNumber)} and physical offset "
                f"{old_device.PhysicalOffset} not found in current backup"
            )

        fresh_device_uuids.append(current_device.DeviceUUID)

    return fresh_device_uuids


def build_mirrored_disk_request(old_master, replicas, fresh_device_uuids):
    volume_config = {}
    if old_master.BlockSize:
        volume_config["blockSize"] = old_master.BlockSize
    if old_master.DiskId:
        volume_config["diskId"] = old_master.DiskId
    if old_master.StorageMediaKind:
        volume_config["storageMediaKind"] = old_master.StorageMediaKind

    return {
        "volumeConfig": volume_config,
        "replicas": [
            {"devices": [MessageToDict(device) for device in replica_devices]}
            for replica_devices in replicas
        ],
        "force": True,
        "keepBlockCount": True,
        "freshDeviceUUIDs": fresh_device_uuids,
    }


def request_payload(request, disk_id):
    try:
        if isinstance(request, dict):
            return json.dumps(request, ensure_ascii=False, separators=(",", ":"))
        return MessageToJson(request)
    except Exception as exc:
        raise RuntimeError(
            f"Error serializing request for disk {quote(disk_id)} error: {exc}"
        ) from exc


def suspend_secure_erase():
    send_request(
        "DiskRegistrySuspendSecureErase",
        {"state": True},
        "disk registry",
    )


def send_request(action, request, disk_id):
    args = [
        "blockstore-client", "executeaction",
        "--action", action,
        "--verbose", "error",
        "--input-bytes", request_payload(request, disk_id),
    ]
    subprocess.run(args, check=True, text=True, stdout=sys.stdout, stderr=sys.stdout)


def process_disk(
    disk_id,
    old_disk,
    disk_id_to_old_config,
    old_by_uuid,
    current_by_physical,
    device_to_disk,
    seen_master_disks
):
    if old_disk is None:
        raise RuntimeError(f" disk with id {quote(disk_id)} not found in old backup")

    if not is_mirrored_disk(old_disk):
        devices = resolve_disk_devices(
            old_disk, old_by_uuid, current_by_physical, device_to_disk
        )
        return "CreateDiskFromDevices", build_nonreplicated_disk_request(old_disk, devices)

    master = resolve_master_disk(old_disk, disk_id_to_old_config)
    if master.DiskId in seen_master_disks:
        return None

    fresh_device_uuids = resolve_fresh_device_uuids(
        master, old_by_uuid, current_by_physical
    )

    replicas = []
    for replica_index in range(master.ReplicaCount + 1):
        replica_id = build_replica_id(master.DiskId, replica_index)

        replica = disk_id_to_old_config.get(replica_id)
        if replica is None:
            raise RuntimeError(f"replica {quote(replica_id)} not found in old backup")
        replicas.append(
            resolve_disk_devices(
                replica, old_by_uuid, current_by_physical, device_to_disk
            )
        )

    seen_master_disks.add(master.DiskId)
    return (
        "CreateMirroredDiskFromDevices",
        build_mirrored_disk_request(master, replicas, fresh_device_uuids),
    )


def run(old_backup_path, current_backup_path, disk_ids, dry_run):
    old_backup = load_backup(old_backup_path)
    old_by_uuid, _ = build_device_index(old_backup)

    current_backup = load_backup(current_backup_path)
    _, current_by_physical = build_device_index(current_backup)

    disk_id_to_old_config = build_disk_id_to_config(old_backup)
    device_to_disk = {}
    disks_to_restore = []
    incomplete_disks = []
    seen_master_disks = set()

    for disk_id in disk_ids:
        try:
            result = process_disk(
                disk_id,
                disk_id_to_old_config.get(disk_id),
                disk_id_to_old_config,
                old_by_uuid,
                current_by_physical,
                device_to_disk,
                seen_master_disks
            )
        except RuntimeError as exc:
            incomplete_disks.append(disk_id)
            print(f"Disk {quote(disk_id)}: incomplete data to restore: {exc}")
            continue

        if result is None:
            print(f"Disk {quote(disk_id)}: skip, its master disk is already queued")
            continue

        action, request = result
        disks_to_restore.append((disk_id, action, request))
        print(f"Disk {quote(disk_id)}: ready to restore")

    print(f"Disks to restore count: {len(disks_to_restore)}")
    print(f"Disks with incomplete data count: {len(incomplete_disks)}")

    restore_failed = []
    if not dry_run:
        try:
            suspend_secure_erase()
        except Exception as exc:
            print(f"Failed to suspend secure erase: {exc}", file=sys.stderr)
            return 1
        print("Secure erase suspended")

        for disk_id, action, request in disks_to_restore:
            try:
                send_request(action, request, disk_id)
            except Exception as exc:
                restore_failed.append(disk_id)
                print(f"Disk {quote(disk_id)}: restore failed: {exc}", file=sys.stderr)
                continue
            print(f"Disk {quote(disk_id)} restored")

        print(f"Disks restored count: {len(disks_to_restore) - len(restore_failed)}")
        print(f"Disks failed to restore count: {len(restore_failed)}")

    if incomplete_disks or restore_failed:
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
        return run(
            args.old_backup_path,
            args.current_backup_path,
            disk_ids,
            args.dry_run,
        )
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
