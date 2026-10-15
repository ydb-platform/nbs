#!/usr/bin/env python3

import argparse
import json
import sys

from subprocess import run

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

    def add_device(device):
        by_uuid[device.DeviceUUID] = device
        by_physical[(device.SerialNumber, device.PhysicalOffset)] = device

    for agent in backup.Agents:
        for device in agent.Devices:
            add_device(device)
        for device in agent.UnknownDevices:
            add_device(device)

    return by_uuid, by_physical


def build_disk_id_to_config(backup):
    disk_id_to_config = {}
    for disk in backup.Disks:
        disk_id_to_config.setdefault(disk.DiskId, disk)
    return disk_id_to_config


def validate_device(old_device, new_device):
    return (
        old_device.SerialNumber == new_device.SerialNumber
        and old_device.PhysicalOffset == new_device.PhysicalOffset
        and old_device.PoolKind == new_device.PoolKind
        and old_device.BlockSize == new_device.BlockSize
    )


def resolve_disk_devices(old_disk, old_by_uuid, current_by_physical, validator):
    if len(old_disk.DeviceUUIDs) == 0:
        raise RuntimeError("disk has no devices in old backup")

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

        if uuid in validator:
            raise RuntimeError(f"device {quote(uuid)} already exists on other disk ")
        validator[uuid] = old_disk.DiskId
        devices.append(current_device)

    return devices


def is_mirrored_disk(disk):
    return disk.StorageMediaKind in (
        STORAGE_MEDIA_SSD_MIRROR2,
        STORAGE_MEDIA_SSD_MIRROR3,
    ) or bool(disk.MasterDiskId)


def build_replica_id(disk_id, replica_index):
    return f"{disk_id}/{replica_index}"


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


def build_mirrored_disk_request(old_master, replicas):
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


def send_request(action, request, disk_id):
    args = [
        "blockstore-client", "executeaction",
        "--action", action,
        "--verbose", "error",
        "--input-bytes", request_payload(request, disk_id),
    ]
    run(args, check=True, text=True, stdout=sys.stdout, stderr=sys.stdout)
    print(f"Disk {quote(disk_id)} restored")


def run(old_backup_path, current_backup_path, disk_ids):
    old_backup = load_backup(old_backup_path)
    old_by_uuid, _ = build_device_index(old_backup)

    current_backup = load_backup(current_backup_path)
    _, current_by_physical = build_device_index(current_backup)

    disk_id_to_old_config = build_disk_id_to_config(old_backup)
    validator = {}

    for disk_id in disk_ids:
        old_disk = disk_id_to_old_config.get(disk_id)
        if old_disk is None:
            raise RuntimeError(f" disk with id {quote(disk_id)} not found in old backup")

        if not is_mirrored_disk(old_disk):
            devices = resolve_disk_devices(
                old_disk, old_by_uuid, current_by_physical, validator
            )
            send_request(
                "CreateDiskFromDevices",
                build_nonreplicated_disk_request(old_disk, devices),
                disk_id,
            )
            continue

        if not old_disk.MasterDiskId:
            continue

        replicas = []
        for replica_index in range(old_disk.ReplicaCount + 1):
            replica_id = build_replica_id(disk_id, replica_index)
            replica = disk_id_to_old_config.get(replica_id)
            if replica is None:
                raise RuntimeError(f"replica {quote(replica_id)} not found in old backup")
            replicas.append(
                resolve_disk_devices(
                    replica, old_by_uuid, current_by_physical, validator
                )
            )

        send_request(
            "CreateMirroredDiskFromDevices",
            build_mirrored_disk_request(old_disk, replicas),
            disk_id,
        )


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
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    disk_ids = [disk_id for disk_id in args.disk_ids.split(",") if disk_id]
    try:
        run(
            args.old_backup_path,
            args.current_backup_path,
            disk_ids,
        )
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
