import argparse
import socket
import subprocess
import sys

from cloud.blockstore.public.sdk.python.client import CreateClient
from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD_LOCAL
from subprocess import call


def _create_request(vol):
    table_lines = []
    logical_offset = 0
    for device in vol.Devices:
        size = device.BlockCount * vol.BlockSize // 512
        physical_offset = device.PhysicalOffset // 512
        table_lines.append(f'{logical_offset} {size} linear {device.DeviceName} {physical_offset}')
        logical_offset += size

    return '\n'.join(table_lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="localhost:9766")
    parser.add_argument("--disk-id", type=str, required=True)
    parser.add_argument("--device-name", type=str, required=False)
    parser.add_argument('--dry-run', action='store_true', default=False)

    args = parser.parse_args()
    if args.device_name is None:
        args.device_name = args.disk_id

    client = CreateClient(args.endpoint)
    vol = client.stat_volume(args.disk_id)["Volume"]

    hostname = socket.getfqdn()

    assert vol.StorageMediaKind == STORAGE_MEDIA_SSD_LOCAL
    for device in vol.Devices:
        assert device.AgentId == hostname

    request = _create_request(vol)
    if args.dry_run:
        print(request)
        return

    subprocess.run(
        ['sudo', 'dmsetup', 'create', args.device_name],
        check=True,
        input=request,
        text=True)


if __name__ == '__main__':
    main()
