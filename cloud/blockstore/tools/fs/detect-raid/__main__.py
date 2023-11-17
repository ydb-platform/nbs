#!/usr/bin/env python

import io
import struct
import sys

from datetime import datetime, timedelta, timezone
from subprocess import run, PIPE


def read_u32(s):
    data = s.read(4)  # little-endian

    return struct.unpack("<I", data)[0]


def read_u64(s):
    data = s.read(8)  # little-endian

    return struct.unpack("<Q", data)[0]


def read_cstr(s, max_size):
    buffer = s.read(max_size)
    s = ''
    for c in buffer:
        if c == 0:
            break
        s += chr(c)
    return s


def read_ctime(s):
    x = read_u64(s)

    # low 40-bits are seconds
    seconds = x & 0x000000FFFFFFFFFF

    # high 24-bits are microseconds
    microseconds = x >> 40

    dt = timedelta(
        seconds=seconds,
        microseconds=microseconds
    )

    return datetime(1970, 1, 1, tzinfo=timezone.utc) + dt


def read_magic(dev):
    return read_u32(dev)


def read_major_version(s):
    return read_u32(dev)


def read_superblock(dev, skip):
    dev.seek(skip)
    buffer = dev.read(256)

    assert len(buffer)

    return buffer


# https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


class RAID:

    def __init__(self, version, name, level, ctime, used_size, disk_count):
        self.version = version
        self.name = name
        self.level = level
        self.ctime = ctime
        self.used_size = used_size
        self.disk_count = disk_count


class Device:

    def __init__(
            self,
            data_offset,
            data_size,
            super_offset,
            recovery_offset,
            dev_number,
            cnt_corrected_read,
            device_uuid,
            devflags):

        self.data_offset = data_offset
        self.data_size = data_size
        self.super_offset = super_offset
        self.recovery_offset = recovery_offset
        self.dev_number = dev_number
        self.device_uuid = device_uuid
        self.devflags = devflags


def read_info(s):
    magic = read_magic(s)
    if magic != 0xa92b4efc:
        return (None, None)

    version = read_u32(s)

    _ = read_u32(s)  # skip feature_map
    _ = read_u32(s)  # skip pad0
    _ = read_cstr(s, 16)  # skip uuid

    name = read_cstr(s, 32)
    ctime = read_ctime(s)
    level = read_u32(s)

    _ = read_u32(s)  # skip layout of array

    used_size = read_u64(s) * 512

    _ = read_u32(s) * 512  # chunksize

    disk_count = read_u32(s)

    _ = read_u32(s)  # skip bitmap_offset

    raid = RAID(version, name, level, ctime, used_size, disk_count)

    # read device specific info

    _ = s.read(28)  # skip 'RAID-Reshape In-Process Metadata Storage/Recovery area'

    data_offset = read_u64(s) * 512
    data_size = read_u64(s) * 512
    super_offset = read_u64(s) * 512
    recovery_offset = read_u64(s) * 512

    dev_number = read_u32(s)
    cnt_corrected_read = read_u32(s)
    device_uuid = s.read(16)
    devflags = s.read(1)[0]

    _ = s.read(7)  # skip pad2

    device = Device(
        data_offset,
        data_size,
        super_offset,
        recovery_offset,
        dev_number,
        cnt_corrected_read,
        device_uuid,
        devflags
    )

    return (raid, device)


def getsize64(path):
    r = run(['blockdev', '--getsize64', path], stdout=PIPE, check=True)
    return int(r.stdout)


def main(path, dev):
    dev_size = getsize64(path)
    assert dev_size > 0

    v09_offset = (dev_size // 65536) * 65536 - 65536
    assert v09_offset > 0

    for offset in [4096, 0, v09_offset]:

        superblock = read_superblock(dev, offset)
        s = io.BytesIO(superblock)

        (raid, device) = read_info(s)
        if raid is None:
            continue

        print(f'Superblock found at offset {offset}:')
        print('  size:', raid.disk_count)
        print('  version:', raid.version)
        print('  level:', raid.level)
        print('  name:', raid.name)
        print('  ctime:', raid.ctime)
        print('  used_size: {} ({})'.format(raid.used_size, sizeof_fmt(raid.used_size)))

        print('Device:')
        print('  number:', device.dev_number)
        print('  data_size: {} ({})'.format(device.data_size, sizeof_fmt(device.data_size)))
        print('  data_offset: {} ({})'.format(device.data_offset, sizeof_fmt(device.data_offset)))
        print('  super_offset: {} ({})'.format(device.super_offset, sizeof_fmt(device.super_offset)))

        print('  flags:', bin(device.devflags))

        return 0

    print('RAID not detected')

    return 1


if __name__ == '__main__':
    p = sys.argv[1]
    print('Read from {}'.format(p))

    with open(p, 'rb') as dev:
        exit(main(p, dev))
