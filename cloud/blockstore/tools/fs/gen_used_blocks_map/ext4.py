import dataclasses
import logging
import re

from dataclasses import dataclass
from pprint import pformat


@dataclass
class Ext4SB:

    block_count: int = 0
    block_size: int = 0
    inode_size: int = 0
    blocks_per_group: int = 0
    inodes_per_group: int = 0
    reserved_gdt_block_count: int = 0
    features: set[str] = dataclasses.field(default_factory=set)


@dataclass
class Ext4Group:

    num: int
    blocks: tuple[int]

    free_blocks: list[tuple[int]] = dataclasses.field(default_factory=list)
    free_inodes: list[tuple[int]] = dataclasses.field(default_factory=list)

    block_bitmap: int = None
    block_bitmap_csum: int = None
    inode_bitmap: int = None
    inode_bitmap_csum: int = None
    inode_table: tuple[int] = None
    free_block_count: int = None
    free_inode_count: int = None
    unused_inodes: int = None

    primary_sb: int = None
    bkp_sb: int = None
    reserved_gdt: tuple[int] = None
    group_descriptors: tuple[int] = None
    flags: set[str] = dataclasses.field(default_factory=set)


def read_ext4_sb(stream):
    sb = Ext4SB()

    newline = 0

    for line in stream:
        if line == '\n':
            newline += 1
            if newline == 2:
                break

        fields = line.split(':')
        if len(fields) != 2:
            continue

        name, val = [x.strip() for x in fields]
        if name == 'Block count':
            sb.block_count = int(val)
            continue

        if name == 'Block size':
            sb.block_size = int(val)
            continue

        if name == 'Blocks per group':
            sb.blocks_per_group = int(val)
            continue

        if name == 'Inodes per group':
            sb.inodes_per_group = int(val)
            continue

        if name == 'Inode size':
            sb.inode_size = int(val)
            continue

        if name == 'Reserved GDT blocks':
            sb.reserved_gdt_block_count = int(val)
            continue

        if name == 'Filesystem features':
            sb.features = set(val.split())
            continue

    return sb


def parse_ranges(line):
    result = []
    for r in line.split(', '):
        rr = r.split('-')

        assert 0 < len(rr) <= 2

        if len(rr) == 1:
            x = int(rr[0])
            result.append((x, x))
        else:
            x = int(rr[0])
            y = int(rr[1])
            result.append((x, y))

    return result


def parse_free_blocks(line):
    if not line.startswith('Free blocks:'):
        return None

    line = line[len('Free blocks:'):].strip()

    if len(line) == 0:
        return []

    return [(x, y + 1) for x, y in parse_ranges(line)]


def parse_free_inodes(line):
    if not line.startswith('Free inodes:'):
        return None

    line = line[len('Free inodes:'):].strip()

    if len(line) == 0:
        return []

    return parse_ranges(line)


def parse_group_flags(line):
    flags = set()

    for f in ['INODE_UNINIT', 'BLOCK_UNINIT']:
        if line.find(f) != -1:
            flags.add(f)

    return flags


def read_ext4_groups(stream):
    group_re = re.compile(r'Group (\d+): \(Blocks (\d+)\-(\d+)\)')
    primary_sb_re = re.compile(r'Primary superblock at (\d+), Group descriptors at (\d+)\-(\d+)')
    bkp_sb_re = re.compile(r'Backup superblock at (\d+), Group descriptors at (\d+)\-(\d+)')
    reserved_gdt_re = re.compile(r'Reserved GDT blocks at (\d+)\-(\d+)')
    block_bitmap_re = re.compile(r'Block bitmap at (\d+) \([^)]+\)(, csum 0x([a-z0-9]+))?')
    inode_bitmap_re = re.compile(r'Inode bitmap at (\d+) \([^)]+\)(, csum 0x([0-9a-z]+))?')
    inode_table_re = re.compile(r'Inode table at (\d+)\-(\d+)')
    summary_re = re.compile(r'(\d+) free blocks, (\d+) free inodes, \d+ directories(, (\d+) unused inodes)?')

    group = None
    for line in stream:
        if line == '\n':
            break

        line = line.strip()

        m = group_re.search(line)
        if m is not None:
            if group is not None:
                yield group

            b = int(m.group(2))
            e = int(m.group(3)) + 1

            group = Ext4Group(
                num=int(m.group(1)),
                blocks=(b, e),
                flags=parse_group_flags(line)
            )

            continue

        m = primary_sb_re.search(line)
        if m is not None:
            group.primary_sb = int(m.group(1))
            b = int(m.group(2))
            e = int(m.group(3)) + 1
            group.group_descriptors = (b, e)
            continue

        m = bkp_sb_re.search(line)
        if m is not None:
            group.bkp_sb = int(m.group(1))
            b = int(m.group(2))
            e = int(m.group(3)) + 1
            group.group_descriptors = (b, e)
            continue

        m = reserved_gdt_re.search(line)
        if m is not None:
            b = int(m.group(1))
            e = int(m.group(2)) + 1
            group.reserved_gdt = (b, e)
            continue

        m = block_bitmap_re.search(line)
        if m is not None:
            group.block_bitmap = int(m.group(1))
            if m.group(2) is not None:
                group.block_bitmap_csum = int(m.group(3), 16)
            continue

        m = inode_bitmap_re.search(line)
        if m is not None:
            group.inode_bitmap = int(m.group(1))
            if m.group(2) is not None:
                group.inode_bitmap_csum = int(m.group(3), 16)
            continue

        m = inode_table_re.search(line)
        if m is not None:
            b = int(m.group(1))
            e = int(m.group(2)) + 1
            group.inode_table = (b, e)
            continue

        m = summary_re.search(line)
        if m is not None:
            group.free_block_count = int(m.group(1))
            group.free_inode_count = int(m.group(2))
            if m.group(3) is not None:
                group.unused_inodes = int(m.group(4))
            continue

        if line.startswith('Free blocks:'):
            group.free_blocks = parse_free_blocks(line)
            continue

        if line.startswith('Free inodes:'):
            group.free_inodes = parse_free_inodes(line)
            continue

    if group is not None:
        yield group


def range_size(r):
    if r is None:
        return 0

    return r[1] - r[0]


def get_unused_blocks_ext4(fs, bs):

    unused_blocks = []

    with open(fs["dumpe2fs"]) as stream:

        sb = read_ext4_sb(stream)
        logging.debug(sb)

        unused_blocks = []

        for group in read_ext4_groups(stream):
            logging.debug(pformat(group))

            unused_blocks += group.free_blocks

            blocks_in_group = range_size(group.blocks)
            if group.bkp_sb is not None or group.primary_sb is not None:
                blocks_in_group -= 1

            blocks_in_group -= range_size(group.group_descriptors)
            blocks_in_group -= range_size(group.reserved_gdt)

            logging.debug(f"actual blocks in Group #{group.num}: {blocks_in_group}")

            # By default, a filesystem is allowed to increase in size by a
            # factor of 1024x over the original filesystem size.
            if group.reserved_gdt is not None and group.num != 0:
                unused_blocks.append(group.reserved_gdt)

            if 'metadata_csum' in sb.features:
                if group.block_bitmap and group.block_bitmap_csum == 0:
                    unused_blocks.append((group.block_bitmap, group.block_bitmap + 1))

                if group.inode_bitmap and group.inode_bitmap_csum == 0:
                    unused_blocks.append((group.inode_bitmap, group.inode_bitmap + 1))
            else:
                if group.block_bitmap and \
                        (
                            group.free_block_count == sb.blocks_per_group or
                            'BLOCK_UNINIT' in group.flags
                        ):
                    unused_blocks.append((group.block_bitmap, group.block_bitmap + 1))

                if group.inode_bitmap and \
                        (
                            group.free_inode_count == sb.inodes_per_group or
                            'INODE_UNINIT' in group.flags
                        ):
                    unused_blocks.append((group.inode_bitmap, group.inode_bitmap + 1))

            # The table is sized to have enough blocks to store at least sb.s_inode_size * sb.s_inodes_per_group bytes.
            # The number of the block group containing an inode can be calculated as (inode_number - 1) / sb.s_inodes_per_group,
            # and the offset into the group's table is (inode_number - 1) % sb.s_inodes_per_group. There is no inode 0.

            inodes_per_block = sb.block_size / sb.inode_size
            for ib, ie in group.free_inodes:
                # offsets in inode table
                ob = (ib - 1) % sb.inodes_per_group
                oe = (ie - 1) % sb.inodes_per_group

                # block indices in inode table
                b = (ob + inodes_per_block - 1) // inodes_per_block
                e = (oe + 1) // inodes_per_block

                if b < e:
                    b += group.inode_table[0]
                    e += group.inode_table[0]

                    unused_blocks.append((int(b), int(e)))

        if sb.block_size != bs:
            logging.info(f"convert ranges from {sb.block_size} to {bs} block size")

            return [
                (
                    (b * sb.block_size + bs - 1) // bs,
                    (e * sb.block_size) // bs
                ) for b, e in unused_blocks]

    return unused_blocks
