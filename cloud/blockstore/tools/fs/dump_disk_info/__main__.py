import argparse
import io
import json
import logging
import os
import sys

from subprocess import PIPE, run


def prepare_logging(args):
    log_level = logging.ERROR

    if args.silent:
        log_level = logging.INFO
    elif args.verbose:
        log_level = max(0, logging.ERROR - 10 * int(args.verbose))

    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="[%(levelname)s] [%(asctime)s] %(message)s")


def invoke_blockdev(path, arg):
    p = run(['blockdev', '--' + arg, path], stdout=PIPE, check=True, text=True)
    return int(p.stdout)


def get_dev_size(path):
    return invoke_blockdev(path, 'getsize64')


def get_block_size(path):
    return invoke_blockdev(path, 'getbsz')


def get_physical_block_size(path):
    return invoke_blockdev(path, 'getpbsz')


def get_sector_size(path):
    return invoke_blockdev(path, 'getss')


def get_partition_table(path):
    p = run(
        ['sfdisk', '-J', path],
        stdout=PIPE,
        stderr=PIPE,
        text=True,
        check=False)

    if p.returncode == 0:
        return json.loads(p.stdout)["partitiontable"]

    if p.returncode != 0 and \
            (p.stderr.find("does not contain a recognized partition table") != -1 or
             p.stderr.find("sfdisk: failed to dump partition table: Success") != -1):
        return None

    logging.error(f"can't get partition list ({path}): {p.stderr} ({p.returncode})")

    raise Exception(p.stderr)


def get_unpartitioned_space(path):
    p = run(
        ['sfdisk', '-F', '--bytes', path],
        stdout=PIPE,
        text=True,
        check=True)

    s = io.StringIO(p.stdout)

    sector_size = get_sector_size(path)
    bs = get_physical_block_size(path)

    assert s.readline().startswith(f"Unpartitioned space {path}:")
    assert s.readline().strip() == f"Units: sectors of 1 * {sector_size} = {sector_size} bytes"
    assert s.readline().startswith(
        f"Sector size (logical/physical): {sector_size} bytes / {bs} bytes"
    )
    assert s.readline().strip() == ""  # EOF or delimiter

    header = s.readline().strip()
    if header == "":
        return []

    assert header.split() == ['Start', 'End', 'Sectors', 'Size']

    ranges = []

    for line in s:
        start, end, _, _ = [int(x) for x in line.split()]
        if start != end:
            ranges.append((start, end + 1))

    return ranges


def try_dump_ext4(part_num, path, args):
    if not os.path.exists('./dumpe2fs'):
        logging.warning("skip ext4: ./dumpe2fs not found")
        return None

    part_size = get_dev_size(path)
    if part_size <= 2 * 1024**2:  # 2 MiB
        logging.debug(f"try_dump_ext4: too small partition: {part_size} B")
        return None

    p = run(
        ['./dumpe2fs', path],
        stdout=PIPE,
        stderr=PIPE,
        check=False,
        text=True)

    logging.debug(f"dumpe2fs: {p.returncode}")

    # 147: Inode bitmap checksum does not match bitmap while trying to read '{path}' bitmaps
    # 156: Block bitmap checksum does not match bitmap
    if p.returncode not in [0, 147, 156]:
        if len(p.stderr) > 0 and p.stderr.find("Bad magic number in super-block while trying to open") != -1:
            return None
        raise Exception(p.stderr)

    dst = os.path.join(args.output_dir, f'p{part_num}.ext4.txt')

    with open(dst, 'w') as f:
        f.write(p.stdout)

    return {
        "name": "ext4",
        "dumpe2fs": dst
    }


def try_dump_xfs(part_num, path, args):
    if not os.path.exists('./xfs_db'):
        logging.warning("skip XFS: ./xfs_db not found")
        return None

    part_size = get_dev_size(path)
    if part_size <= 16 * 1024**2:  # 16 MiB
        logging.debug(f"try_dump_xfs: too small partition: {part_size} B")
        return None

    p = run(
        ['./xfs_db', "-c", "sb 0", "-c", "p", path],
        stdout=PIPE,
        stderr=PIPE,
        check=False,
        text=True)

    if p.returncode != 0:
        if len(p.stderr) > 0 and p.stderr.find("is not a valid XFS filesystem") != -1:
            return None
        raise Exception(p.stderr)

    sb = os.path.join(args.output_dir, f'p{part_num}.xfs.sb.txt')
    with open(sb, 'w') as f:
        f.write(p.stdout)

    p = run(
        ['./xfs_db', '-c', 'freesp -d -h1', path],
        stdout=PIPE,
        check=True,
        text=True)

    freesp = os.path.join(args.output_dir, f'p{part_num}.xfs.freesp.txt')
    with open(freesp, 'w') as f:
        f.write(p.stdout)

    return {
        "name": "xfs",
        "sb": sb,
        "freesp": freesp
    }


def try_dump_ntfs(part_num, path, args):
    if not os.path.exists('./ntfswipe'):
        logging.warning("skip NTFS: ./ntfswipe not found")
        return None

    p = run(
        ['./ntfswipe', "--unused", "--verbose", "--no-action", path],
        stdout=PIPE,
        stderr=PIPE,
        check=False,
        text=True)

    if p.returncode != 0:
        if len(p.stderr) > 0 and p.stderr.find("NTFS signature is missing.") != -1:
            return None
        raise Exception(p.stderr)

    blocks = os.path.join(args.output_dir, f'p{part_num}.ntfs.blocks.txt')
    with open(blocks, 'w') as f:
        f.write(p.stdout)

    return {
        "name": "ntfs",
        "ntfswipe": blocks
    }


def dump_fs_info(part_num, path, args):
    fs = [try_dump_ntfs, try_dump_ext4, try_dump_xfs]

    for f in fs:
        fs = f(part_num, path, args)
        if fs is not None:
            return fs

    return None


def dump_disk_info(args):
    meta = {
        "block_size": args.block_size,
        "size": get_dev_size(args.path),
        "sector_size": get_sector_size(args.path),
    }

    pt = get_partition_table(args.path)

    os.makedirs(args.output_dir, exist_ok=True)

    if pt is not None:
        pt["unpartitioned"] = get_unpartitioned_space(args.path)

        for i, part in enumerate(pt["partitions"]):
            part["fs"] = dump_fs_info(i + 1, part['node'], args)

        meta["partitions"] = pt
    else:
        meta["fs"] = dump_fs_info(0, args.path, args)

    with open(os.path.join(args.output_dir, 'meta.json'), 'w') as f:
        json.dump(meta, f, indent=4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--block-size", type=int, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    parser.add_argument("--path", type=str, default='/dev/nbd0')

    parser.add_argument("-s", '--silent', help="silent mode", default=0, action='count')
    parser.add_argument("-v", '--verbose', help="verbose mode", default=0, action='count')

    args = parser.parse_args()

    prepare_logging(args)

    dump_disk_info(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())
