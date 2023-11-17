import argparse
import json
import logging
import sys

from pprint import pprint, pformat

from . ext4 import get_unused_blocks_ext4
from . ntfs import get_unused_blocks_ntfs
from . xfs import get_unused_blocks_xfs


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


def get_unused_blocks_from_fs(part, bs):
    fs = part.get("fs")

    if fs is None:
        return None

    fns = {
        "ext4": get_unused_blocks_ext4,
        "xfs": get_unused_blocks_xfs,
        "ntfs": get_unused_blocks_ntfs,
    }

    fn = fns.get(fs["name"])

    if fn is None:
        return None

    return fn(fs, bs)


def generate_unused_blocks(meta, bs):
    pt = meta.get("partitions")

    if pt is None:
        return get_unused_blocks_from_fs(meta, bs)

    assert pt['label'] in ['gpt', 'dos']
    assert pt['unit'] == 'sectors'  # TODO

    unused_sectors = pt.get("unpartitioned", [])

    sector_size = meta["sector_size"]

    assert sector_size == bs

    unused_blocks = unused_sectors

    # if sector_size == bs:
    #     unused_blocks = unused_sectors
    # else:
    #     logging.info(f"convert unpartitioned space from {sector_size} to {bs} block size")
    #     unused_blocks = [
    #         (
    #             x * sector_size // bs,
    #             y * sector_size // bs
    #         ) for x, y in unused_sectors]

    logging.debug(f"unpartitioned space (in {bs} blocks) ({len(unused_blocks)}):")
    logging.debug(pformat(unused_blocks))

    for part in pt["partitions"]:
        offset_bytes = part['start'] * sector_size

        assert offset_bytes % bs == 0

        offset = (offset_bytes + bs - 1) // bs

        blocks = get_unused_blocks_from_fs(part, bs)

        logging.debug(f"partition {part['node']} unused blocks ({len(blocks) if blocks else 0}):")

        if blocks is not None:
            blocks = [(offset + s, offset + e) for s, e in blocks]
            unused_blocks += blocks

        logging.debug(pformat(blocks))

    return unused_blocks


def generate_used_blocks(path):
    meta = {}

    with open(path) as f:
        meta = json.load(f)

    bs = meta["block_size"]
    sz = meta["size"]

    default_block_size = 512

    assert bs != 0 and sz != 0
    assert bs >= 4096
    assert sz % default_block_size == 0

    unused_blocks = generate_unused_blocks(meta, default_block_size)
    if not unused_blocks:
        return None

    unused_blocks.sort(key=lambda x: x[0])

    used_blocks = []

    start = 0
    end = sz // default_block_size

    for b, e in unused_blocks:
        if start < b:
            used_blocks.append((start, b))
        start = e

    if start < end:
        used_blocks.append((start, end))

    if bs != default_block_size:
        return [(
                (x * default_block_size) // bs,           # round down
                (y * default_block_size + bs - 1) // bs,  # round up
                ) for x, y in used_blocks]

    return used_blocks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--meta-file", type=str, required=True)
    parser.add_argument("--format", choices=["python", "json"])

    parser.add_argument("-s", '--silent', help="silent mode", default=0, action='count')
    parser.add_argument("-v", '--verbose', help="verbose mode", default=0, action='count')

    args = parser.parse_args()

    prepare_logging(args)

    used_blocks = generate_used_blocks(args.meta_file)

    if args.format == "json":
        json.dump(used_blocks, sys.stdout, indent=4)
    else:
        pprint(used_blocks)

    return 0


if __name__ == '__main__':
    sys.exit(main())
