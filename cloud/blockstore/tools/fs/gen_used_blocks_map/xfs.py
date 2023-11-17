import logging

from dataclasses import dataclass
from pprint import pformat


@dataclass
class XfsSB:

    blocksize: int = 0
    sectsize: int = 0
    agblocks: int = 0
    agcount: int = 0


def read_xfs_sb(stream):

    sb = XfsSB()

    for line in stream:
        fields = line.split(' = ')

        if len(fields) != 2:
            continue

        name, val = fields

        if hasattr(sb, name):
            setattr(sb, name, int(val.strip()))

    return sb


def read_xfs_free_space(stream):
    assert stream.readline().split() == ['agno', 'agbno', 'len']

    freesp = []

    for line in [x.strip() for x in stream]:
        if line.startswith('from'):  # footer
            break

        fields = line.split()
        assert len(fields) == 3

        freesp.append(tuple(map(int, fields)))

    return freesp


def get_xfs_free_space(path):
    with open(path) as f:
        return read_xfs_free_space(f)


def get_xfs_sb(path):
    with open(path) as f:
        return read_xfs_sb(f)


def get_unused_blocks_xfs(fs, bs):

    sb = get_xfs_sb(fs["sb"])

    freesp = get_xfs_free_space(fs["freesp"])

    logging.debug("[XFS] freesp:")
    logging.debug(pformat(freesp))

    unused_blocks = []

    for group, offset, count in freesp:
        offset_in_blocks = group * sb.agblocks + offset
        unused_blocks.append((offset_in_blocks, offset_in_blocks + count))

    if sb.blocksize != bs:
        assert sb.blocksize > bs and sb.blocksize % bs == 0

        logging.info(f"convert ranges from {sb.blocksize} to {bs} block size")

        return [
            (
                (x[0] * sb.blocksize + bs - 1) // bs,
                (x[1] * sb.blocksize) // bs
            ) for x in unused_blocks]

    return unused_blocks
