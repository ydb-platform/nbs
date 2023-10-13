import subprocess

import yatest.common as common


BLOCK_SIZE = 4*1024
EXPECTED_BLOCKS_COUNT = 64*1024


def test_resize_disk():
    common.execute(["lsblk"])

    ex = common.execute(
        ["sudo", "blockdev", "--getsize64", "/dev/vdb"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)

    disk_size = int(ex.stdout)
    assert disk_size == EXPECTED_BLOCKS_COUNT * BLOCK_SIZE
