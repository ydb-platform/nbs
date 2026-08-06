import os
import subprocess
import pytest

import yatest.common as yatest_common


_FILE_SIZE = 1  # GB
_IO_DEPTH = 8
_BLOCKSIZE = 4096  # KB
_REQUEST_BLOCK_COUNT = 3
_BINARY_PATH = "cloud/blockstore/tools/testing/eternal_tests/eternal-load/bin/eternal-load"


def __run_load_test(
    file_name,
    scenario="aligned",
    engine="asyncio",
    direct=True,
    timeout=60,
    test_count=0,
):
    eternal_load = yatest_common.binary_path(_BINARY_PATH)

    params = [
        eternal_load,
        "--config-type", "generated",
        "--scenario", scenario,
        "--engine", engine,
        "--blocksize", str(_BLOCKSIZE),
        "--request-block-count", str(_REQUEST_BLOCK_COUNT),
        "--file", str(file_name),
        "--filesize", str(_FILE_SIZE),
        "--iodepth", str(_IO_DEPTH),
        "--write-rate", "70",
        "--debug",
        "--test-count", str(test_count),
    ]

    if not direct:
        params.append("--no-direct")

    result = subprocess.run(
        params,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=timeout,
    )

    print(result.stderr)
    return result


@pytest.fixture(name="very_small_ext4")
def mount_very_small_ext4(tmp_path):

    loopback_path = tmp_path / "fs.img"
    mount_dir = tmp_path / "testfs"

    mount_dir.mkdir()

    subprocess.run(
        ["dd", "if=/dev/zero", f"of={loopback_path}", "bs=1M", "count=64"],
        check=True,
    )

    subprocess.run(
        ["mkfs.ext4", "-q", "-O", "^has_journal", str(loopback_path)],
        check=True,
    )

    subprocess.run(
        ["sudo", "mount", "-o", "loop", str(loopback_path), str(mount_dir)],
        check=True,
    )

    subprocess.run(
        ["sudo", "chown", f"{os.getuid()}:{os.getgid()}", str(mount_dir)],
        check=True,
    )

    try:
        yield mount_dir
    finally:
        subprocess.run(["sudo", "umount", "-l", str(mount_dir)], check=False)


def test_load_async_io_fails(very_small_ext4):
    file_path = very_small_ext4 / "load.test"

    # Run async-io eternal-load on a small loopback ext4 filesystem to raise ENOSPC.
    result = __run_load_test(
        file_path,
        scenario="aligned",
        engine="asyncio",
        direct=True,
        timeout=60,
    )

    assert result.returncode == 1

    msg = (
        "Can't write to file: (yexception) "
        "(No space left on device) async IO operation failed"
    )
    assert msg in result.stderr
