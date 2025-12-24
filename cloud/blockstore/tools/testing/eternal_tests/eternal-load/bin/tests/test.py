import os
import subprocess
import pytest
import tempfile
import time

from subprocess import run, PIPE, TimeoutExpired
from concurrent.futures import ThreadPoolExecutor

import yatest.common as yatest_common

_FILE_SIZE = 1  # GB
_IO_DEPTH = 8
_BLOCKSIZE = 4096  # KB
_REQUEST_BLOCK_COUNT = 3
_REQUEST_SIZE = _REQUEST_BLOCK_COUNT * _BLOCKSIZE
_REQUEST_COUNT = (_FILE_SIZE * 1024 ** 3) / _REQUEST_SIZE
_BINARY_PATH = 'cloud/blockstore/tools/testing/eternal_tests/eternal-load/bin/eternal-load'

_SCENARIOS = [
    ("aligned", "asyncio", True),
    ("aligned", "sync", False),
    ("unaligned", "sync", False)
]

_SIMPLE_SCENARIOS = [
    ("sequential", "sync", False),
    ("random", "sync", False),
]


def __run_load_test(file_name, scenario="aligned", engine="asyncio", direct=True, timeout=None, test_count=0):
    eternal_load = yatest_common.binary_path(_BINARY_PATH)

    params = [
        eternal_load,
        '--config-type', 'generated',
        '--scenario', scenario,
        '--engine', engine,
        '--blocksize', str(_BLOCKSIZE),
        '--request-block-count', str(_REQUEST_BLOCK_COUNT),
        '--file', file_name,
        '--filesize', str(_FILE_SIZE),
        '--iodepth', str(_IO_DEPTH),
        '--write-rate', '70',
        '--debug',
        '--test-count', str(test_count)
    ]

    if not direct:
        params.append('--no-direct')

    result = run(params, stdout=PIPE, stderr=PIPE, universal_newlines=True, timeout=timeout)
    print(result.stderr)
    return result


@pytest.mark.parametrize("scenario,engine,direct", _SCENARIOS)
def test_load_fails(scenario, engine, direct):
    tmp_file = tempfile.NamedTemporaryFile(suffix=".test")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(__run_load_test, tmp_file.name, scenario, engine, direct)
        time.sleep(20)

        cnt = 0
        while future.running():
            tmp_file.seek(int(cnt * _REQUEST_SIZE))
            block = b'0' * _REQUEST_SIZE
            tmp_file.write(block)
            cnt += 1
            cnt %= _REQUEST_COUNT

        result = future.result()
        assert (result.returncode == 1) and (result.stderr.find('Wrong') != -1) and (result.stderr.find('MiB/s') != -1)


@pytest.mark.parametrize("scenario,engine,direct", _SCENARIOS + _SIMPLE_SCENARIOS)
def test_load_works(scenario, engine, direct):
    timeout = 30
    tmp_file = tempfile.NamedTemporaryFile(suffix=".test")
    try:
        assert __run_load_test(tmp_file.name, scenario, engine, direct, timeout).returncode == 0
    except TimeoutExpired:
        pass
    else:
        pytest.fail(f"Eternal load should not have finished within {timeout} seconds")


@pytest.fixture
def fixture_mount_very_small_tmpfs(request):
    mount_dir = "/tmp/testfs"
    os.makedirs(mount_dir, exist_ok=True)
    mount_cmd = ["sudo", "mount", "-t", "tmpfs", "-o", "size=1K", "tmpfs", mount_dir]
    subprocess.run(mount_cmd, check=True)

    def fin():
        unmount_cmd = ["sudo", "umount", "-l", mount_dir]
        subprocess.run(unmount_cmd, check=False)

    request.addfinalizer(fin)
    return mount_dir


def test_load_async_io_fails(fixture_mount_very_small_tmpfs):
    mount_dir = fixture_mount_very_small_tmpfs

    # Run async-io eternal-load on a small tmpfs to raise ENOSPC error
    with (
        ThreadPoolExecutor(max_workers=1) as executor,
        tempfile.NamedTemporaryFile(suffix='.test', dir=mount_dir) as tmp_file
    ):
        # Linux native aio does not support IO_DIRECT on tmpfs
        future = executor.submit(
            __run_load_test, tmp_file.name, 'aligned', 'asyncio', False)

        result = future.result()

        assert result.returncode == 1
        msg = 'Can\'t write to file: (yexception) (No space left on device) ' \
            'async IO operation failed'
        assert result.stderr.find(msg) != -1


def test_multiple_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        timeout = 10
        pattern = os.path.join(tmpdir, "testfile_{}.test")
        test_count = 5
        tmp_files = [pattern.format(i) for i in range(test_count)]

        try:
            assert __run_load_test(pattern, scenario="unaligned", engine="sync", direct=False, timeout=timeout, test_count=test_count).returncode == 0
        except TimeoutExpired:
            pass
        else:
            pytest.fail(f"Eternal load should not have finished within {timeout} seconds")

        for f in tmp_files:
            assert os.path.exists(f), f"Expected file {f} to exist"

        assert not os.path.exists(pattern), f"Pattern string {pattern} should not have been created as a literal file"
