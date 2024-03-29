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
_TIMEOUT = 30


def __run_load_test(file_name):
    eternal_load = yatest_common.binary_path(_BINARY_PATH)

    params = [
        eternal_load,
        '--config-type', 'generated',
        '--blocksize', str(_BLOCKSIZE),
        '--request-block-count', str(_REQUEST_BLOCK_COUNT),
        '--file', file_name,
        '--filesize', str(_FILE_SIZE),
        '--iodepth', str(_IO_DEPTH),
        '--write-rate', '70'
    ]

    return run(params, stdout=PIPE, stderr=PIPE, universal_newlines=True, timeout=_TIMEOUT)


def test_load_fails():
    tmp_file = tempfile.NamedTemporaryFile(suffix=".test")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(__run_load_test, tmp_file.name)
        time.sleep(_TIMEOUT / 2)

        cnt = 0
        while future.running():
            tmp_file.seek(int(cnt * _REQUEST_SIZE))
            block = b'0' * _REQUEST_SIZE
            tmp_file.write(block)
            cnt += 1
            cnt %= _REQUEST_COUNT

        result = future.result()
        assert (result.returncode == 1) and (result.stderr.find('Wrong') != -1)


def test_load_works():
    tmp_file = tempfile.NamedTemporaryFile(suffix=".test")
    try:
        assert __run_load_test(tmp_file.name).returncode == 0
    except TimeoutExpired:
        pass
    else:
        pytest.fail(f"Eternal load should not have finished in {_TIMEOUT} seconds")
