import os
import pytest

from subprocess import run, PIPE, TimeoutExpired

import yatest.common as common

from cloud.filestore.tests.python.lib.common import get_filestore_mount_path


_BINARY_PATH = 'cloud/blockstore/tools/testing/eternal_tests/eternal-load/bin/eternal-load'


class TestCase:
    def __init__(
        self,
        name,
        filesize="10m",
        iodepth=2,
        blocksize=4096,
        request_block_count=1,
        write_rate=100,
        alternating_phase="1m",
        scenario="aligned",
        timeout_seconds=180,
    ):
        self.name = name
        self.filesize = filesize
        self.iodepth = iodepth
        self.blocksize = blocksize
        self.request_block_count = request_block_count
        self.write_rate = write_rate
        self.alternating_phase = alternating_phase
        self.scenario = scenario
        self.timeout_seconds = timeout_seconds


TESTS = [
    TestCase(
        name="alternating-rw-2-parts",
        filesize="10m",
        iodepth=2,
        blocksize=4096,
        write_rate=100,
        alternating_phase="1m",
    ),
]


def run_eternal_load(test_case, file_path, dump_config_path):
    eternal_load = common.binary_path(_BINARY_PATH)

    params = [
        eternal_load,
        '--config-type', 'generated',
        '--scenario', test_case.scenario,
        '--engine', 'sync',
        '--no-direct',
        '--blocksize', str(test_case.blocksize),
        '--request-block-count', str(test_case.request_block_count),
        '--file', file_path,
        '--filesize', test_case.filesize,
        '--iodepth', str(test_case.iodepth),
        '--write-rate', str(test_case.write_rate),
        '--alternating-phase', test_case.alternating_phase,
        '--dump-config-path', dump_config_path,
        '--debug',
    ]

    result = run(
        params,
        stdout=PIPE,
        stderr=PIPE,
        universal_newlines=True,
        timeout=test_case.timeout_seconds,
    )

    print(f"stdout: {result.stdout}")
    print(f"stderr: {result.stderr}")

    return result


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_eternal_load(test_case):
    mount_dir = get_filestore_mount_path()
    file_path = os.path.join(mount_dir, f"testfile_{test_case.name}")
    dump_config_path = os.path.join(common.output_path(), f"load-config-{test_case.name}.json")

    try:
        result = run_eternal_load(test_case, file_path, dump_config_path)
        # eternal-load should not finish - it runs forever
        # if it finishes with returncode 0, something is wrong
        # returncode 1 means validation error (data corruption)
        assert result.returncode != 1, f"Data validation failed: {result.stderr}"
        pytest.fail(
            f"Eternal load should not have finished within {test_case.timeout_seconds} seconds"
        )
    except TimeoutExpired:
        # This is expected - eternal load runs forever
        pass
