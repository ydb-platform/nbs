import os
import pytest

import yatest.common as common

from cloud.blockstore.tests.python.lib.test_base import run_test


class TestCase(object):

    def __init__(self, name: str, scenario_filename: str, volume_size: int, scenario_args: dict):
        self.name = name
        self.scenario_filename = scenario_filename
        self.volume_size = volume_size
        self.scenario_args = scenario_args


def _generate_test_cases():
    return [
        TestCase(
            "artificial_selftest",
            "artificial_scenario.txt",
            10 * 1024 * 1024,
            dict()
        ),
        TestCase(
            "real_load_spec_selftest",
            "real_load_spec_scenario.txt",
            369623040 * 4096,
            {
                "$PROFILE_LOG_FILE": common.build_path(
                    "cloud/blockstore/tests/loadtest/selftest/data/short.log"
                )
            }
        )
    ]


TESTS = _generate_test_cases()


@pytest.mark.parametrize("params", TESTS, ids=[x.name for x in TESTS])
def test_load(params):
    test_file_path = params.name + ".bin"
    f = open(test_file_path, "w")
    f.truncate(params.volume_size)
    f.close()

    test_config_path = common.source_path(
        os.path.join(
            "cloud/blockstore/tests/loadtest/selftest",
            params.scenario_filename
        )
    )
    with open(test_config_path) as f:
        test_config = f.read()

    params.scenario_args["$FILE"] = os.path.abspath(test_file_path)

    for arg, value in params.scenario_args.items():
        test_config = test_config.replace(arg, value)

    processed_test_config_path = params.scenario_filename
    with open(processed_test_config_path, "w") as f:
        f.write(test_config)

    return run_test(
        params.name,
        os.path.abspath(processed_test_config_path),
        0,
        0,
        host="filesystem",
    )
