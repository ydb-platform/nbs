import pytest
import yatest.common as common

from cloud.blockstore.tests.python.lib.test_base import run_test

DEFAULT_MON_PORT = 8766

TESTS = [
    [
        "myt1-ct5-13",
        "cloud/blockstore/tests/loadtest/remote/remote-myt1-ct5-13.txt",
        "myt1-ct5-13.cloud.yandex.net",
        9766,
        DEFAULT_MON_PORT
    ],
    [
        "myt1-ct5-14",
        "cloud/blockstore/tests/loadtest/remote/remote-myt1-ct5-14.txt",
        "myt1-ct5-14.cloud.yandex.net",
        9766,
        DEFAULT_MON_PORT
    ],
    [
        "remote-overlay-disk-v1",
        "cloud/blockstore/tests/loadtest/remote/remote-overlay-disk-v1.txt",
        "myt1-ct5-15.cloud.yandex.net",
        9766,
        DEFAULT_MON_PORT
    ],
    [
        "remote-overlay-disk-v2",
        "cloud/blockstore/tests/loadtest/remote/remote-overlay-disk-v2.txt",
        "myt1-ct5-15.cloud.yandex.net",
        9766,
        DEFAULT_MON_PORT
    ]
]


def __run_test(test_name, config_path, host, port, mon_port):
    return run_test(
        test_name,
        config_path,
        port,
        mon_port,
        host
    )


@pytest.mark.parametrize("args", TESTS, ids=[x[0] for x in TESTS])
def test_load(args):
    config = common.get_param("config")
    if config is None:
        __run_test(
            args[0],
            config_path=common.source_path(args[1]),
            host=args[2],
            port=args[3],
            mon_port=args[4])


def test_load_custom():
    config = common.get_param("config")
    if config is not None:
        __run_test(
            "custom",
            config_path=common.source_path(config),
            host=common.get_param("host"),
            port=common.get_param("port"),
            mon_port=common.get_param("mon_port", DEFAULT_MON_PORT))
