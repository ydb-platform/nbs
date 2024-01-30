import pytest
import yatest.common as common

from cloud.blockstore.config.server_pb2 import \
    TKikimrServiceConfig, TServerAppConfig, TServerConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import run_test


class TestCase(object):

    def __init__(
        self,
        name,
        config_path,
        track_filter,
        max_read_bandwidth,
        max_write_bandwidth,
        max_read_iops,
        max_write_iops,
        max_burst_time
    ):
        self.name = name
        self.config_path = config_path
        self.track_filter = track_filter
        self.max_read_bandwidth = max_read_bandwidth
        self.max_write_bandwidth = max_write_bandwidth
        self.max_read_iops = max_read_iops
        self.max_write_iops = max_write_iops
        self.max_burst_time = max_burst_time


LIMITS = {
    "Low": {
        "MaxReadBandwidth":  10 * 1024 * 1024,   # 10 MiB
        "MaxWriteBandwidth": 10 * 1024 * 1024,   # 10 MiB
        "MaxReadIops":       10,
        "MaxWriteIops":      10,
        "MaxBurstTime":      1 * 1000            # 1 sec
    },
}


TESTS = [
    TestCase(
        "fixed-requests-low-limits",
        "cloud/blockstore/tests/loadtest/local-throttling/fixed-requests.txt",
        "RequestPostponed_ThrottlingService",
        LIMITS["Low"]["MaxReadBandwidth"],
        LIMITS["Low"]["MaxWriteBandwidth"],
        LIMITS["Low"]["MaxReadIops"],
        LIMITS["Low"]["MaxWriteIops"],
        LIMITS["Low"]["MaxBurstTime"]
    ),
]


def __run_test(test_case):
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThrottlingEnabled = True
    server.ServerConfig.MaxReadBandwidth = test_case.max_read_bandwidth
    server.ServerConfig.MaxWriteBandwidth = test_case.max_write_bandwidth
    server.ServerConfig.MaxReadIops = test_case.max_read_iops
    server.ServerConfig.MaxWriteIops = test_case.max_write_iops
    server.ServerConfig.MaxBurstTime = test_case.max_burst_time
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        tracking_enabled=True,
        use_in_memory_pdisks=True,
    )

    try:
        ret = run_test(
            "throttling-%s" % test_case.name,
            common.source_path(test_case.config_path),
            env.nbs_port,
            env.mon_port,
            nbs_log_path=env.nbs_log_path,
            track_filter=test_case.track_filter,
            env_processes=[env.nbs],
        )
    finally:
        env.tear_down()

    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_load(test_case):
    return __run_test(test_case)
