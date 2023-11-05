import signal
import subprocess
import time

import yatest.common as common
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count


BINARY_PATH = common.binary_path("cloud/blockstore/apps/client/blockstore-client")
BLOCK_SIZE = 4096
BLOCKS_COUNT = 64 * 1024 * 1024 // BLOCK_SIZE
KEEPALIVE_TIME_MS = 10
KEEPALIVE_TIMEOUT_MS = 10


class TestEnv(LocalLoadTest):
    def __wrap(self, *args):
        return [BINARY_PATH] + list(args) + [
            "--host", "localhost",
            "--port", str(self.nbs_port)]

    def run(self, *args):
        assert subprocess.call(self.__wrap(*args), stdout=self.results_file) == 0

    def run_async(self, *args):
        return subprocess.Popen(self.__wrap(*args))


def setup():
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.ServerConfig.GrpcKeepAliveTime = KEEPALIVE_TIME_MS
    server.ServerConfig.GrpcKeepAliveTimeout = KEEPALIVE_TIMEOUT_MS
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = TestEnv(
        "",
        server_app_config=server,
        use_in_memory_pdisks=False)

    env.results_path = yatest_common.output_path() + "/results.txt"
    env.results_file = open(env.results_path, "w")

    return env


def freeze(proc, duration):
    proc.send_signal(signal.SIGSTOP)
    time.sleep(duration)
    proc.send_signal(signal.SIGCONT)


def stagger(proc, delta):
    status = None

    while status is None:
        try:
            status = proc.wait(delta)
        except Exception:
            freeze(proc, delta)


def test_keepalive():
    env = setup()

    env.run(
        "createvolume",
        "--disk-id", "volume-0",
        "--blocks-count", str(BLOCKS_COUNT))

    reader = env.run_async(
        "readblocks",
        "--disk-id", "volume-0",
        "--read-all")

    stagger(reader, KEEPALIVE_TIMEOUT_MS / 1000.0)
    assert reader.wait() == 0

    env.tear_down()
