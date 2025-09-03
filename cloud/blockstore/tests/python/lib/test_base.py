from __future__ import print_function

import json
import logging
import os
import requests
import retrying
import socket
import sys
import tempfile
import time
import urllib3
import uuid
import logging

import yatest.common as common
import contrib.ydb.tests.library.common.yatest_common as yatest_common
from contrib.ydb.tests.library.harness.daemon import Daemon

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.config.client_pb2 import TClientConfig
from cloud.blockstore.tests.python.lib.stats import dump_stats

from cloud.blockstore.public.sdk.python.client.grpc_client import GrpcClient
from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client.error_codes import EFacility

from library.python.testing.recipe import set_env

from google.protobuf import text_format


logger = logging.getLogger(__name__)

def get_restart_interval():
    if common.context.sanitize is not None:
        return 30
    return 15


def thread_count():
    return int(common.get_param("threads", 2))


def counters_url(host, mon_port):
    return "http://%s:%s/counters/counters=blockstore/json" % (host, mon_port)


def _extract_tracks(nbs_log_path, track_filter):
    if nbs_log_path is None:
        return []
    tracks = []
    with open(nbs_log_path) as f:
        for line in f.readlines():
            if track_filter is not None and line.find(track_filter) < 0:
                continue
            parts = line.rstrip().split("BLOCKSTORE_TRACE WARN: ")
            if len(parts) == 2:
                tracks.append(json.loads(parts[1]))

    return tracks


_STABLE_PROBES = []

for request in ["ZeroBlocks", "WriteBlocks", "ReadBlocks"]:
    _STABLE_PROBES += [
        ("RequestStarted", request),
        ("RequestSent_Proxy", request),
        ("RequestReceived_Service", request),
        ("RequestReceived_ThrottlingService", request),
        ("RequestPostponed_ThrottlingService", request),
        ("RequestAdvanced_ThrottlingService", request),
        ("RequestReceived_Volume", request),
        ("RequestPostponed_Volume", request),
        ("RequestAdvanced_Volume", request),
        # ("ResponseReceived_Proxy", request),
        ("ResponseSent", request),
    ]


def _dump_tracks(tracks, f):
    signatures = set()
    for track in tracks:
        if not len(track):
            continue

        if len(track[0]) < 3:
            continue

        if (track[0][0], track[0][2]) not in _STABLE_PROBES:
            continue

        signature = []
        for item in track:
            if len(item) < 3:
                continue
            if (item[0], item[2]) not in _STABLE_PROBES:
                continue
            s = "%s:%s" % (item[0], item[2])
            if s not in signature:
                signature.append(s)
        signatures.add(json.dumps(signature))
    for s in sorted(list(signatures)):
        print(s, file=f)


class LoadTest(Daemon):

    def __init__(
            self,
            test_name,
            config_path,
            nbs_port,
            host="localhost",
            client_config=TClientConfig(),
            enable_tls=False,
            endpoint_storage_dir=None,
            spdk_config=None):

        self.__test_name = test_name
        self.__results_path = yatest_common.output_path() + "/results.txt"

        binary_path = common.binary_path(
            "cloud/blockstore/tools/testing/loadtest/bin/blockstore-loadtest")

        params = [
            binary_path,
            "--host", host,
            "--secure-port" if enable_tls else "--port", str(nbs_port),
            "--config", config_path,
            "--results", self.__results_path,
            "--verbose", "info"
        ]

        if client_config is not None:
            client_config_path = yatest_common.output_path() + "/client.txt"
            with open(client_config_path, "w") as f:
                client_config.ThreadsCount = thread_count()
                f.write(str(client_config))
            params.extend(["--client-config", client_config_path])

        if spdk_config is not None:
            spdk_config_path = yatest_common.output_path() + "/spdk.txt"
            with open(spdk_config_path, "w") as f:
                f.write(str(spdk_config))
            params.extend(["--spdk-config", spdk_config_path])

        if endpoint_storage_dir is not None:
            params.extend(["--endpoint-storage-dir", endpoint_storage_dir])

        super(LoadTest, self).__init__(
            command=params,
            cwd=common.output_path(),
            timeout=180,
            stdout_file=os.path.join(common.output_path(), "%s_stdout.txt" % test_name),
            stderr_file=os.path.join(common.output_path(), "%s_stderr.txt" % test_name))

    def create_canonical_files(
            self,
            mon_port=None,
            host="localhost",
            stat_filter=None,
            nbs_log_path=None,
            track_filter=None):

        if self.daemon.exit_code != 0:
            return None

        canonical_files = []

        full_stats_path = os.path.join(common.output_path(), "full_stats_%s.txt" % self.__test_name)

        if stat_filter is not None:
            stats_path = os.path.join(common.output_path(), "stats_%s.txt" % self.__test_name)
            with open(stats_path, "w") as f, open(full_stats_path, "w") as ff:
                dump_stats(counters_url(host, mon_port), stat_filter, f, ff)
            canonical_files.append(common.canonical_file(stats_path, local=True))

        with open(self.__results_path, "r") as f:
            for s in f:
                s = s.strip()
                if len(s) == 0:
                    continue

                try:
                    data = json.loads(s)
                except ValueError as e:
                    logging.error("Failed to parse json: {}. Error: {}".format(s, e))
                    continue

                if "TestResults" not in data:
                    continue

                svr = data["StatVolumeResponse"]
                assert "WriteAndZeroRequestsInFlight" not in svr, \
                    "there should be zero write and zero requests in flight"

                tr = data["TestResults"]

                delta = (float)(tr["EndTime"] - tr["StartTime"]) / 1000000
                iops = (float)(tr["RequestsCompleted"]) / delta
                print("iops = {}".format(iops), file=sys.stderr)
                if "BlocksRead" in data:
                    bw = (float)(tr["BlocksRead"] * 4096) / delta
                    print("read performance = {} MB/s".format(bw / 1024 ** 2), file=sys.stderr)
                    print("read latency = " + json.dumps(tr["ReadLatency"]), file=sys.stderr)
                if "BlocksWritten" in tr:
                    bw = (float)(tr["BlocksWritten"] * 4096) / delta
                    print("write performance = {} MB/s".format(bw / 1024 ** 2), file=sys.stderr)
                    print("write latency = " + json.dumps(tr["WriteLatency"]), file=sys.stderr)
                if "BlocksZeroed" in tr:
                    bw = (float)(tr["BlocksZeroed"] * 4096) / delta
                    print("zero performance = {} MB/s".format(bw / 1024 ** 2), file=sys.stderr)
                    print("zero latency = " + json.dumps(tr["ZeroLatency"]), file=sys.stderr)

        tracks = _extract_tracks(nbs_log_path, track_filter)
        if len(tracks):
            tracks_path = os.path.join(common.output_path(), "tracks_%s.txt" % self.__test_name)
            with open(tracks_path, "w") as f:
                _dump_tracks(tracks, f)
            canonical_files.append(common.canonical_file(tracks_path, local=True))

        return canonical_files if len(canonical_files) else None


def run_test(
        test_name,
        config_path,
        nbs_port,
        mon_port,
        host="localhost",
        stat_filter=None,
        nbs_log_path=None,
        client_config=TClientConfig(),
        enable_tls=False,
        track_filter=None,
        endpoint_storage_dir=None,
        spdk_config=None,
        env_processes=[]):

    r = LoadTest(
        test_name,
        config_path,
        nbs_port,
        host,
        client_config,
        enable_tls,
        endpoint_storage_dir,
        spdk_config)

    def kill_all():
        for p in env_processes + [r]:
            try:
                p.kill()
            except Exception as e:
                logging.warning(e)

    r.start()

    while r.is_alive():
        time.sleep(5)
        dead = [p for p in env_processes if not p.is_alive()]
        crashed = 0
        for p in dead:
            if p.returncode in [-9, -15]:
                # being killed is fine - some of our tests kill our processes
                # e.g. tests with regular restarts
                continue

            crashed += 1
            logging.warning(
                "Found a terminated process: {}, {}.".format(
                    p.command,
                    p.returncode
                ))
        if crashed > 0:
            kill_all()
            raise Exception(
                'Several ({}) processes terminated prematurely.'.format(crashed))

    r.stop()

    return r.create_canonical_files(
        mon_port,
        host,
        stat_filter,
        nbs_log_path,
        track_filter)


def is_grpc_error(exception):
    if isinstance(exception, ClientError):
        return exception.facility == EFacility.FACILITY_GRPC.value

    return False


def is_request_error(exception):
    return isinstance(exception, requests.exceptions.RequestException)


@retrying.retry(stop_max_delay=60000, wait_fixed=1000, retry_on_exception=is_grpc_error)
def wait_for_nbs_server(port):
    '''
    Ping NBS server with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''

    with GrpcClient(str("localhost:%d" % port)) as grpc_client:
        grpc_client.ping(protos.TPingRequest())


@retrying.retry(stop_max_delay=60000, wait_fixed=1000)
def wait_for_nbs_server_proxy(secure_port):
    '''
    Ping NBS server proxy with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    r = requests.post(str("https://localhost:%d/ping" % secure_port), verify=False)
    r.raise_for_status()


@retrying.retry(stop_max_delay=60000, wait_fixed=1000)
def wait_for_disk_agent(mon_port):
    '''
    Check disk agent mon_port with delay between attempts to ensure
    it is running and listening by the moment the actual test execution begins
    '''

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        result = sock.connect_ex(('localhost', mon_port))

    if result != 0:
        raise RuntimeError("Failed to connect to disk agent")


@retrying.retry(stop_max_delay=60000, wait_fixed=3000, retry_on_exception=is_request_error)
def get_nbs_counters(mon_port):
    r = requests.get(counters_url('localhost', mon_port), timeout=10)
    r.raise_for_status()

    return r.json()


def get_free_socket_path(name):
    file_name = str(uuid.uuid4()) + "_" + name
    unix_socket_path = tempfile.gettempdir() + "/" + file_name

    # Chop path because it's limited in some environments.
    UDS_PATH_LEN_LIMIT = 100
    if len(unix_socket_path) > UDS_PATH_LEN_LIMIT:
        to_chop = min(len(unix_socket_path) - UDS_PATH_LEN_LIMIT, len(file_name) - 1)
        unix_socket_path = unix_socket_path[:-to_chop]
    assert len(unix_socket_path) <= UDS_PATH_LEN_LIMIT

    if os.path.exists(unix_socket_path):
        os.remove(unix_socket_path)

    return unix_socket_path


def get_file_size(filename):
    fd = os.open(filename, os.O_RDONLY)
    try:
        return os.lseek(fd, 0, os.SEEK_END)
    finally:
        os.close(fd)


def get_nbs_device_path(path=None):
    if not path:
        path = os.getenv("NBS_DEVICE_PATH")

    if not path:
        raise RuntimeError("Invalid path")

    if not os.path.exists(path):
        raise RuntimeError("Path does not exist: {}".format(path))

    return path


def get_nbs_device_path_by_index(index):
        path = (os.getenv(env_with_guest_index("NBS_DEVICE_PATH", index)))
        logger.info("path from NBS_{}_DEVICE_PATH: {}".format(index, path))

        if not path:
            raise RuntimeError("Invalid index")

        if not os.path.exists(path):
            logger.info("path with index {} doen't exist: {}".format(index, path))
            raise RuntimeError("Path does not exist: {}".format(path))

        logger.info("path with index {} exist: {}".format(index, path))
        return path


def get_all_nbs_paths(nbs_instance_count):
    paths = []

    for i in range(nbs_instance_count):
        path = (os.getenv("NBS_{}_DEVICE_PATH".format(i)))
        logger.info("path from NBS_{}_DEVICE_PATH: {}".format(i, path))

        if not path:
            raise RuntimeError("Invalid path")

        if not os.path.exists(path):
            raise RuntimeError("Path does not exist: {}".format(path))

        logger.info("this path exist: {}".format(path))
        paths.append(path)

    logger.info("find paths: {}".format(len(paths)))
    return paths


def get_sensor(sensors, default_value, **kwargs):
    for sensor in sensors:
        labels = sensor['labels']
        if all([labels.get(n) == v for n, v in kwargs.items()]):
            return sensor.get('value', default_value)
    return default_value


def get_sensor_by_name(sensors, component, name, def_value=None):
    return get_sensor(
        sensors,
        default_value=def_value,
        component=component,
        sensor=name)


def __get_free_bytes(sensors, pool, default_value=0):
    bytes = get_sensor(
        sensors,
        default_value=default_value,
        component='disk_registry',
        sensor='FreeBytes',
        pool=pool)

    return bytes if bytes is not None else default_value


def __get_dirty_devices(sensors, pool, default_value=0):
    return get_sensor(
        sensors,
        default_value=default_value,
        component='disk_registry',
        sensor='DirtyDevices',
        pool=pool)


# wait for DiskAgent registration & secure erase
def wait_for_free_bytes(mon_port, pool='default'):
    while True:
        logging.info("Wait for agents...")
        time.sleep(1)
        sensors = get_nbs_counters(mon_port)['sensors']
        bytes = __get_free_bytes(sensors, pool)

        if bytes > 0:
            logging.info("Bytes: {}".format(bytes))
            break


# wait for DA & secure erase of all available devices
def wait_for_secure_erase(mon_port, pool='default', expectedAgents=1):
    seen_zero_free_bytes = False

    while True:
        logging.info("Wait for agents ...")
        time.sleep(1)
        sensors = get_nbs_counters(mon_port)['sensors']
        agents = get_sensor_by_name(sensors, 'disk_registry', 'AgentsInOnlineState', 0)
        if agents < expectedAgents:
            continue

        logging.info("Agents: {}".format(agents))

        dd = __get_dirty_devices(sensors, pool)
        logging.info("Dirty devices: {}".format(dd))

        if dd:
            continue

        bytes = __get_free_bytes(sensors, pool)
        logging.info("Bytes: {}".format(bytes))

        if not bytes:
            logging.error(
                "There is no free space in '%s'. Sensors: %s",
                pool,
                json.dumps(sensors, indent=4))

            # Since counters are incremented one by one, there can be a
            # situation where the dirty device counter "dd" is already 0, but
            # the free bytes counter "bytes" is still 0. To ensure all counters
            # are published, we need to do one more loop.
            if not seen_zero_free_bytes:
                seen_zero_free_bytes = True
                continue

        assert bytes != 0

        break


################################################################################
# result comparison utils

def files_equal(path_0, path_1, cb=None, block_size=4096):
    file_0 = open(path_0, "rb")
    file_1 = open(path_1, "rb")

    block_no = 0
    while True:
        block_0 = file_0.read(block_size)
        block_1 = file_1.read(block_size)

        if block_0 != block_1:
            if cb is None or not cb(block_no, block_0, block_1):
                return False

        if not block_0:
            break

        block_no += 1

    return True


def compare_bitmaps(path_0, path_1, block_size=4096):
    errors = []

    def cb(block_no, block_0, block_1):
        per_block = block_size * 8
        offset = per_block * block_no

        for i in range(block_size):
            byte_offset = offset + i * 8
            byte_0 = ord(block_0[i])
            byte_1 = ord(block_1[i])
            for j in range(8):
                bit_offset = byte_offset + j
                bit_0 = (byte_0 >> j) & 1
                bit_1 = (byte_1 >> j) & 1
                if bit_0 != bit_1:
                    errors.append("bit %s: %s -> %s" % (bit_offset, bit_0, bit_1))

    files_equal(path_0, path_1, cb)

    return errors


def file_equal(file_path, str):
    file_content = ''
    with open(file_path, 'r') as file:
        file_content = file.read()
    return file_content == str


def file_parse(file_path, proto_type):
    file_content = ''
    with open(file_path, 'r') as file:
        file_content = file.read()
    return text_format.Parse(file_content, proto_type)


def file_parse_as_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def env_with_guest_index(env: str, guest_index: int) -> str:
    if guest_index == 0:
        return env

    return "{}__{}".format(env, guest_index)

def recipe_set_env(key: str, val: str, guest_index=0):
    set_env(env_with_guest_index(key, guest_index), val)
