import json
import logging
import os
import random
import subprocess
import tempfile
import threading
import time

import yatest.common as common
import yatest.common.network as network
import ydb.tests.library.common.yatest_common as yatest_common

from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.public.api.protos.placement_pb2 import (
    TListPlacementGroupsResponse,
    TDescribePlacementGroupResponse,
    TDescribePlacementGroupRequest,
    EPlacementStrategy,
)
from cloud.blockstore.public.api.protos.volume_pb2 import (
    TDescribeVolumeRequest,
    TDescribeVolumeResponse,
    TListVolumesResponse,
)
from cloud.storage.core.protos.endpoints_pb2 import (
    EEndpointStorageType,
)
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.nonreplicated_setup import (
    enable_writable_state,
    setup_disk_registry_config_simple,
)
from cloud.blockstore.tests.python.lib.test_base import thread_count, get_nbs_counters, get_sensor_by_name
from contrib.ydb.core.protos import msgbus_pb2 as msgbus
from contrib.ydb.core.protos import console_config_pb2 as console
from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from contrib.ydb.core.protos.config_pb2 import TStaticNameserviceConfig
from contrib.ydb.tests.library.common.yatest_common import PortManager
from contrib.ydb.library.actors.protos.interconnect_pb2 import TNodeLocation

from google.protobuf import text_format


BINARY_PATH = common.binary_path("cloud/blockstore/apps/client/blockstore-client")
WRITEREQ_FILE = "writereq.bin"
DATA_FILE = "/dev/urandom"
BLOCK_SIZE = 4096
BLOCKS_COUNT = 25000
SAMPLE_SIZE = 100
CHANGED_BLOCKS_COUNT = 1234

NRD_BLOCKS_COUNT = 1024**3 // BLOCK_SIZE


################################################################################
# proc utils

def run_async(job, stderr, stdout, cwd=None):
    def run():
        with open(os.path.join(common.output_path(), stderr), "w") as err:
            with open(os.path.join(common.output_path(), stdout), "w") as out:
                subprocess.call(job, stderr=err, stdout=out, cwd=cwd)
    t = threading.Thread(target=run)
    t.setDaemon(True)
    t.start()


################################################################################
# result comparison utils

def files_equal(path_0, path_1, cb=None):
    file_0 = open(path_0, "rb")
    file_1 = open(path_1, "rb")

    block_no = 0
    while True:
        block_0 = file_0.read(BLOCK_SIZE)
        block_1 = file_1.read(BLOCK_SIZE)

        if block_0 != block_1:
            if cb is None or not cb(block_no, block_0, block_1):
                return False

        if not block_0:
            break

        block_no += 1

    return True


def compare_bitmaps(path_0, path_1):
    errors = []

    def cb(block_no, block_0, block_1):
        per_block = BLOCK_SIZE * 8
        offset = per_block * block_no

        for i in range(BLOCK_SIZE):
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


################################################################################
# client cmd helpers

def list_placement_groups(env, run):
    clear_file(env.results_file)
    run("listplacementgroups",
        "--proto")
    return file_parse(env.results_path, TListPlacementGroupsResponse())


def list_volumes(env, run):
    clear_file(env.results_file)
    run("listvolumes",
        "--proto")
    return file_parse(env.results_path, TListVolumesResponse())


def describe_placement_group(env, run, group_id):
    clear_file(env.results_file)
    req = TDescribePlacementGroupRequest()
    req.GroupId = group_id
    tmp_file = tempfile.NamedTemporaryFile(suffix=".tmp")
    tmp_file.write(text_format.MessageToString(req).encode("utf8"))
    tmp_file.flush()

    run("describeplacementgroup",
        "--input", tmp_file.name,
        "--proto")
    return file_parse(env.results_path, TDescribePlacementGroupResponse())


def describe_volume(env, run, disk_id):
    clear_file(env.results_file)
    req = TDescribeVolumeRequest()
    req.DiskId = disk_id
    tmp_file = tempfile.NamedTemporaryFile(suffix=".tmp")
    tmp_file.write(text_format.MessageToString(req).encode("utf8"))
    tmp_file.flush()

    run("describevolume",
        "--input", tmp_file.name,
        "--proto")
    return file_parse(env.results_path, TDescribeVolumeResponse())


def clear_file(file):
    file.seek(0)
    file.truncate()


def update_cms_config(client, config):
    req = msgbus.TConsoleRequest()
    action = req.ConfigureRequest.Actions.add()
    action.AddConfigItem.ConfigItem.Kind = 3

    action.AddConfigItem.ConfigItem.Config.NameserviceConfig.CopyFrom(config)
    action.AddConfigItem.ConfigItem.MergeStrategy = console.TConfigItem.OVERWRITE

    response = client.invoke(req, 'ConsoleRequest')
    assert response.Status.Code == StatusIds.SUCCESS


def get_static_nodes(env, run):
    clear_file(env.results_file)
    run("ExecuteAction",
        "--action", "GetNameserverNodes",
        "--input-bytes", "",
        "--verbose")
    nodes = file_parse_as_json(env.results_path)

    static_nodes = list(
        filter(lambda node: node.get("IsStatic") is True, nodes["Nodes"]))
    for node in static_nodes:
        assert "Port" in node, "node = {}".format(node)
        node["Port"] = "<static_node_port>"
    return static_nodes


def send_two_node_nameservice_config(env):
    nameservice_config = TStaticNameserviceConfig()
    nameservice_config.Node.add(
        NodeId=1,
        Port=env.configurator.port_allocator.get_node_port_allocator(
            env.configurator.all_node_ids()[0]).ic_port,
        Host="localhost",
        InterconnectHost="localhost",
        WalleLocation=TNodeLocation(Body=1, DataCenter="1", Rack="1")
    )
    pm = PortManager()
    second_node_port = pm.get_port()
    nameservice_config.Node.add(
        NodeId=2,
        Port=second_node_port,
        Host="localhost",
        InterconnectHost="localhost",
        WalleLocation=TNodeLocation(Body=2, DataCenter="2", Rack="2")
    )
    update_cms_config(env.kikimr_cluster.client, nameservice_config)


################################################################################
# env setup/tear_down

def setup(
        with_nrd=False,
        nrd_device_count=1,
        rack='',
        storage_config_patches=None,
        server_config_patch=TServerConfig()):
    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(server_config_patch)
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = True
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        storage_config_patches=storage_config_patches,
        use_in_memory_pdisks=True,
        with_nrd=with_nrd,
        nrd_device_count=nrd_device_count,
        rack=rack)

    env.results_path = yatest_common.output_path() + "/results.txt"
    env.results_file = open(env.results_path, "w")

    def run(*args, **kwargs):
        args = [BINARY_PATH] + list(args) + [
            "--host", "localhost",
            "--port", str(env.nbs_port)]
        input = kwargs.get("input")
        if input is not None:
            input = (input + "\n").encode("utf8")

        process = subprocess.Popen(
            args,
            stdout=env.results_file,
            stdin=subprocess.PIPE,
            cwd=kwargs.get("cwd")
        )
        process.communicate(input=input)

        assert process.returncode == kwargs.get("code", 0)

    if with_nrd:
        enable_writable_state(env.nbs_port, BINARY_PATH)
        setup_disk_registry_config_simple(
            env.devices,
            env.nbs_port,
            BINARY_PATH)

        while True:
            sensors = get_nbs_counters(env.mon_port)['sensors']

            free_bytes = get_sensor_by_name(sensors, 'disk_registry', 'FreeBytes', -1)
            if free_bytes > 0:
                break
            logging.info('wait for free bytes ...')
            time.sleep(1)

    return env, run


def tear_down(env):
    if env is None:
        return
    env.results_file.close()
    env.tear_down()


def random_writes(run, block_count=BLOCKS_COUNT):
    sample = random.sample(range(block_count), SAMPLE_SIZE)
    sample.sort()

    for i in range(0, SAMPLE_SIZE, 2):
        start_index = sample[i]
        blocks_count = sample[i + 1] - start_index

        with open(DATA_FILE, "rb") as r:
            with open(WRITEREQ_FILE, "wb") as w:
                block = r.read(BLOCK_SIZE * blocks_count)
                w.write(block)

        run("writeblocks",
            "--disk-id", "volume-0",
            "--input", WRITEREQ_FILE,
            "--start-index", str(start_index))


################################################################################
# test cases

def test_successive_remounts_and_writes():
    env, run = setup()

    with open(WRITEREQ_FILE, "w") as wr:
        wr.write(" " * 1024 * 16)

    run("createvolume",
        "--disk-id", "vol0",
        "--blocks-count", str(BLOCKS_COUNT))

    run("writeblocks",
        "--disk-id", "vol0",
        "--start-index", "0",
        "--input", WRITEREQ_FILE)

    run("writeblocks",
        "--disk-id", "vol0",
        "--start-index", "0",
        "--input", WRITEREQ_FILE)

    run("readblocks",
        "--disk-id", "vol0",
        "--start-index", "0",
        "--blocks-count", "32")

    ret = common.canonical_file(env.results_path, local=True)
    tear_down(env)
    return ret


def test_read_all_with_io_depth():
    env, run = setup()

    run("createvolume",
        "--disk-id", "volume-0",
        "--blocks-count", str(BLOCKS_COUNT))

    random_writes(run)

    run("readblocks",
        "--disk-id", "volume-0",
        "--read-all",
        "--output", "readblocks-0")

    run("readblocks",
        "--disk-id", "volume-0",
        "--read-all",
        "--io-depth", "8",
        "--output", "readblocks-1")

    assert files_equal("readblocks-0", "readblocks-1")
    tear_down(env)


def test_read_with_io_depth():
    env, run = setup()

    run("createvolume",
        "--disk-id", "volume-0",
        "--blocks-count", str(BLOCKS_COUNT))

    random_writes(run)

    run("readblocks",
        "--disk-id", "volume-0",
        "--start-index", "0",
        "--blocks-count", "2000",
        "--output", "readblocks-0")

    run("readblocks",
        "--disk-id", "volume-0",
        "--start-index", "0",
        "--blocks-count", "2000",
        "--io-depth", "8",
        "--output", "readblocks-1")

    assert files_equal("readblocks-0", "readblocks-1")
    tear_down(env)


def test_flush_profile_log():
    env, run = setup()
    print("output_path = {}".format(yatest_common.output_path()))

    run("createvolume",
        "--disk-id", "volume-0",
        "--blocks-count", str(BLOCKS_COUNT))

    # The size is probably zero right now, but it is not guaranteed.
    profile_log_size_1 = os.stat(env.nbs_profile_log_path).st_size

    random_writes(run)
    run("ExecuteAction",
        "--action", "FlushProfileLog",
        "--input-bytes", "",
        "--verbose")
    profile_log_size_2 = os.stat(env.nbs_profile_log_path).st_size
    assert profile_log_size_2 > profile_log_size_1

    for i in range(0, 5):
        run("readblocks",
            "--disk-id", "volume-0",
            "--start-index", str(i * 10),
            "--blocks-count", "10",
            "--output", "readblocks-1")
    run("ExecuteAction",
        "--action", "FlushProfileLog",
        "--input-bytes", "",
        "--verbose")
    profile_log_size_3 = os.stat(env.nbs_profile_log_path).st_size
    assert profile_log_size_3 > profile_log_size_2

    tear_down(env)


def do_test_restore(run, disk_id, backup_disk_id, block_count, start_index, orig_data):

    # spoil disk content
    random_writes(run, block_count)

    corrupt_data = orig_data + ".corrupt"

    run("readblocks",
        "--disk-id", disk_id,
        "--start-index", str(start_index),
        "--blocks-count", str(block_count),
        "--output", corrupt_data)

    assert not files_equal(orig_data, corrupt_data)

    run("restorevolume",
        "--disk-id", disk_id,
        "--backup-disk-id", backup_disk_id,
        "--io-depth", "16",
        "--verbose")

    fixed_data = orig_data + ".fixed"

    run("readblocks",
        "--disk-id", disk_id,
        "--start-index", str(start_index),
        "--blocks-count", str(block_count),
        "--output", fixed_data)

    assert files_equal(orig_data, fixed_data)


def test_backup():
    env, run = setup()

    run("createvolume",
        "--disk-id", "volume-0",
        "--blocks-count", str(BLOCKS_COUNT),
        "--tablet-version", "2")

    random_writes(run)

    run("backupvolume",
        "--disk-id", "volume-0",
        "--backup-disk-id", "volume-1",
        "--checkpoint-id", "checkpoint-0",
        "--io-depth", "16",
        "--verbose")

    run("backupvolume",
        "--disk-id", "volume-0",
        "--backup-disk-id", "volume-1",
        "--checkpoint-id", "checkpoint-0",
        "--io-depth", "16",
        "--changed-blocks-count", str(CHANGED_BLOCKS_COUNT),
        "--verbose")

    run("getchangedblocks",
        "--disk-id", "volume-0",
        "--start-index", "0",
        "--blocks-count", str(BLOCKS_COUNT),
        "--output", "changedblocks-0")

    run("getchangedblocks",
        "--disk-id", "volume-1",
        "--start-index", "0",
        "--blocks-count", str(BLOCKS_COUNT),
        "--output", "changedblocks-1")

    """
    with open("changedblocks-0", "r+b") as f:
        f.seek(128)
        f.write(bytearray([0 for i in range(128)]))

    with open("changedblocks-1", "r+b") as f:
        f.seek(256)
        f.write(bytearray([0 for i in range(128)]))
    """

    errors = compare_bitmaps("changedblocks-0", "changedblocks-1")

    run("readblocks",
        "--disk-id", "volume-0",
        "--start-index", "0",
        "--blocks-count", str(BLOCKS_COUNT),
        "--output", "readblocks-0")

    run("readblocks",
        "--disk-id", "volume-1",
        "--start-index", "0",
        "--blocks-count", str(BLOCKS_COUNT),
        "--output", "readblocks-1")

    if not files_equal("readblocks-0", "readblocks-1"):
        errors.append("volume data not equal")

    do_test_restore(
        run,
        "volume-0",
        "volume-1",
        BLOCKS_COUNT,
        0,
        "readblocks-0")

    tear_down(env)


def test_backup_and_restore_nrd():
    env, run = setup(with_nrd=True)

    run("createvolume",
        "--disk-id", "volume-0",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated")

    random_writes(run, NRD_BLOCKS_COUNT)

    run("backupvolume",
        "--disk-id", "volume-0",
        "--backup-disk-id", "volume-0.bkp",
        "--storage-media-kind", "hdd",
        "--io-depth", "16",
        "--verbose")

    run("readblocks",
        "--disk-id", "volume-0",
        "--start-index", "0",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--output", "readblocks-0")

    run("readblocks",
        "--disk-id", "volume-0.bkp",
        "--start-index", "0",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--output", "readblocks-1")

    assert files_equal("readblocks-0", "readblocks-1")

    do_test_restore(
        run,
        "volume-0",
        "volume-0.bkp",
        NRD_BLOCKS_COUNT,
        0,
        "readblocks-0")

    tear_down(env)


def test_destroy_volume():
    env, run = setup()

    run("createvolume",
        "--disk-id", "vol0",
        "--blocks-count", str(BLOCKS_COUNT))

    run("createvolume",
        "--disk-id", "vol1",
        "--blocks-count", str(BLOCKS_COUNT))

    run("destroyvolume",
        "--disk-id", "vol0",
        input=None,
        code=1)

    run("listvolumes")

    run("destroyvolume",
        "--disk-id", "vol0",
        input="xxx",
        code=1)

    run("listvolumes")

    run("destroyvolume",
        "--disk-id", "vol0",
        input="vol0",
        code=0)

    run("listvolumes")

    ret = common.canonical_file(env.results_path, local=True)
    tear_down(env)
    return ret


def test_createplacementgroup():
    env, run = setup(with_nrd=True)

    # create PLACEMENT_STRATEGY_SPREAD by default
    run("createplacementgroup",
        "--group-id", "group-0")
    assert file_equal(env.results_path, 'OK\n')

    response = list_placement_groups(env, run)
    assert len(response.GroupIds) == 1
    assert 'group-0' in response.GroupIds

    # fail if --partition-count is not specified
    run("createplacementgroup",
        "--group-id", "group-1",
        "--placement-strategy", "partition",
        code=1)

    # create PLACEMENT_STRATEGY_PARTITION with 4 partition with generated ids
    run("createplacementgroup",
        "--group-id", "group-1",
        "--placement-strategy", "partition",
        "--partition-count", "4")

    response = list_placement_groups(env, run)
    assert len(response.GroupIds) == 2
    assert 'group-1' in response.GroupIds

    response = describe_placement_group(env, run, 'group-1')
    assert response.Group.PlacementStrategy == EPlacementStrategy.PLACEMENT_STRATEGY_PARTITION
    assert response.Group.PlacementPartitionCount == 4

    tear_down(env)


def test_createvolume_in_partition_placementgroup():
    env, run = setup(with_nrd=True, nrd_device_count=2)

    run("createplacementgroup",
        "--group-id", "group-0",
        "--placement-strategy", "partition",
        "--partition-count", "4")
    assert file_equal(env.results_path, 'OK\n')
    clear_file(env.results_file)

    # Failed to create volume without specifying a placement partition index
    run("createvolume",
        "--disk-id", "vol0",
        "--placement-group-id", "group-0",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated",
        code=1)

    # Failed to create volume with partition index out of range
    run("createvolume",
        "--disk-id", "vol1",
        "--placement-group-id", "group-0",
        "--placement-partition-index", "10",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated",
        code=1)

    # Successfully creating a first volume
    clear_file(env.results_file)
    run("createvolume",
        "--disk-id", "vol2",
        "--placement-group-id", "group-0",
        "--placement-partition-index", "1",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated")
    assert file_equal(env.results_path, 'OK\n')

    # Check that the created volume is located in the partition
    volume_info = describe_volume(env, run, 'vol2')
    assert volume_info.Volume.PlacementPartitionIndex == 1

    # Failed creating a second volume in other partition
    clear_file(env.results_file)
    run("createvolume",
        "--disk-id", "vol3",
        "--placement-group-id", "group-0",
        "--placement-partition-index", "2",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated",
        code=1)

    # Successfully creating a second volume in same partition
    clear_file(env.results_file)
    run("createvolume",
        "--disk-id", "vol4",
        "--placement-group-id", "group-0",
        "--placement-partition-index", "1",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated")
    assert file_equal(env.results_path, 'OK\n')

    # Check that the created volume is located in the partition
    volume_info = describe_volume(env, run, 'vol4')
    assert volume_info.Volume.PlacementPartitionIndex == 1

    tear_down(env)


def test_alterplacementgroupmembership_in_partition_placementgroup():
    env, run = setup(with_nrd=True, nrd_device_count=2, rack='')

    run("createplacementgroup",
        "--group-id", "group-0",
        "--placement-strategy", "partition",
        "--partition-count", "5")

    run("createplacementgroup",
        "--group-id", "group-1",
        "--placement-strategy", "partition",
        "--partition-count", "5")

    run("createvolume",
        "--disk-id", "vol1",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated")

    run("createvolume",
        "--disk-id", "vol2",
        "--blocks-count", str(NRD_BLOCKS_COUNT),
        "--storage-media-kind", "nonreplicated")

    # Failed to alter volume without partition
    run("alterplacementgroupmembership",
        "--config-version", "1",
        "--disk-to-add", "vol1",
        "--group-id", "group-0",
        code=1)

    # Failed to alter volume with placement partition index out of range
    run("alterplacementgroupmembership",
        "--config-version", "1",
        "--disk-to-add", "vol1",
        "--group-id", "group-0",
        "--placement-partition-index", "10",
        code=1)

    # Successfully add vol1 to group-0 partition-1
    run("alterplacementgroupmembership",
        "--config-version", "1",
        "--disk-to-add", "vol1",
        "--group-id", "group-0",
        "--placement-partition-index", "1")
    # Check that the altered volume is located in the expected partition
    volume_info = describe_volume(env, run, 'vol1')
    assert volume_info.Volume.PlacementGroupId == 'group-0'
    assert volume_info.Volume.PlacementPartitionIndex == 1

    # Can't change group.
    run("alterplacementgroupmembership",
        "--config-version", "2",
        "--disk-to-add", "vol1",
        "--group-id", "group-1",
        "--placement-partition-index", "3",
        code=1)

    # Can't change partition.
    run("alterplacementgroupmembership",
        "--config-version", "2",
        "--disk-to-add", "vol1",
        "--group-id", "group-0",
        "--placement-partition-index", "4",
        code=1)

    # Successfully remove from group.
    run("alterplacementgroupmembership",
        "--config-version", "2",
        "--group-id", "group-0",
        "--disk-to-remove", "vol1")
    volume_info = describe_volume(env, run, 'vol1')
    assert volume_info.Volume.PlacementGroupId == ''
    assert volume_info.Volume.PlacementPartitionIndex == 0

    # Successfully add vol1 to group-1 partition-3.
    run("alterplacementgroupmembership",
        "--config-version", "1",
        "--disk-to-add", "vol1",
        "--group-id", "group-1",
        "--placement-partition-index", "3")
    volume_info = describe_volume(env, run, 'vol1')
    assert volume_info.Volume.PlacementGroupId == 'group-1'
    assert volume_info.Volume.PlacementPartitionIndex == 3

    # Successfully add vol2 to same partition.
    run("alterplacementgroupmembership",
        "--config-version", "2",
        "--disk-to-add", "vol2",
        "--group-id", "group-1",
        "--placement-partition-index", "3")
    volume_info = describe_volume(env, run, 'vol2')
    assert volume_info.Volume.PlacementGroupId == 'group-1'
    assert volume_info.Volume.PlacementPartitionIndex == 3

    # Successfully remove vol2 from group.
    run("alterplacementgroupmembership",
        "--config-version", "3",
        "--group-id", "group-1",
        "--disk-to-remove", "vol2")
    volume_info = describe_volume(env, run, 'vol2')
    assert volume_info.Volume.PlacementGroupId == ''
    assert volume_info.Volume.PlacementPartitionIndex == 0

    # Failed ro add vol2 to another partition same group since all devices in same rack
    run("alterplacementgroupmembership",
        "--config-version", "4",
        "--disk-to-add", "vol2",
        "--group-id", "group-1",
        "--placement-partition-index", "5",
        code=1)
    volume_info = describe_volume(env, run, 'vol2')
    assert volume_info.Volume.PlacementGroupId == ''
    assert volume_info.Volume.PlacementPartitionIndex == 0

    tear_down(env)


def test_create_destroy_placementgroup():
    env, run = setup(with_nrd=True, nrd_device_count=2, rack='')

    run("createplacementgroup",
        "--group-id", "group-0",
        "--placement-strategy", "partition",
        "--partition-count", "5")

    run("createplacementgroup",
        "--group-id", "group-1",
        "--placement-strategy", "spread")

    run("destroyplacementgroup",
        "--group-id", "group-1")

    run("destroyplacementgroup",
        "--group-id", "group-0")


def test_disabled_configs_dispatcher():
    storage = TStorageServiceConfig()
    storage.ConfigsDispatcherServiceEnabled = False
    env, run = setup(storage_config_patches=[storage])

    static_nodes = get_static_nodes(env, run)
    assert len(static_nodes) == 1

    send_two_node_nameservice_config(env)

    updated_static_nodes = get_static_nodes(env, run)
    assert updated_static_nodes == static_nodes

    with open(env.results_path, "w") as results:
        results.write(json.dumps(static_nodes) + "\n")
        results.write(json.dumps(updated_static_nodes) + "\n")

    ret = common.canonical_file(env.results_path, local=True)
    tear_down(env)
    return ret


def test_enabled_configs_dispatcher():
    storage = TStorageServiceConfig()
    storage.ConfigsDispatcherServiceEnabled = True
    env, run = setup(storage_config_patches=[storage])

    static_nodes = get_static_nodes(env, run)
    assert len(static_nodes) == 1

    send_two_node_nameservice_config(env)

    updated_static_nodes = get_static_nodes(env, run)
    assert len(updated_static_nodes) == 2
    assert updated_static_nodes[0]["NodeId"] == 1
    assert updated_static_nodes[1]["NodeId"] == 2

    with open(env.results_path, "w") as results:
        results.write(json.dumps(static_nodes) + "\n")
        results.write(json.dumps(updated_static_nodes) + "\n")

    ret = common.canonical_file(env.results_path, local=True)
    tear_down(env)
    return ret


# it's a smoke test right now - it doesn't test nbd proxying logic
def test_endpoint_proxy():
    env, run = setup()

    endpoint_proxy_path = common.binary_path(
        "cloud/blockstore/apps/endpoint_proxy/blockstore-endpoint-proxy")

    port_manager = network.PortManager()
    port = port_manager.get_port()

    run_async(
        [
            endpoint_proxy_path, "--server-port", str(port),
        ],
        "endpoint-proxy-%s.out" % port,
        "endpoint-proxy-%s.err" % port
    )

    run("startproxyendpoint",
        "--endpoint-proxy-host", "localhost",
        "--endpoint-proxy-port", str(port),
        "--socket", "TODO-nbs-socket",
        # can't use modprobe nbd in tests
        # "--nbd-device", "TODO-nbd-device",
        "--block-size", "4096",
        "--blocks-count", "1024")

    run("listproxyendpoints",
        "--endpoint-proxy-host", "localhost",
        "--endpoint-proxy-port", str(port))

    run("stopproxyendpoint",
        "--endpoint-proxy-host", "localhost",
        "--endpoint-proxy-port", str(port),
        "--socket", "TODO-nbs-socket")

    ret = common.canonical_file(env.results_path, local=True)
    tear_down(env)
    return ret


# it's a smoke test right now - it doesn't test nbd proxying logic
def test_endpoint_proxy_uds():
    env, run = setup()

    endpoint_proxy_path = common.binary_path(
        "cloud/blockstore/apps/endpoint_proxy/blockstore-endpoint-proxy")

    ep_sock = "ep-%s.sock" % hash(common.context.test_name)

    run_async(
        [
            endpoint_proxy_path, "--unix-socket-path", ep_sock,
        ],
        "endpoint-proxy.out",
        "endpoint-proxy.err",
    )

    run("startproxyendpoint",
        "--endpoint-proxy-unix-socket-path", ep_sock,
        "--socket", "TODO-nbs-socket",
        # can't use modprobe nbd in tests
        # "--nbd-device", "TODO-nbd-device",
        "--block-size", "4096",
        "--blocks-count", "1024")

    run("listproxyendpoints",
        "--endpoint-proxy-unix-socket-path", ep_sock)

    run("stopproxyendpoint",
        "--endpoint-proxy-unix-socket-path", ep_sock,
        "--socket", "TODO-nbs-socket")

    ret = common.canonical_file(env.results_path, local=True)
    tear_down(env)
    return ret


def test_nbs_with_endpoint_proxy_uds():
    ep_sock = "ep-%s.sock" % hash(common.context.test_name)
    vol_sock = "vol-%s.sock" % hash(common.context.test_name)

    endpoints_dir = os.path.join(
        common.output_path(),
        "endpoints-%s" % hash(common.context.test_name))

    server_config = TServerConfig()
    server_config.EndpointProxySocketPath = ep_sock
    server_config.NbdEnabled = True
    server_config.EndpointStorageType = EEndpointStorageType.ENDPOINT_STORAGE_FILE
    server_config.EndpointStorageDir = endpoints_dir
    env, run = setup(server_config_patch=server_config)

    endpoint_proxy_path = common.binary_path(
        "cloud/blockstore/apps/endpoint_proxy/blockstore-endpoint-proxy")

    run_async(
        [
            endpoint_proxy_path, "--unix-socket-path", ep_sock,
        ],
        "endpoint-proxy.out",
        "endpoint-proxy.err",
        # we need to use the same cwd as nbs to be able to use relative unix
        # socket paths because abs paths are longer than the maximum unix
        # socket path length which is 107
        cwd=env.nbs_cwd,
    )

    run("createvolume",
        "--disk-id", "vol0",
        "--blocks-count", str(BLOCKS_COUNT))

    # TODO: uncomment after we find a way to use nbd in github ci
    # TODO: write a proper test scenario including multiple reads and writes,
    # data validation, nbs restarts, etc
    """
    nbd_device = "/dev/nbd10"

    run("startendpoint",
        "--socket", vol_sock,
        "--disk-id", "vol0",
        "--ipc-type", "nbd",
        "--nbd-device", nbd_device,
        "--persistent",
        "--mount-mode", "local")

    ifpath = os.path.join(common.output_path(), "iblock.txt")
    ofpath = os.path.join(common.output_path(), "oblock.txt")
    f = open(ifpath, "w")
    f.write("a" * 4096)
    f.close()

    time.sleep(5)

    subprocess.call(("dd oflag=direct of=%s if=%s bs=4k count=1" % (nbd_device, ifpath)).split(" "))
    subprocess.call(("dd iflag=direct if=%s of=%s bs=4k count=1" % (nbd_device, ofpath)).split(" "))
    """

    run("listproxyendpoints",
        "--endpoint-proxy-unix-socket-path", ep_sock, cwd=env.nbs_cwd)

    run("stopendpoint", "--socket", vol_sock)

    ret = common.canonical_file(env.results_path, local=True)
    tear_down(env)
    return ret
