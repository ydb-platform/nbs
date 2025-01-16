import json
import logging
import os
import subprocess
import time
import tempfile
import uuid

import pytest
import requests
import yatest.common as yatest_common
from cloud.blockstore.config.server_pb2 import TServerConfig, \
    TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.nonreplicated_setup import \
    setup_nonreplicated, create_file_devices, \
    setup_disk_registry_config_simple, enable_writable_state
from cloud.blockstore.tests.python.lib.test_base import \
    thread_count, wait_for_nbs_server, wait_for_secure_erase
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

DEFAULT_BLOCK_SIZE = 4096
DEFAULT_DEVICE_COUNT = 4
DEFAULT_BLOCK_COUNT_PER_DEVICE = 262144
DISK_SIZE = 1048576


def prepare_partitions(nbs, disk_id, tablet_version=1):
    nbs.create_volume(disk_id, tablet_version=tablet_version)
    nbs.stat_volume(disk_id)
    out = nbs.describe_volume(disk_id)

    volume_config = out.decode("utf-8")
    volume_config = json.loads(volume_config)

    logging.info(
        "partition tablet %s" % volume_config["Partitions"][0]["TabletId"])
    logging.info(
        "volume tablet %s" % volume_config["VolumeTabletId"])

    return \
        volume_config["VolumeTabletId"], \
        volume_config["Partitions"][0]["TabletId"]


def prepare_volume(nbs, disk_id):
    nbs.create_volume(disk_id)
    out = nbs.describe_volume(disk_id)

    volume_config = out.decode("utf-8")
    volume_config = json.loads(volume_config)

    logging.info(
        "partition tablet %s" % volume_config["Partitions"][0]["TabletId"])
    logging.info(
        "volume tablet %s" % volume_config["VolumeTabletId"])

    return \
        volume_config["VolumeTabletId"], \
        volume_config["Partitions"][0]["TabletId"]


def check_get(session, url, params, strings, check_redirect=False):
    full_url = url + '&'.join(["{}={}".format(k, v) for k, v in params.items()])
    r = session.get(full_url)
    html = r.text

    assert r.status_code == 200

    if strings is None:
        return

    if isinstance(strings, str):
        strings = [strings]
    assert isinstance(strings, list), "strings should be str or dict of str"

    for s in strings:
        assert html.find(s) != -1

    if check_redirect:
        assert html.find("window.location.href") != -1
    else:
        assert html.find("window.location.href") == -1


def check_get_redirect(session, url, params, strings):
    return check_get(session, url, params, strings, True)


def check_post(session, url, params, strings, check_redirect=False):
    r = session.post(url, data=params)
    html = r.text

    assert r.status_code == 200

    if strings is None:
        return

    if isinstance(strings, str):
        strings = [strings]
    assert isinstance(strings, list), "strings should be str or dict of str"

    for s in strings:
        assert html.find(s) != -1

    if check_redirect:
        assert html.find("window.location.href") != -1
    else:
        assert html.find("window.location.href") == -1


def check_post_redirect(session, url, params, strings):
    return check_post(session, url, params, strings, True)


def check_tablet_get(session, url, id, params, strings):
    params = {**params, **{"TabletID": id}}
    return check_get(session, url, params, strings)


def check_tablet_post(session, url, id, params, strings):
    params = {**params, **{"TabletID": id}}
    return check_post(session, url, params, strings)


def check_tablet_get_redirect(session, url, id, params, strings):
    params = {**params, **{"TabletID": id}}
    return check_get_redirect(session, url, params, strings)


def check_tablet_post_redirect(session, url, id, params, strings):
    params = {**params, **{"TabletID": id}}
    return check_post_redirect(session, url, params, strings)


class TestKikimrPartitionTablet:

    def __init__(self, name, disk_id, tablet_version=1):
        self.name = name
        self.tablet_version = tablet_version
        self.disk_id = disk_id
        self.use_nrd = False
        self.session = requests.Session()
        self.session.mount(
            'http://',
            HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))

    def check_fullcompaction(self):
        params = {"action": "compactAll"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Nothing to compact</h2>")
        self.nbs.write_blocks(self.disk_id, 0)
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Compaction has been started</h2>")

    def check_rangecompaction(self):
        params = {
            "action": "compactAll",
            "BlockIndex": "1024",
            "BlockCount": "1024"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Wrong HTTP method</h2>")
        self.nbs.write_blocks(self.disk_id, 1024)
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Compaction has been started</h2>")

    def check_rebuildmetadata(self):
        params = {"action": "rebuildMetadata"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Batch size is 0</h2>")

        params = {
            "action": "rebuildMetadata",
            "BatchSize": "10"
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.partition_id, params,
            "<h2>Metadata rebuild(used blocks) has been started</h2>")

    def check_describe(self):
        params = {
            "action": "describe",
            "range": "0:1023"
        }
        check_tablet_get(
            self.session,
            self.base_url,
            self.partition_id,
            params,
            ["# Block", "BlobId", "Offset"])

    def check_collectgarbage(self):
        params = {"action": "collectGarbage"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.partition_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.partition_id, params,
            "<h2>Operation successfully completed</h2>")

    def check_mainpage(self):
        check_tablet_get(
            self.session, self.base_url,
            self.partition_id, {},
            ["Overview", "Tables", "Channels", "Index"])

    def run(self, nbs, nbs_http_port):
        self.nbs = nbs
        self.volume_id, self.partition_id = prepare_partitions(
            nbs,
            self.disk_id,
            self.tablet_version)

        self.base_url = 'http://localhost:%s/tablets/app?' % nbs_http_port

        self.check_mainpage()
        self.check_fullcompaction()
        time.sleep(20)
        self.check_rangecompaction()
        self.check_describe()
        self.check_collectgarbage()
        if self.tablet_version != 2:
            self.check_rebuildmetadata()

        return True


class TestKikimrVolumeTablet:

    def __init__(self, name, disk_id, use_nrd):
        self.name = name
        self.disk_id = disk_id
        self.use_nrd = use_nrd
        self.session = requests.Session()
        self.session.mount(
            'http://',
            HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))

    def check_startpartitions(self):
        params = {"action": "startpartitions"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Start initiated</h2>")

    def check_createcheckpoint(self):
        bad_params = {"action": "createCheckpoint"}
        params = {"action": "createCheckpoint", "checkpointid": "cp1"}

        check_tablet_get_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Operation successfully completed</h2>")

        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, bad_params, "<h2>No checkpoint id is given</h2>")

    def check_deletecheckpoint(self):
        bad_params = {"action": "deleteCheckpoint"}
        params = {"action": "deleteCheckpoint", "checkpointid": "cp1"}

        check_tablet_get_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Operation successfully completed</h2>")

        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, bad_params, "<h2>No checkpoint id is given</h2>")

    def check_resetmountseqnumber(self):
        params = {"action": "resetmountseqnumber"}

        check_tablet_get_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>No client id is given</h2>")

        params = {"action": "resetmountseqnumber", "ClientId": "client"}
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Operation successfully completed</h2>")

    def check_removeclient(self):
        params = {"action": "removeclient"}

        check_tablet_get_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>No client id is given</h2>")

        params = {"action": "resetmountseqnumber", "ClientId": "client"}
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.volume_id, params, "<h2>Operation successfully completed</h2>")

    def check_mainpage(self):
        check_tablet_get(
            self.session, self.base_url,
            self.volume_id, {},
            ["Overview", "History", "Checkpoints", "Traces", "StorageConfig"])

    def run(self, nbs, nbs_http_port):
        self.volume_id, self.partition_id = prepare_volume(nbs, self.disk_id)
        self.base_url = 'http://localhost:%s/tablets/app?' % nbs_http_port

        self.check_mainpage()
        self.check_startpartitions()
        self.check_createcheckpoint()
        self.check_deletecheckpoint()

        socket_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())

        nbs.mount_volume(self.disk_id, "client", socket_path)

        try:
            self.check_resetmountseqnumber()
            self.check_removeclient()
        finally:
            nbs.unmount_volume(socket_path)

        return True


class TestDiskRegistryTablet:

    def __init__(self, name, disk_id, use_nrd):
        self.name = name
        self.disk_id = disk_id
        self.use_nrd = use_nrd
        self.session = requests.Session()
        self.session.mount(
            'http://',
            HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))

    def check_mainpage(self):
        check_tablet_get(
            self.session, self.base_url,
            self.dr_id, {}, ["Current state", "Disks"])

    def check_reallocate(self):
        params = {"action": "volumeRealloc"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No disk id is given</h2>")

        params = {
            "action": "volumeRealloc",
            "DiskID": self.disk_id
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Operation successfully completed</h2>")

    def check_replacedevice(self):
        params = {"action": "replaceDevice"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No disk id is given</h2>")

        params = {
            "action": "replaceDevice",
            "DiskID": self.disk_id
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No device id is given</h2>")

        params = {
            "action": "replaceDevice",
            "DiskID": self.disk_id,
            "DeviceUUID": "FileDevice-1"
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Replace device is not allowed</h2>")

    def check_change_device_state(self):
        params = {"action": "changeDeviceState"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No new state is given</h2>")

        params = {
            "action": "changeDeviceState",
            "NewState" : "DEVICE_STATE_ONLINE",
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No device id is given</h2>")

        params = {
            "action": "changeDeviceState",
            "NewState" : "not a state",
            "DeviceUUID": "FileDevice-1"
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Invalid new state</h2>")

        params = {
            "action": "changeDeviceState",
            "NewState" : "DEVICE_STATE_WARNING",
            "DeviceUUID": "FileDevice-1"
        }

        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Operation successfully completed</h2>")

        state = self.client.backup_disk_registry_state()

        for agent in state["Backup"]["Agents"]:
            for device in agent["Devices"]:
                if device["DeviceUUID"] == "FileDevice-1":
                    assert device["State"] == "DEVICE_STATE_WARNING"

    def check_change_agent_state(self):
        params = {"action": "changeAgentState"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Wrong HTTP method</h2>")
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No new state is given</h2>")

        params = {
            "action": "changeAgentState",
            "NewState" : "AGENT_STATE_ONLINE",
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No agent id is given</h2>")

        params = {
            "action": "changeAgentState",
            "NewState" : "not a state",
            "AgentID": "localhost"
        }
        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Invalid new state</h2>")

        params = {
            "action": "changeAgentState",
            "NewState" : "AGENT_STATE_WARNING",
            "AgentID": "localhost"
        }

        check_tablet_post_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>Operation successfully completed</h2>")

        state = self.client.backup_disk_registry_state()

        for agent in state["Backup"]["Agents"]:
            if agent["AgentId"] == "localhost":
                assert agent["State"] == "AGENT_STATE_WARNING"

        self.client.change_agent_state("localhost", "0")

    def check_showdisk(self):
        params = {"action": "disk"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No disk id is given</h2>")

        params = {
            "action": "disk",
            "DiskID": self.disk_id
        }

        check_tablet_get(
            self.session, self.base_url,
            self.dr_id, params, ["Disk", "Folder Id", "Cloud Id"])

    def check_showdevice(self):
        params = {"action": "dev"}
        check_tablet_get_redirect(
            self.session, self.base_url,
            self.dr_id, params, "<h2>No device id is given</h2>")

        params = {
            "action": "dev",
            "DeviceUUID": "FileDevice-1"
        }

        check_tablet_get(
            self.session, self.base_url,
            self.dr_id, params, ["Device", "Name:", "Block size:"])

    def run(self, nbs, nbs_http_port):
        self.volume_id = nbs.create_nrd_volume(self.disk_id)
        self.base_url = \
            'http://localhost:%s/tablets/app?' % nbs_http_port

        self.dr_id = nbs.get_dr_tablet_id()
        logging.info("disk registry tablet %s" % self.dr_id)

        self.client = NbsClient(nbs.nbs_port)

        self.check_mainpage()
        self.check_replacedevice()
        self.check_change_device_state()
        self.check_change_agent_state()
        self.check_reallocate()
        self.check_showdisk()
        self.check_showdevice()

        return True


class TestService:

    def __init__(self, name, disk_id, use_nrd):
        self.name = name
        self.disk_id = disk_id
        self.use_nrd = use_nrd
        self.session = requests.Session()
        self.session.mount(
            'http://',
            HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))

    def check_listclients(self):
        params = {"action": "listclients"}
        check_get_redirect(
            self.session, self.base_url,
            params, "<h2>No Volume is given</h2>")

        params = {
            "action": "listclients",
            "Volume": "xyz"
        }
        check_get_redirect(self.session, self.base_url, params, "not found")

        params = {
            "action": "listclients",
            "Volume": self.disk_id
        }
        check_get(
            self.session, self.base_url, params,
            ["Client ID", "Access mode", "Mount mode", "some-client"])

    def check_search(self):
        params = {"action": "search"}
        check_get_redirect(
            self.session, self.base_url, params, "<h2>No Volume is given</h2>")

        params = {
            "action": "search",
            "Volume": "xyz"
        }
        check_get(
            self.session, self.base_url,
            params, "Could not resolve path")

        params = {
            "action": "search",
            "Volume": self.disk_id
        }
        check_get(
            self.session, self.base_url,
            params, ["Volume", "Tablet ID", "Clients"])

    def check_unmount(self):
        params = {"action": "unmount"}
        check_get_redirect(
            self.session, self.base_url,
            params, "<h2>Wrong HTTP method</h2>")

        params = {
            "action": "unmount",
            "Volume": self.disk_id
        }
        check_post_redirect(
            self.session,
            self.base_url,
            params,
            "<h2>No client id is given</h2>")

        params = {
            "action": "unmount",
            "ClientId": "some-client"
        }
        check_post_redirect(
            self.session, self.base_url, params, "<h2>No Volume is given</h2>")

        params = {
            "action": "unmount",
            "Volume": self.disk_id,
            "ClientId": "some-client"
        }
        check_post_redirect(
            self.session, self.base_url,
            params, "<h2>Operation successfully completed</h2>")

    def check_preemption(self):
        params = {"action": "togglePreemption"}
        check_get_redirect(
            self.session, self.base_url, params, "<h2>Wrong HTTP method</h2>")
        check_post_redirect(
            self.session, self.base_url, params, "<h2>No Volume is given</h2>")

        params = {
            "action": "togglePreemption",
            "Volume": self.disk_id
        }
        check_post_redirect(
            self.session, self.base_url, params, "No action type is given")

        params = {
            "action": "togglePreemption",
            "Volume": self.disk_id,
            "type": "push"
        }
        check_post_redirect(
            self.session, self.base_url,
            params, "<h2>Operation successfully completed</h2>")

    def check_mainpage(self):
        check_get(
            self.session, self.base_url,
            {}, ["Service", "Volumes", "Config"])

    def run(self, nbs, nbs_http_port):
        self.volume_id, self.partition_id = prepare_volume(nbs, self.disk_id)
        self.base_url = \
            'http://localhost:%s/blockstore/service?' % nbs_http_port

        self.check_search()

        socket_path = tempfile.gettempdir() + '/' + str(uuid.uuid4())

        nbs.mount_volume(self.disk_id, "some-client", socket_path)

        try:
            self.check_mainpage()
            self.check_listclients()
            self.check_preemption()
            self.check_unmount()
        finally:
            nbs.unmount_volume(socket_path)

        return True


TESTS = [
    TestService(
        "TestService", disk_id="vol0", use_nrd=False),
    TestKikimrPartitionTablet(
        name="TestKikimrPartition1Tablet", disk_id="vol1", tablet_version=1),
    TestKikimrPartitionTablet(
        name="TestKikimrPartition2Tablet", disk_id="vol2", tablet_version=2),
    TestKikimrVolumeTablet(
        "TestKikimrVolumeTablet", disk_id="vol0", use_nrd=False),
    TestDiskRegistryTablet(
        "TestDiskRegistryTablet", disk_id="vol3", use_nrd=True)
]


class Nbs(LocalNbs):
    BINARY_PATH = yatest_common.binary_path(
        "cloud/blockstore/apps/client/blockstore-client")

    def create_volume(self, disk_id, tablet_version=1):
        logging.info('[NBS] create volume "{disk_id}"')

        self.__rpc(
            "createvolume",
            "--disk-id", disk_id,
            "--blocks-count", str(2048),
            "--storage-media-kind", "ssd",
            "--tablet-version", str(tablet_version))

    def create_nrd_volume(self, disk_id):
        logging.info('[NBS] create NRD volume "{disk_id}"')

        self.__rpc(
            "createvolume",
            "--disk-id", disk_id,
            "--blocks-count", str(DEFAULT_BLOCK_COUNT_PER_DEVICE),
            "--storage-media-kind", "ssd_nonrepl")

    def stat_volume(self, disk_id):
        logging.info('[NBS] stat volume "{disk_id}"')

        self.__rpc(
            "statvolume",
            "--disk-id", disk_id)

    def describe_volume(self, disk_id):
        logging.info(f'[NBS] describe volume "{disk_id}"')

        out, _ = self.__rpc(
            "executeaction",
            "--action", "describevolume",
            "--input-bytes", '{"DiskId":"%s"}' % disk_id)

        return out

    def mount_volume(self, disk_id, client_id, socket):
        logging.info(f'[NBS] mount volume "{disk_id}" for socket "{socket}"')

        out, err = self.__rpc(
            "startendpoint",
            "--disk-id", disk_id,
            "--client-id", client_id,
            "--access-mode", "rw",
            "--mount-mode", "local",
            "--ipc-type", "grpc",
            "--socket", socket)

        return out

    def unmount_volume(self, socket):
        logging.info(f'[NBS] unmount volume "{socket}"')

        out, _ = self.__rpc(
            "stopendpoint",
            "--socket", socket)

        return out

    def write_blocks(self, disk_id, pos):
        logging.info(f'[NBS] write data for volume "{disk_id}"')

        with tempfile.NamedTemporaryFile() as f:
            f.write(bytearray(1024 * 4096))

            out, _ = self.__rpc(
                "writeblocks",
                "--disk-id", disk_id,
                "--start-index", str(pos),
                "--input", f.name)

        return out

    def get_dr_tablet_id(self):
        logging.info('[NBS] get dr tablet id')

        out, _ = self.__rpc(
            "executeaction",
            "--action", "getdiskregistrytabletinfo")

        out = out.decode("utf-8")
        response = json.loads(out)
        return response["TabletId"]

    def __rpc(self, *args, **kwargs):
        args = [self.BINARY_PATH] + list(args) + [
            "--host", "localhost",
            "--port", str(self.nbs_port),
            "--verbose", "error"]

        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        outs, errs = process.communicate()

        if outs:
            logging.info(f'blockstore-client output: {outs}')

        if errs:
            logging.error(f'blockstore-client errors: {errs}')

        assert process.returncode == 0

        return outs, errs


def __run_test(test_case):
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    nbs_binary_path = yatest_common.binary_path(
        "cloud/blockstore/apps/server/nbsd")

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()

    if test_case.use_nrd:
        devices = create_file_devices(
            None,  # dir
            DEFAULT_DEVICE_COUNT,
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_COUNT_PER_DEVICE)

        setup_nonreplicated(kikimr_cluster.client, [devices])

    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())
    server_app_config.ServerConfig.NodeType = 'nbs_control'

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')

    server_app_config.ServerConfig.RootCertsFile = os.path.join(
        certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.SchemeShardDir = "/Root/nbs"
    if test_case.use_nrd:
        storage.AllocationUnitNonReplicatedSSD = 1
        storage.AcquireNonReplicatedDevices = True
        storage.ClientRemountPeriod = 1000
        storage.NonReplicatedInfraTimeout = 60000
        storage.NonReplicatedAgentMinTimeout = 3000
        storage.NonReplicatedAgentMaxTimeout = 3000
        storage.NonReplicatedDiskRecyclingPeriod = 5000

    storage.EnableToChangeStatesFromDiskRegistryMonpage = True
    storage.EnableToChangeErrorStatesFromDiskRegistryMonpage = True

    nbs = Nbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=kikimr_binary_path,
        nbs_binary_path=nbs_binary_path)

    nbs.start()

    wait_for_nbs_server(nbs.nbs_port)

    if test_case.use_nrd:
        nbs_client_binary_path = \
            yatest_common.binary_path("cloud/blockstore/apps/client/blockstore-client")
        enable_writable_state(nbs.nbs_port, nbs_client_binary_path)
        setup_disk_registry_config_simple(
            devices,
            nbs.nbs_port,
            nbs_client_binary_path)

        # node with DiskAgent

        server_app_config.ServerConfig.NodeType = 'disk-agent'
        storage.DisableLocalService = True

        disk_agent = Nbs(
            kikimr_port,
            configurator.domains_txt,
            server_app_config=server_app_config,
            storage_config_patches=[storage],
            enable_tls=True,
            kikimr_binary_path=kikimr_binary_path,
            nbs_binary_path=nbs_binary_path)

        disk_agent.start()
        wait_for_nbs_server(disk_agent.nbs_port)

        # wait for DiskAgent registration & secure erase
        wait_for_secure_erase(nbs.mon_port)

    try:
        ret = test_case.run(nbs, nbs.mon_port)
    finally:
        nbs.stop()
        kikimr_cluster.stop()
        if test_case.use_nrd:
            disk_agent.stop()
    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
def test_monitoring(test_case):
    assert __run_test(test_case) is True
