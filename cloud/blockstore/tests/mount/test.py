import json
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid

import pytest
import requests
import yatest.common as yatest_common
from cloud.blockstore.config.server_pb2 import TServerConfig, \
    TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import \
    thread_count, wait_for_nbs_server
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class MountContext:

    def __init__(self, nbs, socket):
        self.nbs = nbs
        self.socket = socket

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.nbs.unmount_volume(self.socket)


class CommonPaths:

    def __init__(self):
        self.kikimr_binary_path = \
            yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")
        self.nbs_binary_path = \
            yatest_common.binary_path(
                "cloud/blockstore/apps/server/nbsd")
        self.certs_dir = \
            yatest_common.source_path('cloud/blockstore/tests/certs')
        self.nbs_client_path = \
            yatest_common.binary_path(
                "cloud/blockstore/apps/client/blockstore-client")


class TestMaxLocalVolumes:

    def __init__(self, name):
        self.name = name

    def get_node_id(self, session, http_port, id):
        template = "http://localhost:{}/tablets?TabletID={}"
        url = template.format(http_port, id)

        response = session.get(url)
        html = response.text
        assert response.status_code == 200

        m = re.search(r'NodeID: (\d+)', html)
        node_id = m.group(1)
        logging.info("Node id %s" % node_id)
        return node_id

    def run(self, nbs, nbs_http_port, nbs_control_http_port):
        s = requests.Session()
        s.mount(
            'http://',
            HTTPAdapter(max_retries=Retry(total=10, backoff_factor=0.5)))

        local_tablet_id, _ = nbs.create_volume("local")
        remote_tablet_id, _ = nbs.create_volume("remote")

        local_sk = tempfile.gettempdir() + '/' + str(uuid.uuid4())
        remote_sk = tempfile.gettempdir() + '/' + str(uuid.uuid4())

        old = None
        with nbs.mount_volume("local", "local", local_sk):
            with nbs.mount_volume("remote", "local", remote_sk):
                assert \
                    self.get_node_id(s, nbs_http_port, local_tablet_id) \
                    != self.get_node_id(s, nbs_http_port, remote_tablet_id)

                for _ in range(20):
                    template = "http://localhost:{}/tablets?TabletID={}"
                    url = template.format(nbs_http_port, remote_tablet_id)
                    response = s.get(url)
                    html = response.text

                    assert response.status_code == 200

                    m = re.search(r'Tablet generation: (\d+)', html)
                    new = m.group(1)
                    logging.info("Tablet generation %s" % new)

                    if old is not None and old != new:
                        assert False, \
                            "generation changed from {} to {}".format(old, new)

                    old = new

                    time.sleep(10)

        return True


TESTS = [TestMaxLocalVolumes("TestMaxLocalVolumes")]


class Nbs(LocalNbs):
    BINARY_PATH = yatest_common.binary_path(
        "cloud/blockstore/apps/client/blockstore-client")

    def create_volume(self, disk_id):
        logging.info('[NBS] create volume "{disk_id}"')

        out, _ = self.__rpc(
            "createvolume",
            "--disk-id", disk_id,
            "--blocks-count", str(2048),
            "--storage-media-kind", "ssd")

        assert out.decode("utf-8").strip() == "OK"

        self.stat_volume(disk_id)
        out = self.describe_volume(disk_id)

        volume_config = out.decode("utf-8")
        volume_config = json.loads(volume_config)

        logging.info(
            "partition tablet %s" % volume_config["Partitions"][0]["TabletId"])
        logging.info(
            "volume tablet %s" % volume_config["VolumeTabletId"])

        return \
            volume_config["VolumeTabletId"], \
            volume_config["Partitions"][0]["TabletId"]

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

        out, _ = self.__rpc(
            "startendpoint",
            "--disk-id", disk_id,
            "--client-id", client_id,
            "--access-mode", "rw",
            "--mount-mode", "local",
            "--ipc-type", "grpc",
            "--socket", socket)

        assert out.decode("utf-8").strip() == "OK"
        return MountContext(self, socket)

    def unmount_volume(self, socket):
        logging.info(f'[NBS] unmount volume "{socket}"')

        out, _ = self.__rpc(
            "stopendpoint",
            "--socket", socket)

        assert out.decode("utf-8").strip() == "OK"

        return out

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


def setup_kikimr(paths):
    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_paths=[paths.kikimr_binary_path],
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator)
    kikimr_cluster.start()
    return kikimr_cluster, configurator


def setup_nbs_control(paths, kikimr_cluster, configurator, use_scheme_cache):
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    server_app_config.ServerConfig.RootCertsFile = os.path.join(
        paths.certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(paths.certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(paths.certs_dir, 'server.key')

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.SchemeShardDir = "/Root/nbs"
    storage.NodeType = 'nbs_control'
    storage.UseSchemeCache = use_scheme_cache

    nbs = Nbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=paths.kikimr_binary_path,
        nbs_binary_path=paths.nbs_binary_path)

    nbs.start()

    wait_for_nbs_server(nbs.nbs_port)
    return nbs


def setup_nbs_hw(paths, kikimr_cluster, configurator, use_scheme_cache):
    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    server_app_config.ServerConfig.RootCertsFile = os.path.join(
        paths.certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(paths.certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(paths.certs_dir, 'server.key')

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage = TStorageServiceConfig()
    storage.DisableLocalService = True
    storage.SchemeShardDir = "/Root/nbs"
    storage.MaxLocalVolumes = 1
    storage.NodeType = 'nbs'
    storage.UseSchemeCache = use_scheme_cache

    nbs = Nbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        storage_config_patches=[storage],
        enable_tls=True,
        kikimr_binary_path=paths.kikimr_binary_path,
        nbs_binary_path=paths.nbs_binary_path)

    nbs.start()

    wait_for_nbs_server(nbs.nbs_port)
    return nbs


def __run_test(test_case, use_scheme_cache):
    common_paths = CommonPaths()

    kikimr_cluster, configurator = setup_kikimr(common_paths)
    nbs_control = setup_nbs_control(common_paths, kikimr_cluster, configurator, use_scheme_cache)
    nbs_hw = setup_nbs_hw(common_paths, kikimr_cluster, configurator, use_scheme_cache)

    try:
        ret = test_case.run(nbs_hw, nbs_hw.mon_port, nbs_control.mon_port)
    finally:
        nbs_hw.stop()
        nbs_control.stop()
        kikimr_cluster.stop()
    return ret


@pytest.mark.parametrize("test_case", TESTS, ids=[x.name for x in TESTS])
@pytest.mark.parametrize("use_scheme_cache", [True, False], ids=['schemeCache', 'noSchemeCache'])
def test_monitoring(test_case, use_scheme_cache):
    assert __run_test(test_case, use_scheme_cache) is True
