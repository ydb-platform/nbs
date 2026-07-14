import logging
import os
import pytest
import time
import sys
import socket
import itertools
import struct

import cloud.storage.core.protos.device_pb2 as device_pb2

from cloud.blockstore.tests.python.lib.test_client import CreateTestClient

from cloud.blockstore.public.sdk.python.client import Session
from cloud.blockstore.public.sdk.python.client.error import ClientError
from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.protos import STORAGE_MEDIA_SSD_LOCAL

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, \
    generate_disk_agent_txt

import cloud.blockstore.tests.python.lib.daemon as daemon

import yatest.common as yatest_common

from yatest.common.network import PortManager

from contrib.ydb.tests.library.harness.kikimr_runner import \
    get_unique_path_for_current_test, ensure_path_exists


DEVICE_SIZE = 8 * 1024 ** 2  # 8 MiB
DEVICE_COUNT = 8
DEVICE_PADDING = 4096
DEVICE_HEADER = 4096
DEVICE_BLOCK_SIZE = 4096
STORAGE_POOL_NAME = "journalled"

KNOWN_DEVICE_POOLS = {
    "KnownDevicePools": [
        {
            "Name": STORAGE_POOL_NAME,
            "Kind": "DEVICE_POOL_KIND_LOCAL",
            "AllocationUnit": DEVICE_SIZE
        },
    ]}

@pytest.fixture(name='agent_id')
def get_agent_id():
    return daemon.get_fqdn()

@pytest.fixture(name='tcp_port')
def get_tcp_port():
    pm = PortManager()

    return pm.get_port()


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = daemon.start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


@pytest.fixture(name='nbs')
def start_nbs_daemon(ydb):

    cfg = NbsConfigurator(ydb)
    cfg.generate_default_nbs_configs()

    cfg.files["storage"].DisableLocalService = 0
    cfg.files["storage"].NonReplicatedDontSuspendDevices = True
    cfg.files["storage"].NonReplicatedAgentMinTimeout = 600000  # 10min
    cfg.files["storage"].NonReplicatedAgentMaxTimeout = 600000  # 10min

    p = daemon.start_nbs(cfg)

    client = CreateTestClient(f"localhost:{p.port}")
    client.execute_DiskRegistrySetWritableState(State=True)
    client.update_disk_registry_config(KNOWN_DEVICE_POOLS)

    yield p

    p.kill()

@pytest.fixture(autouse=True)
def start_disk_agent(ydb, nbs, agent_id, tcp_port, tmp_path):

    data_path = get_unique_path_for_current_test(
        output_path=tmp_path,
        sub_folder="data")

    data_path = os.path.join(data_path, "dev", "disk", "by-partlabel")
    ensure_path_exists(data_path)

    with open(os.path.join(data_path, 'NVMEJD01'), 'wb') as f:
        os.truncate(
            f.fileno(),
            DEVICE_HEADER + DEVICE_SIZE * DEVICE_COUNT + (DEVICE_COUNT - 1) *
            DEVICE_PADDING)

    cfg = NbsConfigurator(ydb, 'disk-agent')
    cfg.generate_default_nbs_configs()

    config = generate_disk_agent_txt(
        agent_id='',
        storage_discovery_config={
            "PathConfigs": [{
                "BlockSize": DEVICE_BLOCK_SIZE,
                "PathRegExp": f"{data_path}/NVMEJD([0-9]+)",
                "PoolConfigs": [{
                    "PoolName": STORAGE_POOL_NAME,
                    "Layout": {
                        "DeviceSize": DEVICE_SIZE,
                        "DevicePadding": DEVICE_PADDING,
                        "HeaderSize": DEVICE_HEADER
                    }
                }]}
            ]})

    config.JournalledDeviceTcpServerListenAddress = f"localhost:{tcp_port}"

    cfg.files["disk-agent"] = config

    disk_agent = daemon.start_disk_agent(cfg)
    disk_agent.wait_for_registration()

    client = CreateTestClient(f"localhost:{nbs.port}")

    client.add_host(agent_id)
    client.wait_for_devices_to_be_cleared()

    yield disk_agent

    disk_agent.stop()

################################################################################

class DeviceProtocolError(Exception):
    """Journalled Device Protocol Error"""


class DeviceTcpClient:

    _HEADER_STRUCT = struct.Struct("!I")

    def __init__(self, port):
        self._port = port
        self._socket = None
        self._request_ids = itertools.count(1)
        self._connect_timeout = 5.0
        self._request_timeout = 30.0

    def connect(self) -> None:
        if self._socket is not None:
            return

        sock = socket.create_connection(
            ("localhost", self._port),
            timeout=self._connect_timeout,
        )
        sock.settimeout(self._request_timeout)
        self._socket = sock

    def close(self) -> None:
        if self._socket is None:
            return

        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        self._socket.close()
        self._socket = None

    def __enter__(self) -> "DeviceTcpClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def acquire_device(self, client_id, device_uuids) -> device_pb2.TAcquireDevicesResponse:
        request = device_pb2.TDeviceProtocolRequest(
            RequestId=next(self._request_ids),
            AcquireDevices=device_pb2.TAcquireDevicesRequest(
                Headers=self._make_headers(client_id),
                DeviceUUIDs=device_uuids,
            ),
        )

        response = self._exchange(request)
        self._ensure_response_type(response, "AcquireDevices")

        return response.AcquireDevices

    def read_pages(
        self,
        client_id,
        device_uuid: str,
        page_groups: list[tuple[int, int, int]],
    ) -> device_pb2.TReadPagesResponse:
        """
        page_groups:
            [(first_page_no, page_count, page_size), ...]
        """

        refs = [
            device_pb2.TDevicePageGroupRef(
                FirstPageNo=first_page_no,
                PageCount=page_count,
                PageSize=page_size,
            )
            for first_page_no, page_count, page_size in page_groups
        ]

        request = device_pb2.TDeviceProtocolRequest(
            RequestId=next(self._request_ids),
            ReadPages=device_pb2.TReadPagesRequest(
                Headers=self._make_headers(client_id),
                DeviceUUID=device_uuid,
                PageGroupRefs=refs,
            ),
        )

        response = self._exchange(request)
        self._ensure_response_type(response, "ReadPages")

        return response.ReadPages

    def write_log_record(
        self,
        client_id,
        device_uuid: str,
        page_groups: list[tuple[int, list[bytes]]],
        log_sequence_number: int,
    ) -> device_pb2.TWriteLogRecordResponse:
        """
        page_groups:
            [
                (first_page_no, [page_0_bytes, page_1_bytes, ...]),
                ...
            ]
        """

        groups = [
            device_pb2.TDevicePageGroup(
                FirstPageNo=first_page_no,
                Content=pages,
            )
            for first_page_no, pages in page_groups
        ]

        request = device_pb2.TDeviceProtocolRequest(
            RequestId=next(self._request_ids),
            WriteLogRecord=device_pb2.TWriteLogRecordRequest(
                Headers=self._make_headers(client_id),
                DeviceUUID=device_uuid,
                PageGroups=groups,
                LogSequenceNumber=log_sequence_number,
            ),
        )

        response = self._exchange(request)
        self._ensure_response_type(response, "WriteLogRecord")

        return response.WriteLogRecord

    def _make_headers(self, client_id):
        return device_pb2.TDeviceRequestHeaders(
            ClientId=client_id,
        )

    def _recv_exact(self, size: int) -> bytes:
        assert self._socket is not None

        chunks: list[bytes] = []
        remaining = size

        while remaining:
            chunk = self._socket.recv(remaining)

            if not chunk:
                raise ConnectionError(
                    "Server closed the connection unexpectedly"
                )

            chunks.append(chunk)
            remaining -= len(chunk)

        return b"".join(chunks)

    @staticmethod
    def _ensure_response_type(
        response: device_pb2.TDeviceProtocolResponse,
        expected_type: str):

        actual_type = response.WhichOneof("Response")

        if actual_type != expected_type:
            raise DeviceProtocolError(
                f"Unexpected response type: "
                f"expected {expected_type}, got {actual_type}"
            )

    def _exchange(
        self,
        request: device_pb2.TDeviceProtocolRequest,
    ) -> device_pb2.TDeviceProtocolResponse:

        self.connect()

        assert self._socket is not None

        payload = request.SerializeToString()

        frame = self._HEADER_STRUCT.pack(len(payload)) + payload

        try:
            self._socket.sendall(frame)

            response_size_data = self._recv_exact(
                self._HEADER_STRUCT.size
            )
            (response_size,) = self._HEADER_STRUCT.unpack(
                response_size_data
            )

            response_payload = self._recv_exact(response_size)

        except (OSError, socket.timeout) as error:
            self.close()
            raise DeviceProtocolError(
                f"TCP exchange failed: {error}"
            ) from error

        response = device_pb2.TDeviceProtocolResponse()

        try:
            response.ParseFromString(response_payload)
        except DecodeError as error:
            raise DeviceProtocolError(
                "Server returned an invalid protobuf response"
            ) from error

        if response.RequestId != request.RequestId:
            raise DeviceProtocolError(
                "RequestId mismatch: "
                f"sent {request.RequestId}, "
                f"received {response.RequestId}"
            )

        response_type = response.WhichOneof("Response")

        if response_type is None:
            raise DeviceProtocolError(
                "Response does not contain a Response field"
            )

        if response_type == "ProtocolError":
            raise DeviceProtocolError(
                f"Protocol error: {response.ProtocolError}"
            )

        return response

################################################################################

def test_journalled_devices(nbs, tcp_port):

    logger = logging.getLogger("client")
    logger.setLevel(logging.DEBUG)

    nbsClient = CreateTestClient(f"localhost:{nbs.port}", log=logger)

    dummy_disk_id = "dummy"

    nbsClient.create_volume(
        disk_id=dummy_disk_id,
        block_size=DEVICE_BLOCK_SIZE,
        blocks_count=3*DEVICE_SIZE//DEVICE_BLOCK_SIZE,
        storage_media_kind=STORAGE_MEDIA_SSD_LOCAL,
        storage_pool_name=STORAGE_POOL_NAME)

    response = nbsClient.describe_volume(dummy_disk_id)

    assert len(response.Devices) == 3

    device_uuid = response.Devices[0].DeviceUUID
    page_size = DEVICE_BLOCK_SIZE
    client_id = "client-id"

    with DeviceTcpClient(tcp_port) as tcp:

        resp = tcp.acquire_device(client_id, device_uuids=[device_uuid])
        assert resp.Error.Code == 0

        resp = tcp.write_log_record(
            client_id,
            device_uuid=device_uuid,
            page_groups=[
                (
                    100,
                    [
                        b"A" * page_size,
                        b"B" * page_size,
                    ],
                ),
            ],
            log_sequence_number=42,
        )
        assert resp.Error.Code == 0

        resp = tcp.read_pages(client_id, device_uuid, page_groups=[
            (100, 2, page_size),
        ])
        assert resp.Error.Code == 0

        assert len(resp.PageGroups) == 1
        group = resp.PageGroups[0]
        assert group.FirstPageNo == 100
        assert len(group.Content) == 2
        assert group.Content[0] == b"A" * page_size
        assert group.Content[1] == b"B" * page_size

