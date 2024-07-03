import grpc
import logging
import pytest
from concurrent import futures

import yatest.common.network as network

import cloud.blockstore.public.sdk.python.protos as protos
import cloud.blockstore.public.api.grpc.service_pb2_grpc as service_pb2_grpc

from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.client import CreateClient
from cloud.blockstore.public.sdk.python.client.discovery import CreateDiscoveryClient
from cloud.blockstore.public.sdk.python.client.error import ClientError


class NbsServiceMock(service_pb2_grpc.TBlockStoreServiceServicer):

    def __init__(self, endpoint):
        super(NbsServiceMock, self).__init__()
        self.__endpoint = endpoint

    def Ping(self, request, context):
        return protos.TPingResponse()

    def DiscoverInstances(self, request, context):
        response = protos.TDiscoverInstancesResponse()

        n = request.Limit if request.Limit > 0 else 10

        response.Instances.add(
            Host=self.__endpoint.split(':')[0],
            Port=int(self.__endpoint.split(':')[1]),
        )

        for i in range(n-1):
            response.Instances.add(
                Host=f'myt1-ct5-{i + 1}.cloud.yandex.net',
                Port=9766
            )

        return response

    def QueryAvailableStorage(self, request, context):
        response = protos.TQueryAvailableStorageResponse()

        for i, name in enumerate(request.AgentIds):
            response.AvailableStorage.add(
                AgentId=name,
                ChunkSize=93*1024**3,
                ChunkCount=i*3
            )

        return response

    def CreateVolumeFromDevice(self, request, context):
        response = protos.TCreateVolumeFromDeviceResponse()

        if request.AgentId != "myt1-ct5-1.cloud.yandex.net":
            response.Error.Code = EResult.E_ARGUMENT.value

        if request.Path != "/dev/disk/by-partlabel/NVMECOMPUTE01":
            response.Error.Code = EResult.E_ARGUMENT.value

        return response

    def ResumeDevice(self, request, context):
        response = protos.TResumeDeviceResponse()

        if request.AgentId != "myt1-ct5-1.cloud.yandex.net":
            response.Error.Code = EResult.E_ARGUMENT.value

        if request.Path != "/dev/disk/by-partlabel/NVMECOMPUTE01":
            assert request.DryRun
            response.Error.Code = EResult.E_ARGUMENT.value

        assert not request.DryRun
        return response

    def QueryAgentsInfo(self, request, context):
        response = protos.TQueryAgentsInfoResponse()
        device = protos.TDeviceInfo()
        device.DeviceName = "vol-1"
        device.DeviceSerialNumber = "123456"
        device.DeviceTotalSpaceInBytes = 93*1024**3
        agent = protos.TAgentInfo()
        agent.AgentId = "myt1-ct5-1.cloud.yandex.net"
        agent.Devices.append(device)
        response = protos.TQueryAgentsInfoResponse()
        response.Agents.append(agent)
        return response


class NbsServer:

    def __init__(self):
        self.__pm = None
        self.__endpoint = None
        self.__impl = None

    @property
    def endpoint(self):
        return self.__endpoint

    def __enter__(self):
        self.__pm = network.PortManager()
        self.__endpoint = f'localhost:{self.__pm.get_port()}'

        self.__impl = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

        service_pb2_grpc.add_TBlockStoreServiceServicer_to_server(
            NbsServiceMock(self.endpoint),
            self.__impl
        )

        self.__impl.add_insecure_port(self.endpoint)
        self.__impl.start()

        return self

    def __exit__(self, *args):
        self.__impl.stop(grace=10)
        self.__impl.wait_for_termination()

        self.__impl = None
        self.__endpoint = None
        self.__pm = None


def test_discovery():
    logging.basicConfig()

    with NbsServer() as nbs:
        client = CreateDiscoveryClient([nbs.endpoint])

        response = client.discover_instances(limit=3)

        assert len(response.Instances) == 3

        assert response.Instances[0].Host == 'localhost'
        assert response.Instances[0].Port == int(nbs.endpoint.split(':')[1])

        for i, ep in enumerate(response.Instances[1:]):
            assert ep.Host == f'myt1-ct5-{i + 1}.cloud.yandex.net'
            assert ep.Port == 9766

        client.close()


def test_query_available_storage():
    logging.basicConfig()

    with NbsServer() as nbs:
        client = CreateDiscoveryClient([nbs.endpoint])

        storage = client.query_available_storage([
            f'myt1-ct5-{i + 1}.cloud.yandex.net' for i in range(3)
        ])

        assert len(storage) == 3
        for i, info in enumerate(storage):
            assert info.AgentId == f'myt1-ct5-{i + 1}.cloud.yandex.net'
            assert info.ChunkSize == 93*1024**3
            assert info.ChunkCount == i*3

        client.close()


def test_create_volume_from_device():
    logging.basicConfig()

    with NbsServer() as nbs:
        client = CreateClient(nbs.endpoint)

        client.create_volume_from_device(
            "vol0",
            "myt1-ct5-1.cloud.yandex.net",
            "/dev/disk/by-partlabel/NVMECOMPUTE01")

        with pytest.raises(ClientError):
            client.create_volume_from_device(
                "vol0",
                "unknown",
                "/dev/disk/by-partlabel/NVMECOMPUTE01")

        client.close()


def test_resume_device():
    logging.basicConfig()

    with NbsServer() as nbs:
        client = CreateClient(nbs.endpoint)

        client.resume_device(
            "myt1-ct5-1.cloud.yandex.net",
            "/dev/disk/by-partlabel/NVMECOMPUTE01",
            dry_run=False)

        with pytest.raises(ClientError):
            client.resume_device(
                "unknown",
                "/dev/disk/by-partlabel/NVMECOMPUTE01",
                dry_run=True)

        client.close()


def test_query_agents_info():
    logging.basicConfig()

    with NbsServer() as nbs:
        client = CreateClient(nbs.endpoint)

        response = client.query_agents_info()
        agents = response.Agents
        assert len(agents) == 1
        agent = agents[0]
        assert agent.AgentId == "myt1-ct5-1.cloud.yandex.net"
        assert len(agent.Devices) == 1
        device = agent.Devices[0]
        assert device.DeviceName == "vol-1"
        assert device.DeviceSerialNumber == "123456"
        assert device.DeviceTotalSpaceInBytes == 93*1024**3
        client.close()
