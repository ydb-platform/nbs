from concurrent import futures
from datetime import datetime
from logging import Logger

import cloud.blockstore.public.sdk.python.protos as protos

from .credentials import ClientCredentials
from .durable import DurableClient
from .error import _handle_errors
from .grpc_client import GrpcClient
from .http_client import HttpClient
from .safe_client import _SafeClient

from google.protobuf.internal.containers import RepeatedScalarFieldContainer


class SessionInfo(object):

    def __init__(
            self,
            session_id: str,
            inactive_clients_timeout: int):
        self.__session_id = session_id
        self.__inactive_clients_timeout = inactive_clients_timeout

    @property
    def session_id(self) -> str:
        return self.__session_id

    @property
    def inactive_clients_timeout(self) -> int:
        return self.__inactive_clients_timeout


class Client(_SafeClient):

    def __init__(self, impl):
        super(Client, self).__init__(impl)

    def close(self):
        super(Client, self).close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @_handle_errors
    def mount_volume_async(
            self,
            disk_id: str,
            token: str,
            access_mode: protos.EVolumeAccessMode = protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode: protos.EVolumeMountMode = protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags: int = 0,
            mount_seq_number: int = 0,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TMountVolumeRequest(
            DiskId=disk_id,
            Token=token,
            VolumeAccessMode=access_mode,
            VolumeMountMode=mount_mode,
            MountFlags=mount_flags,
            MountSeqNumber=mount_seq_number
        )

        future = futures.Future()
        response = self._impl.mount_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f: futures.Future):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                result = f.result()
                session_info = SessionInfo(
                    result.SessionId,
                    result.InactiveClientsTimeout / 1000.0
                )
                future.set_result({
                    "Volume": result.Volume,
                    "SessionInfo": session_info,
                })
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def mount_volume(
            self,
            disk_id: str,
            token: str,
            access_mode: protos.EVolumeAccessMode = protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode: protos.EVolumeMountMode = protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags: int = 0,
            mount_seq_number: int = 0,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> dict:

        request = protos.TMountVolumeRequest(
            DiskId=disk_id,
            Token=token,
            VolumeAccessMode=access_mode,
            VolumeMountMode=mount_mode,
            MountFlags=mount_flags,
            MountSeqNumber=mount_seq_number
        )
        response = self._impl.mount_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        session_info = SessionInfo(
            response.SessionId,
            response.InactiveClientsTimeout / 1000.0
        )
        return {
            "Volume": response.Volume,
            "SessionInfo": session_info,
        }

    @_handle_errors
    def unmount_volume_async(
            self,
            disk_id: str,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TUnmountVolumeRequest(
            DiskId=disk_id,
            SessionId=session_id,
        )
        return self._impl.unmount_volume_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def unmount_volume(
            self,
            disk_id: str,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TUnmountVolumeRequest(
            DiskId=disk_id,
            SessionId=session_id,
        )
        self._impl.unmount_volume(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def read_blocks_async(
            self,
            disk_id: str,
            start_index: int,
            blocks_count: int,
            checkpoint_id: str,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TReadBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            BlocksCount=blocks_count,
            CheckpointId=checkpoint_id,
            SessionId=session_id,
        )

        future = futures.Future()
        response = self._impl.read_blocks_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f: futures.Future):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(f.result().Blocks.Buffers)
        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def read_blocks(
            self,
            disk_id: str,
            start_index: int,
            blocks_count: int,
            checkpoint_id: str,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> RepeatedScalarFieldContainer:

        request = protos.TReadBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            BlocksCount=blocks_count,
            CheckpointId=checkpoint_id,
            SessionId=session_id,
        )
        response = self._impl.read_blocks(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.Blocks.Buffers

    @_handle_errors
    def write_blocks_async(
            self,
            disk_id: str,
            start_index: int,
            blocks: bytes,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TWriteBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            Blocks=protos.TIOVector(Buffers=blocks),
            SessionId=session_id,
        )
        return self._impl.write_blocks_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def write_blocks(
            self,
            disk_id: str,
            start_index: int,
            blocks: bytes,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TWriteBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            Blocks=protos.TIOVector(Buffers=blocks),
            SessionId=session_id,
        )
        self._impl.write_blocks(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def zero_blocks_async(
            self,
            disk_id: str,
            start_index: int,
            blocks_count: int,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TZeroBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            BlocksCount=blocks_count,
            SessionId=session_id,
        )
        return self._impl.zero_blocks_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def zero_blocks(
            self,
            disk_id: str,
            start_index: int,
            blocks_count: int,
            session_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TZeroBlocksRequest(
            DiskId=disk_id,
            StartIndex=start_index,
            BlocksCount=blocks_count,
            SessionId=session_id,
        )
        self._impl.zero_blocks(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def start_endpoint_async(
            self,
            unix_socket_path: str,
            disk_id: str,
            ipc_type: protos.EClientIpcType,
            client_id: str,
            access_mode: protos.EVolumeAccessMode = protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode: protos.EVolumeMountMode = protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags: int = 0,
            unaligned_requests_disabled: bool = False,
            seq_number: int = 0,
            vhost_queues: int = 1,
            endpoint_request_timeout: int | None = None,
            endpoint_retry_timeout: int | None = None,
            endpoint_retry_timeout_increment: int | None = None,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None,
            device_name: str | None = None) -> futures.Future:

        request = protos.TStartEndpointRequest(
            UnixSocketPath=unix_socket_path,
            DiskId=disk_id,
            IpcType=ipc_type,
            ClientId=client_id,
            VolumeAccessMode=access_mode,
            VolumeMountMode=mount_mode,
            MountFlags=mount_flags,
            UnalignedRequestsDisabled=unaligned_requests_disabled,
            MountSeqNumber=seq_number,
            VhostQueuesCount=vhost_queues
        )

        if endpoint_request_timeout is not None:
            request.RequestTimeout = endpoint_request_timeout

        if endpoint_retry_timeout is not None:
            request.RetryTimeout = endpoint_retry_timeout

        if endpoint_retry_timeout_increment is not None:
            request.RetryTimeoutIncrement = endpoint_retry_timeout_increment

        if device_name is not None:
            request.DeviceName = device_name

        future = futures.Future()
        response = self._impl.start_endpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def set_result(f: futures.Future):
            exception = f.exception()
            if exception:
                future.set_exception(exception)
            else:
                result = f.result()
                future.set_result({
                    "Volume": result.Volume,
                })

        response.add_done_callback(set_result)

        return future

    @_handle_errors
    def start_endpoint(
            self,
            unix_socket_path: str,
            disk_id: str,
            ipc_type: protos.EClientIpcType,
            client_id: str,
            access_mode: protos.EVolumeAccessMode = protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode: protos.EVolumeMountMode = protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags: int = 0,
            unaligned_requests_disabled: bool = False,
            seq_number: int = 0,
            vhost_queues: int = 1,
            endpoint_request_timeout: int | None = None,
            endpoint_retry_timeout: int | None = None,
            endpoint_retry_timeout_increment: int | None = None,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None,
            device_name: str | None = None) -> dict:

        request = protos.TStartEndpointRequest(
            UnixSocketPath=unix_socket_path,
            DiskId=disk_id,
            IpcType=ipc_type,
            ClientId=client_id,
            VolumeAccessMode=access_mode,
            VolumeMountMode=mount_mode,
            MountFlags=mount_flags,
            UnalignedRequestsDisabled=unaligned_requests_disabled,
            MountSeqNumber=seq_number,
            VhostQueuesCount=vhost_queues
        )

        if endpoint_request_timeout is not None:
            request.RequestTimeout = endpoint_request_timeout

        if endpoint_retry_timeout is not None:
            request.RetryTimeout = endpoint_retry_timeout

        if endpoint_retry_timeout_increment is not None:
            request.RetryTimeoutIncrement = endpoint_retry_timeout_increment

        if device_name is not None:
            request.DeviceName = device_name

        response = self._impl.start_endpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout
        )
        return {
            "Volume": response.Volume,
        }

    @_handle_errors
    def stop_endpoint_async(
            self,
            unix_socket_path: str,
            client_id: str | None = None,
            disk_id: str | None = None,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TStopEndpointRequest(
            UnixSocketPath=unix_socket_path,
        )
        if client_id is not None:
            request.Headers.ClientId = client_id
        if disk_id is not None:
            request.DiskId = disk_id

        return self._impl.stop_endpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def stop_endpoint(
            self,
            unix_socket_path: str,
            client_id: str | None = None,
            disk_id: str | None = None,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TStopEndpointRequest(
            UnixSocketPath=unix_socket_path,
        )
        if client_id is not None:
            request.Headers.ClientId = client_id
        if disk_id is not None:
            request.DiskId = disk_id
        self._impl.stop_endpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def refresh_endpoint_async(
            self,
            unix_socket_path: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TRefreshEndpointRequest(
            UnixSocketPath=unix_socket_path,
        )
        return self._impl.refresh_endpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def refresh_endpoint(
            self,
            unix_socket_path: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TRefreshEndpointRequest(
            UnixSocketPath=unix_socket_path,
        )
        self._impl.refresh_endpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def list_endpoints_async(
            self,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TListEndpointsRequest()
        return self._impl.list_endpoints_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def list_endpoints(
            self,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TListEndpointsRequest()
        response = self._impl.list_endpoints(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)
        return response.Endpoints

    @_handle_errors
    def kick_endpoint_async(
            self,
            keyring_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TKickEndpointRequest(
            KeyringId=keyring_id
        )
        return self._impl.kick_endpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def kick_endpoint(
            self,
            keyring_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TKickEndpointRequest(
            KeyringId=keyring_id,
        )
        self._impl.kick_endpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_volume_from_device_async(
            self,
            disk_id: str,
            agent_id: str,
            path: str,
            project_id: str = "",
            folder_id: str = "",
            cloud_id: str = "",
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        request = protos.TCreateVolumeFromDeviceRequest(
            DiskId=disk_id,
            AgentId=agent_id,
            Path=path,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
        )
        return self._impl.create_volume_from_device_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_volume_from_device(
            self,
            disk_id: str,
            agent_id: str,
            path: str,
            project_id: str = "",
            folder_id: str = "",
            cloud_id: str = "",
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        request = protos.TCreateVolumeFromDeviceRequest(
            DiskId=disk_id,
            AgentId=agent_id,
            Path=path,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
        )
        self._impl.create_volume_from_device(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)


def CreateClient(
        endpoint: str,
        credentials: ClientCredentials | None = None,
        request_timeout: int | None = None,
        retry_timeout: int | None = None,
        retry_timeout_increment: int | None = None,
        log: type[Logger] | None = None,
        executor: futures.ThreadPoolExecutor | None = None):

    backend = None

    if endpoint.startswith('https://') or \
            endpoint.startswith('http://'):
        backend = HttpClient(
            endpoint,
            credentials,
            request_timeout,
            log,
            executor)
    else:
        backend = GrpcClient(
            endpoint,
            credentials,
            request_timeout,
            log)

    durable_client = DurableClient(
        backend,
        retry_timeout,
        retry_timeout_increment,
        log)

    return Client(durable_client)
