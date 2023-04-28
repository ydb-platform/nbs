from concurrent import futures

import cloud.blockstore.public.sdk.python.protos as protos

from .durable import DurableClient
from .error import _handle_errors
from .grpc_client import GrpcClient
from .http_client import HttpClient
from .safe_client import _SafeClient


class SessionInfo(object):

    def __init__(self, session_id, inactive_clients_timeout):
        self.__session_id = session_id
        self.__inactive_clients_timeout = inactive_clients_timeout

    @property
    def session_id(self):
        return self.__session_id

    @property
    def inactive_clients_timeout(self):
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
            disk_id,
            token,
            access_mode=protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode=protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags=0,
            mount_seq_number=0,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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

        def set_result(f):
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
            disk_id,
            token,
            access_mode=protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode=protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags=0,
            mount_seq_number=0,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            start_index,
            blocks_count,
            checkpoint_id,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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

        def set_result(f):
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
            disk_id,
            start_index,
            blocks_count,
            checkpoint_id,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            start_index,
            blocks,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            start_index,
            blocks,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            start_index,
            blocks_count,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            start_index,
            blocks_count,
            session_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            unix_socket_path,
            disk_id,
            ipc_type,
            client_id,
            access_mode=protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode=protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags=0,
            unaligned_requests_disabled=False,
            seq_number=0,
            vhost_queues=1,
            endpoint_request_timeout=None,
            endpoint_retry_timeout=None,
            endpoint_retry_timeout_increment=None,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None,
            device_name=None):

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

        def set_result(f):
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
            unix_socket_path,
            disk_id,
            ipc_type,
            client_id,
            access_mode=protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode=protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            mount_flags=0,
            unaligned_requests_disabled=False,
            seq_number=0,
            vhost_queues=1,
            endpoint_request_timeout=None,
            endpoint_retry_timeout=None,
            endpoint_retry_timeout_increment=None,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None,
            device_name=None):

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
            unix_socket_path,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TStopEndpointRequest(
            UnixSocketPath=unix_socket_path,
        )
        return self._impl.stop_endpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def stop_endpoint(
            self,
            unix_socket_path,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TStopEndpointRequest(
            UnixSocketPath=unix_socket_path,
        )
        self._impl.stop_endpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def list_endpoints_async(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            keyring_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            keyring_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            agent_id,
            path,
            project_id="",
            folder_id="",
            cloud_id="",
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
            disk_id,
            agent_id,
            path,
            project_id="",
            folder_id="",
            cloud_id="",
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

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
        endpoint,
        credentials=None,
        request_timeout=None,
        retry_timeout=None,
        retry_timeout_increment=None,
        log=None,
        executor=None):

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
