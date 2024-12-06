import cloud.filestore.public.sdk.python.protos as protos

from .error import _handle_errors
from .grpc_client import CreateGrpcEndpointClient
from .durable import DurableEndpointClient


class EndpointClient(object):

    def __init__(self, impl):
        self.__impl = impl

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.__impl.close()

    @_handle_errors
    def ping_async(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TPingRequest()
        return self.__impl.ping_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def ping(
            self,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TPingRequest()
        return self.__impl.ping(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def start_endpoint_async(
            self,
            filesystem_id,
            client_id,
            socket_path,
            read_only=False,
            retry_timeout=0,
            ping_timeout=0,
            service_endpoint="",
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        endpoint = protos.TEndpointConfig(
            FileSystemId=filesystem_id,
            ClientId=client_id,
            SocketPath=socket_path,
            ReadOnly=read_only,
            SessionRetryTimeout=retry_timeout,
            SessionPingTimeout=ping_timeout,
            ServiceEndpoint=service_endpoint
        )

        request = protos.TStartEndpointRequest(
            Endpoint=endpoint
        )

        return self.__impl.start_endpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def start_endpoint(
            self,
            filesystem_id,
            client_id,
            socket_path,
            read_only=False,
            retry_timeout=0,
            ping_timeout=0,
            service_endpoint="",
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None,
            persistent=False):
        endpoint = protos.TEndpointConfig(
            FileSystemId=filesystem_id,
            ClientId=client_id,
            SocketPath=socket_path,
            ReadOnly=read_only,
            SessionRetryTimeout=retry_timeout,
            SessionPingTimeout=ping_timeout,
            ServiceEndpoint=service_endpoint,
            Persistent=persistent,
        )

        request = protos.TStartEndpointRequest(
            Endpoint=endpoint
        )

        return self.__impl.start_endpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def stop_endpoint_async(
            self,
            socket_path,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TStopEndpointRequest(
            SocketPath=socket_path
        )

        return self.__impl.stop_endpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def stop_endpoint(
            self,
            socket_path,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TStopEndpointRequest(
            SocketPath=socket_path
        )

        return self.__impl.stop_endpoint(
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

        return self.__impl.list_endpoints_async(
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

        return self.__impl.list_endpoints(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

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

        return self.__impl.kick_endpoint_async(
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
            KeyringId=keyring_id
        )

        return self.__impl.kick_endpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)


def CreateEndpointClient(
        endpoint,
        credentials=None,
        request_timeout=None,
        retry_timeout=None,
        retry_timeout_increment=None,
        log=None,
        executor=None):

    grpc_client = CreateGrpcEndpointClient(
        endpoint,
        credentials,
        request_timeout,
        log)

    durable_client = DurableEndpointClient(
        grpc_client,
        retry_timeout,
        retry_timeout_increment,
        log)

    return EndpointClient(durable_client)
