from dataclasses import dataclass
from typing import List, Optional, Union

import cloud.filestore.public.sdk.python.protos as protos

from .error import _handle_errors
from .grpc_client import CreateGrpcClient, CreateGrpcEndpointClient
from .durable import DurableClient


class Client(object):

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
    def create_filestore_async(
            self,
            filesystem_id,
            project_id="",
            folder_id="",
            cloud_id="",
            block_size=0,
            blocks_count=0,
            storage_media=protos.STORAGE_MEDIA_HDD,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateFileStoreRequest(
            FileSystemId=filesystem_id,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
            BlockSize=block_size,
            BlocksCount=blocks_count,
            StorageMediaKind=storage_media,
        )

        return self.__impl.create_filestore_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_filestore(
            self,
            filesystem_id,
            project_id="",
            folder_id="",
            cloud_id="",
            block_size=0,
            blocks_count=0,
            storage_media=protos.STORAGE_MEDIA_HDD,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateFileStoreRequest(
            FileSystemId=filesystem_id,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
            BlockSize=block_size,
            BlocksCount=blocks_count,
            StorageMediaKind=storage_media,
        )

        return self.__impl.create_filestore(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def alter_filestore_async(
            self,
            filesystem_id,
            project_id,
            folder_id,
            cloud_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TAlterFileStoreRequest(
            FileSystemId=filesystem_id,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
        )

        return self.__impl.alter_filestore_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def alter_filestore(
            self,
            filesystem_id,
            project_id,
            folder_id,
            cloud_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TAlterFileStoreRequest(
            FileSystemId=filesystem_id,
            ProjectId=project_id,
            FolderId=folder_id,
            CloudId=cloud_id,
        )

        return self.__impl.alter_filestore(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def resize_filestore_async(
            self,
            filesystem_id,
            blocks_count,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TResizeFileStoreRequest(
            FileSystemId=filesystem_id,
            BlocksCount=blocks_count,
        )

        return self.__impl.resize_filestore_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def resize_filestore(
            self,
            filesystem_id,
            blocks_count,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TResizeFileStoreRequest(
            FileSystemId=filesystem_id,
            BlocksCount=blocks_count,
        )

        return self.__impl.resize_filestore(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def destroy_filestore_async(
            self,
            filesystem_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDestroyFileStoreRequest(
            FileSystemId=filesystem_id,
        )

        return self.__impl.destroy_filestore_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def destroy_filestore(
            self,
            filesystem_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDestroyFileStoreRequest(
            FileSystemId=filesystem_id,
        )

        return self.__impl.destroy_filestore(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def get_filestore_info_async(
            self,
            filesystem_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TGetFileStoreInfoRequest(
            FileSystemId=filesystem_id,
        )

        return self.__impl.get_filestore_info_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def get_filestore_info(
            self,
            filesystem_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TGetFileStoreInfoRequest(
            FileSystemId=filesystem_id,
        )

        return self.__impl.get_filestore_info(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_checkpoint_async(
            self,
            filesystem_id,
            checkpoint_id,
            node_id=0,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateCheckpointRequest(
            FileSystemId=filesystem_id,
            CheckpointId=checkpoint_id,
            NodeId=node_id,
        )

        return self.__impl.create_checkpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_checkpoint(
            self,
            filesystem_id,
            checkpoint_id,
            node_id=0,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TCreateCheckpointRequest(
            FileSystemId=filesystem_id,
            CheckpointId=checkpoint_id,
            NodeId=node_id,
        )

        return self.__impl.create_checkpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def destroy_checkpoint_async(
            self,
            filesystem_id,
            checkpoint_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDestroyCheckpointRequest(
            FileSystemId=filesystem_id,
            CheckpointId=checkpoint_id,
        )

        return self.__impl.destroy_checkpoint_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def destroy_checkpoint(
            self,
            filesystem_id,
            checkpoint_id,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDestroyCheckpointRequest(
            FileSystemId=filesystem_id,
            CheckpointId=checkpoint_id,
        )

        return self.__impl.destroy_checkpoint(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def describe_filestore_model_async(
            self,
            block_size,
            blocks_count,
            storage_media,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):

        request = protos.TDescribeFileStoreModelRequest(
            BlockSize=block_size,
            BlocksCount=blocks_count,
            StorageMediaKind=storage_media,
        )

        return self.__impl.describe_filestore_model_async(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def describe_filestore_model(
            self,
            block_size,
            blocks_count,
            storage_media,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TDescribeFileStoreModelRequest(
            BlockSize=block_size,
            BlocksCount=blocks_count,
            StorageMediaKind=storage_media,
        )

        return self.__impl.describe_filestore_model(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    @_handle_errors
    def create_session(
        self,
        filesystem_id: str,
        idempotence_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        trace_id: Optional[str] = None,
        request_timeout: Optional[float] = None,
    ) -> protos.TCreateSessionResponse:
        request = protos.TCreateSessionRequest(
            FileSystemId=filesystem_id,
        )
        return self.__impl.create_session(
            request, idempotence_id, timestamp, trace_id, request_timeout
        )

    @dataclass
    class Directory:
        Mode: int  # uint32

    @dataclass
    class File:
        Mode: int  # uint32

    @dataclass
    class Link:
        TargetNode: int  # uint64
        ShardNodeName: Optional[str] = None

    @dataclass
    class SymLink:
        TargetPath: bytes

    @dataclass
    class Socket:
        Mode: int  # uint32

    Node = Union[Directory, File, Link, SymLink, Socket]

    @_handle_errors
    def create_node(
        self,
        filesystem_id: str,
        session_id: bytes,
        node: Node,
        parent_node_id: int,
        name: str,
        idempotence_id=None,
        timestamp=None,
        trace_id=None,
        request_timeout=None,
    ) -> protos.TCreateNodeResponse:

        kwargs = {}
        if isinstance(node, Client.Directory):
            kwargs["Directory"] = protos.TCreateNodeRequest.TDirectory(
                Mode=node.Mode
            )
        elif isinstance(node, Client.File):
            kwargs["File"] = protos.TCreateNodeRequest.TFile(Mode=node.Mode)
        elif isinstance(node, Client.Link):
            kwargs["Link"] = protos.TCreateNodeRequest.TLink(
                TargetNode=node.TargetNode
            )
        elif isinstance(node, Client.SymLink):
            kwargs["SymLink"] = protos.TCreateNodeRequest.TSymLink(
                TargetPath=node.TargetPath
            )
        elif isinstance(node, Client.Socket):
            kwargs["Socket"] = protos.TCreateNodeRequest.TSocket(Mode=node.Mode)
        else:
            raise ValueError("Unsupported node type: {}".format(type(node)))
        request = protos.TCreateNodeRequest(
            Headers=protos.THeaders(SessionId=session_id),
            FileSystemId=filesystem_id,
            NodeId=parent_node_id,
            Name=name.encode("utf-8"),
            **kwargs
        )

        return self.__impl.create_node(
            request, idempotence_id, timestamp, trace_id, request_timeout
        )

    @_handle_errors
    def list_nodes(
        self,
        filesystem_id: str,
        session_id: bytes,
        node_id: int,
        cookie: Optional[bytes] = None,
        max_bytes: Optional[int] = None,
        idempotence_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        trace_id: Optional[str] = None,
        request_timeout: Optional[float] = None,
    ) -> protos.TListNodesResponse:
        request = protos.TListNodesRequest(
            Headers=protos.THeaders(SessionId=session_id),
            FileSystemId=filesystem_id,
            NodeId=node_id,
            Cookie=cookie,
            MaxBytes=max_bytes,
        )
        return self.__impl.list_nodes(
            request, idempotence_id, timestamp, trace_id, request_timeout
        )

    @_handle_errors
    def unlink_node(
        self,
        filesystem_id: str,
        session_id: bytes,
        node_id: int,
        name: str,
        unlink_directory: bool = False,
        idempotence_id=None,
        timestamp=None,
        trace_id=None,
        request_timeout=None,
    ) -> protos.TUnlinkNodeResponse:
        request = protos.TUnlinkNodeRequest(
            Headers=protos.THeaders(SessionId=session_id),
            FileSystemId=filesystem_id,
            NodeId=node_id,
            Name=name.encode("utf-8"),
            UnlinkDirectory=unlink_directory,
        )
        return self.__impl.unlink_node(
            request, idempotence_id, timestamp, trace_id, request_timeout
        )

    @_handle_errors
    def execute_action(
            self,
            action: str,
            input: bytes,
            idempotence_id=None,
            timestamp=None,
            trace_id=None,
            request_timeout=None):
        request = protos.TExecuteActionRequest(
            Action=action,
            Input=input
        )

        return self.__impl.execute_action(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

    class CreateHandleFlags:
        def __init__(self, flags: List[protos.TCreateHandleRequest.EFlags]):
            self.flags = 0

            for flag in flags:
                self.flags |= 1 << (flag - 1)

    def create_handle(
        self,
        filesystem_id: str,
        session_id: bytes,
        node_id: int,
        name: str = "",
        flags: CreateHandleFlags = CreateHandleFlags([]),
        mode: int = 0o644,
        uid: int = 0,
        gid: int = 0,
        idempotence_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        trace_id: Optional[str] = None,
        request_timeout: Optional[float] = None,
    ) -> protos.TCreateHandleResponse:
        request = protos.TCreateHandleRequest(
            Headers=protos.THeaders(SessionId=session_id),
            FileSystemId=filesystem_id.encode("utf-8"),
            NodeId=node_id,
            Name=name.encode("utf-8"),
            Flags=flags.flags,
            Mode=mode,
            Uid=uid,
            Gid=gid,
        )

        return self.__impl.create_handle(
            request, idempotence_id, timestamp, trace_id, request_timeout)

    def read_data(
        self,
        filesystem_id: str,
        session_id: bytes,
        node_id: int,
        handle: int,
        offset: int,
        length: int,
        idempotence_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        trace_id: Optional[str] = None,
        request_timeout: Optional[float] = None
    ) -> protos.TReadDataResponse:
        request = protos.TReadDataRequest(
            Headers=protos.THeaders(SessionId=session_id),
            FileSystemId=filesystem_id.encode("utf-8"),
            NodeId=node_id,
            Handle=handle,
            Offset=offset,
            Length=length
        )

        return self.__impl.read_data(
            request, idempotence_id, timestamp, trace_id, request_timeout)

    def write_data(
        self,
        filesystem_id: str,
        session_id: bytes,
        node_id: int,
        handle: int,
        offset: int,
        buffer: bytes,
        buffer_offset: int = 0,
        idempotence_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        trace_id: Optional[str] = None,
        request_timeout: Optional[float] = None
    ) -> protos.TWriteDataResponse:
        request = protos.TWriteDataRequest(
            Headers=protos.THeaders(SessionId=session_id),
            FileSystemId=filesystem_id.encode("utf-8"),
            NodeId=node_id,
            Handle=handle,
            Offset=offset,
            Buffer=buffer[buffer_offset:]
        )

        return self.__impl.write_data(
            request, idempotence_id, timestamp, trace_id, request_timeout)


def CreateClient(
        endpoint,
        credentials=None,
        request_timeout=None,
        retry_timeout=None,
        retry_timeout_increment=None,
        log=None,
        executor=None):

    grpc_client = CreateGrpcClient(
        endpoint,
        credentials,
        request_timeout,
        log)

    durable_client = DurableClient(
        grpc_client,
        retry_timeout,
        retry_timeout_increment,
        log)

    return Client(durable_client)


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

    durable_client = DurableClient(
        grpc_client,
        retry_timeout,
        retry_timeout_increment,
        log)

    return Client(durable_client)
