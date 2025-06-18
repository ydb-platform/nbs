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
