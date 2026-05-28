#include "fs.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemShardStub: IFileSystemShard
{
    template <typename TResponse, typename TRequest>
    NThreading::TFuture<TResponse> NotImplemented(TRequest request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return NThreading::MakeFuture(std::move(response));
    }

    NThreading::TFuture<NProtoPrivate::TGetNodeAttrBatchResponse>
    GetNodeAttrBatch(NProtoPrivate::TGetNodeAttrBatchRequest request) override
    {
        return NotImplemented<NProtoPrivate::TGetNodeAttrBatchResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TGetNodeAttrResponse>
    GetNodeAttr(NProto::TGetNodeAttrRequest request) override
    {
        return NotImplemented<NProto::TGetNodeAttrResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TSetNodeAttrResponse>
    SetNodeAttr(NProto::TSetNodeAttrRequest request) override
    {
        return NotImplemented<NProto::TSetNodeAttrResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TAccessNodeResponse>
    AccessNode(NProto::TAccessNodeRequest request) override
    {
        return NotImplemented<NProto::TAccessNodeResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TCreateNodeResponse>
    CreateNode(NProto::TCreateNodeRequest request) override
    {
        return NotImplemented<NProto::TCreateNodeResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TUnlinkNodeResponse>
    UnlinkNode(NProto::TUnlinkNodeRequest request) override
    {
        return NotImplemented<NProto::TUnlinkNodeResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TCreateHandleResponse>
    CreateHandle(NProto::TCreateHandleRequest request) override
    {
        return NotImplemented<NProto::TCreateHandleResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TDestroyHandleResponse>
    DestroyHandle(NProto::TDestroyHandleRequest request) override
    {
        return NotImplemented<NProto::TDestroyHandleResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TAllocateDataResponse>
    AllocateData(NProto::TAllocateDataRequest request) override
    {
        return NotImplemented<NProto::TAllocateDataResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TAcquireLockResponse>
    AcquireLock(NProto::TAcquireLockRequest request) override
    {
        return NotImplemented<NProto::TAcquireLockResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TReleaseLockResponse>
    ReleaseLock(NProto::TReleaseLockRequest request) override
    {
        return NotImplemented<NProto::TReleaseLockResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TTestLockResponse>
    TestLock(NProto::TTestLockRequest request) override
    {
        return NotImplemented<NProto::TTestLockResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TWriteDataResponse>
    WriteData(NProto::TWriteDataRequest request) override
    {
        return NotImplemented<NProto::TWriteDataResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TReadDataResponse>
    ReadData(NProto::TReadDataRequest request) override
    {
        return NotImplemented<NProto::TReadDataResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TRemoveNodeXAttrResponse>
    RemoveNodeXAttr(NProto::TRemoveNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TRemoveNodeXAttrResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TGetNodeXAttrResponse>
    GetNodeXAttr(NProto::TGetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TGetNodeXAttrResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TSetNodeXAttrResponse>
    SetNodeXAttr(NProto::TSetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TSetNodeXAttrResponse>(
            std::move(request));
    }

    NThreading::TFuture<NProto::TListNodeXAttrResponse>
    ListNodeXAttr(NProto::TListNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TListNodeXAttrResponse>(
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateFileSystemShardStub()
{
    return std::make_shared<TFileSystemShardStub>();
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
