#include "shard.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

class TMirrorUnsafeFileSystemShard: public IFileSystemShard
{
private:
    const ui32 ShardNo;
    const NProtoPrivate::TPersistentFastShardConfig Config;

public:
    TMirrorUnsafeFileSystemShard(
            ui32 shardNo,
            NProtoPrivate::TPersistentFastShardConfig config)
        : ShardNo(shardNo)
        , Config(std::move(config))
    {
        Y_UNUSED(ShardNo, Config);
    }

public:
    TFuture<NProtoPrivate::TGetNodeAttrBatchResponse>
    GetNodeAttrBatch(NProtoPrivate::TGetNodeAttrBatchRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TGetNodeAttrResponse>
    GetNodeAttr(NProto::TGetNodeAttrRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TSetNodeAttrResponse>
    SetNodeAttr(NProto::TSetNodeAttrRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TAccessNodeResponse>
    AccessNode(NProto::TAccessNodeRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TCreateNodeResponse>
    CreateNode(NProto::TCreateNodeRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TUnlinkNodeResponse>
    UnlinkNode(NProto::TUnlinkNodeRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TCreateHandleResponse>
    CreateHandle(NProto::TCreateHandleRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TDestroyHandleResponse>
    DestroyHandle(NProto::TDestroyHandleRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TAllocateDataResponse>
    AllocateData(NProto::TAllocateDataRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TAcquireLockResponse>
    AcquireLock(NProto::TAcquireLockRequest request) override
    {
        return NotImplemented<NProto::TAcquireLockResponse>(request);
    }

    TFuture<NProto::TReleaseLockResponse>
    ReleaseLock(NProto::TReleaseLockRequest request) override
    {
        return NotImplemented<NProto::TReleaseLockResponse>(request);
    }

    TFuture<NProto::TTestLockResponse>
    TestLock(NProto::TTestLockRequest request) override
    {
        return NotImplemented<NProto::TTestLockResponse>(request);
    }

    TFuture<NProto::TWriteDataResponse>
    WriteData(NProto::TWriteDataRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TReadDataResponse>
    ReadData(NProto::TReadDataRequest request) override
    {
        Y_UNUSED(request);
        return {};
    }

    TFuture<NProto::TRemoveNodeXAttrResponse>
    RemoveNodeXAttr(NProto::TRemoveNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TRemoveNodeXAttrResponse>(request);
    }

    TFuture<NProto::TGetNodeXAttrResponse>
    GetNodeXAttr(NProto::TGetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TGetNodeXAttrResponse>(request);
    }

    TFuture<NProto::TSetNodeXAttrResponse>
    SetNodeXAttr(NProto::TSetNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TSetNodeXAttrResponse>(request);
    }

    TFuture<NProto::TListNodeXAttrResponse>
    ListNodeXAttr(NProto::TListNodeXAttrRequest request) override
    {
        return NotImplemented<NProto::TListNodeXAttrResponse>(request);
    }

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> NotImplemented(const TRequest& request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return MakeFuture(std::move(response));
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateMirrorUnsafeFileSystemShard(
    ui32 shardNo,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    return std::make_shared<TMirrorUnsafeFileSystemShard>(shardNo, config);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
