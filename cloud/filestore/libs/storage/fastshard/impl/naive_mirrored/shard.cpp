#include "shard.h"

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>

#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFiberShardImpl
{
private:
    const ui32 ShardNo;
    const NProtoPrivate::TPersistentFastShardConfig Config;

    IStorageGroupPtr Storage;

public:
    TFiberShardImpl(
            ui32 shardNo,
            NProtoPrivate::TPersistentFastShardConfig config)
        : ShardNo(shardNo)
        , Config(std::move(config))
    {
        Y_UNUSED(ShardNo);

        //
        // Overall it's better to pass the group into this code via dependency
        // injection but for now it's not necessary.
        //
        // Using only one storage group for now.
        //

        TVector<IStorageNodePtr> nodes;
        const auto& sg = Config.GetStorageGroups(0);
        for (const auto& d: sg.GetDevices()) {
            nodes.push_back(CreateStorageNodeClient(d.GetHost(), d.GetPort()));
        }
        Storage = CreateNaiveMirroredStorageGroup(std::move(nodes));
    }

public:
    NProtoPrivate::TGetNodeAttrBatchResponse
    GetNodeAttrBatch(NProtoPrivate::TGetNodeAttrBatchRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TGetNodeAttrResponse
    GetNodeAttr(NProto::TGetNodeAttrRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TSetNodeAttrResponse
    SetNodeAttr(NProto::TSetNodeAttrRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TAccessNodeResponse
    AccessNode(NProto::TAccessNodeRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TCreateNodeResponse
    CreateNode(NProto::TCreateNodeRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TUnlinkNodeResponse
    UnlinkNode(NProto::TUnlinkNodeRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TCreateHandleResponse
    CreateHandle(NProto::TCreateHandleRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TDestroyHandleResponse
    DestroyHandle(NProto::TDestroyHandleRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TAllocateDataResponse
    AllocateData(NProto::TAllocateDataRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TAcquireLockResponse
    AcquireLock(NProto::TAcquireLockRequest request)
    {
        return NotImplemented<NProto::TAcquireLockResponse>(std::move(request));
    }

    NProto::TReleaseLockResponse
    ReleaseLock(NProto::TReleaseLockRequest request)
    {
        return NotImplemented<NProto::TReleaseLockResponse>(std::move(request));
    }

    NProto::TTestLockResponse
    TestLock(NProto::TTestLockRequest request)
    {
        return NotImplemented<NProto::TTestLockResponse>(std::move(request));
    }

    NProto::TWriteDataResponse WriteData(NProto::TWriteDataRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TReadDataResponse ReadData(NProto::TReadDataRequest request)
    {
        Y_UNUSED(request);
        return {};
    }

    NProto::TRemoveNodeXAttrResponse
    RemoveNodeXAttr(NProto::TRemoveNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TRemoveNodeXAttrResponse>(request);
    }

    NProto::TGetNodeXAttrResponse
    GetNodeXAttr(NProto::TGetNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TGetNodeXAttrResponse>(request);
    }

    NProto::TSetNodeXAttrResponse
    SetNodeXAttr(NProto::TSetNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TSetNodeXAttrResponse>(request);
    }

    NProto::TListNodeXAttrResponse
    ListNodeXAttr(NProto::TListNodeXAttrRequest request)
    {
        return NotImplemented<NProto::TListNodeXAttrResponse>(request);
    }

private:
    template <typename TResponse, typename TRequest>
    TResponse NotImplemented(TRequest request)
    {
        Y_UNUSED(request);

        TResponse response;
        *response.MutableError() = MakeError(E_NOT_IMPLEMENTED);
        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define FAST_SHARD_DEFINE_METHOD(name, ns, ...)                                \
    struct TFiberShard##name##Params                                           \
    {                                                                          \
        std::shared_ptr<TFiberShardImpl> FiberShard;                           \
        std::shared_ptr<ns::T##name##Request> Request;                         \
        NThreading::TPromise<ns::T##name##Response> Promise;                   \
    };                                                                         \
                                                                               \
    int name##FiberMain(TFiberShard##name##Params* params) noexcept            \
    {                                                                          \
        auto response = params->FiberShard->name(std::move(*params->Request)); \
        params->Promise.SetValue(std::move(response));                         \
        return 0;                                                              \
    }                                                                          \
// FAST_SHARD_DEFINE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_DEFINE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_DEFINE_METHOD, NProto)

#undef FAST_SHARD_DEFINE_METHOD

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NThreading;

class TNaiveMirroredFileSystemShard: public IFileSystemShard
{
private:
    std::shared_ptr<TFiberShardImpl> FiberShard;

public:
    TNaiveMirroredFileSystemShard(
            ui32 shardNo,
            NProtoPrivate::TPersistentFastShardConfig config)
        : FiberShard(
            std::make_shared<TFiberShardImpl>(shardNo, std::move(config)))
    {
    }

public:
#define FAST_SHARD_DEFINE_METHOD(name, ns, ...)                                \
    NThreading::TFuture<ns::T##name##Response> name(                           \
        ns::T##name##Request request) override                                 \
    {                                                                          \
        auto promise = NThreading::NewPromise<ns::T##name##Response>();        \
        auto future = promise.GetFuture();                                     \
                                                                               \
        int r = silk::FiberScheduler::run(                                     \
            name##FiberMain,                                                   \
            TFiberShard##name##Params{                                         \
                .FiberShard = FiberShard,                                      \
                .Request =                                                     \
                    std::make_shared<ns::T##name##Request>(std::move(request)),\
                .Promise = promise,                                            \
            },                                                                 \
            nullptr /* future */);                                             \
        if (r) {                                                               \
            ns::T##name##Response response;                                    \
            *response.MutableError() = MakeError(E_FAIL, TStringBuilder()      \
                << "failed to spawn fiber: " << ::strerror(r));                \
            promise.SetValue(std::move(response));                             \
        }                                                                      \
                                                                               \
        return future;                                                         \
    }                                                                          \
// FAST_SHARD_DEFINE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_DEFINE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_DEFINE_METHOD, NProto)

#undef FAST_SHARD_DEFINE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

IFileSystemShardPtr CreateNaiveMirroredFileSystemShard(
    ui32 shardNo,
    const NProtoPrivate::TPersistentFastShardConfig& config)
{
    return std::make_shared<TNaiveMirroredFileSystemShard>(shardNo, config);
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
