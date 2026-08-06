#include "fsync_queue.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <library/cpp/threading/future/wait/wait.h>

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NVFS {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TFSyncQueue::TFSyncQueue(
        const TString& fileSystemId,
        ILoggingServicePtr logging)
    : FileSystemId(fileSystemId)
    , LogTag("[" + fileSystemId + "][FSYNC_QUEUE]")
    , Logging(std::move(logging))
    , Log(Logging->CreateLog("NFS_FUSE"))
{}

TFSyncQueue::TShard& TFSyncQueue::AccessShard(TNodeId nodeId)
{
    return Shards[ToUnderlying(nodeId) % ShardCount];
}

bool TFSyncQueue::IsFSync(const TItem& item)
{
    return item.Promise.Initialized();
}

bool TFSyncQueue::NotifyAndEraseLeadingFSyncs(TRequestMap& map)
{
    // Fires leading fsyncs in `map` (calls SetValue on their promises).
    // Returns true if the map is empty afterwards.
    // SetValue runs subscriber callbacks synchronously, so they execute
    // while the caller still holds the shard lock.
    while (!map.empty()) {
        auto it = map.begin();
        if (!IsFSync(it->second)) {
            break;
        }

        auto promise = std::move(it->second.Promise);
        map.erase(it);
        promise.SetValue(MakeError(S_OK));
    }

    return map.empty();
}

void TFSyncQueue::Enqueue(TRequestId reqId, TNodeId nodeId, THandle handle)
{
    STORAGE_VERIFY(nodeId, TWellKnownEntityTypes::FILESYSTEM, FileSystemId);

    TRequest request{.ReqId = reqId, .NodeId = nodeId, .Handle = handle};

    STORAGE_TRACE(LogTag << " Request was started " << request);

    auto& shard = AccessShard(nodeId);
    with_lock (shard.Lock) {
        TItem item{.Request = request};
        auto& metaMap = shard.Meta[nodeId];
        // There is no need to call NotifyAndEraseLeadingFSyncs here since the
        // head of any map (lowest reqId) is always non-fsync, because
        // Dequeue and WaitFor* drain leading fsyncs under the shard lock
        // before releasing it. A non-fsync insert cannot violate this. It
        // either becomes the new head (still non-fsync) or lands below the
        // existing head (head unchanged).
        STORAGE_VERIFY_DEBUG_C(
            metaMap.empty() || !IsFSync(metaMap.begin()->second),
            TWellKnownEntityTypes::FILESYSTEM,
            FileSystemId,
            TStringBuilder() << "FSync at head of meta map for nodeId="
                             << ToUnderlying(nodeId) << " before Enqueue");
        const bool metaEmplaced = metaMap.emplace(reqId, item).second;
        STORAGE_VERIFY_C(
            metaEmplaced,
            TWellKnownEntityTypes::FILESYSTEM,
            FileSystemId,
            TStringBuilder() << "Duplicate requestId: " << reqId
                             << " for nodeId: " << ToUnderlying(nodeId)
                             << " in local meta map");
        if (handle) {
            auto& dataMap = shard.Data[nodeId][handle];
            STORAGE_VERIFY_DEBUG_C(
                dataMap.empty() || !IsFSync(dataMap.begin()->second),
                TWellKnownEntityTypes::FILESYSTEM,
                FileSystemId,
                TStringBuilder() << "FSync at head of data map for nodeId="
                                 << ToUnderlying(nodeId) << " handle="
                                 << ToUnderlying(handle) << " before Enqueue");
            const bool dataEmplaced = dataMap.emplace(reqId, item).second;
            STORAGE_VERIFY_C(
                dataEmplaced,
                TWellKnownEntityTypes::FILESYSTEM,
                FileSystemId,
                TStringBuilder() << "Duplicate requestId: " << reqId
                                 << " for nodeId: " << ToUnderlying(nodeId)
                                 << " and handle: " << ToUnderlying(handle)
                                 << " in local data map");
        }
    }
}

void TFSyncQueue::Dequeue(
    TRequestId reqId,
    const NProto::TError& error,
    TNodeId nodeId,
    THandle handle)
{
    // TODO: Request can finish with error
    Y_UNUSED(error);
    STORAGE_VERIFY(nodeId, TWellKnownEntityTypes::FILESYSTEM, FileSystemId);

    TRequest request{.ReqId = reqId, .NodeId = nodeId, .Handle = handle};

    STORAGE_TRACE(LogTag << " Request was finished " << request);

    auto& shard = AccessShard(nodeId);
    with_lock (shard.Lock) {
        // Local meta map: always present.
        auto metaNodeIt = shard.Meta.find(nodeId);
        STORAGE_VERIFY(
            metaNodeIt != shard.Meta.end(),
            TWellKnownEntityTypes::FILESYSTEM,
            FileSystemId);
        const bool metaErased = metaNodeIt->second.erase(reqId);
        STORAGE_VERIFY_C(
            metaErased,
            TWellKnownEntityTypes::FILESYSTEM,
            FileSystemId,
            TStringBuilder() << "Cannot find requestId: " << reqId
                             << " for nodeId: " << ToUnderlying(nodeId)
                             << " in local meta map");
        if (NotifyAndEraseLeadingFSyncs(metaNodeIt->second)) {
            shard.Meta.erase(metaNodeIt);
        }

        if (handle) {
            // Local data map: present only for data requests.
            auto dataNodeIt = shard.Data.find(nodeId);
            STORAGE_VERIFY(
                dataNodeIt != shard.Data.end(),
                TWellKnownEntityTypes::FILESYSTEM,
                FileSystemId);
            auto handleIt = dataNodeIt->second.find(handle);
            STORAGE_VERIFY(
                handleIt != dataNodeIt->second.end(),
                TWellKnownEntityTypes::FILESYSTEM,
                FileSystemId);
            const bool dataErased = handleIt->second.erase(reqId);
            STORAGE_VERIFY_C(
                dataErased,
                TWellKnownEntityTypes::FILESYSTEM,
                FileSystemId,
                TStringBuilder() << "Cannot find requestId: " << reqId
                                 << " for nodeId: " << ToUnderlying(nodeId)
                                 << " and handle: " << ToUnderlying(handle)
                                 << " in local data map");
            if (NotifyAndEraseLeadingFSyncs(handleIt->second)) {
                dataNodeIt->second.erase(handleIt);
                if (dataNodeIt->second.empty()) {
                    shard.Data.erase(dataNodeIt);
                }
            }
        }
    }
}

TFuture<NProto::TError> TFSyncQueue::WaitForRequests(
    TRequestId reqId,
    TNodeId nodeId)
{
    TRequest request{.ReqId = reqId, .NodeId = nodeId};

    STORAGE_TRACE(
        LogTag << " FSync request was received " << request << " meta");

    if (!nodeId) {
        return WaitForGlobalMeta(reqId);
    }

    TFuture<NProto::TError> future;
    auto& shard = AccessShard(nodeId);
    with_lock (shard.Lock) {
        auto it = shard.Meta.find(nodeId);
        if (it == shard.Meta.end()) {
            return MakeFuture(MakeError(S_OK));
        }
        TItem item{
            .Request = request,
            .Promise = NewPromise<NProto::TError>(),
        };
        future = item.Promise.GetFuture();
        const bool emplaced = it->second.emplace(reqId, std::move(item)).second;
        STORAGE_VERIFY_C(
            emplaced,
            TWellKnownEntityTypes::FILESYSTEM,
            FileSystemId,
            TStringBuilder() << "Duplicate requestId: " << reqId
                             << " for nodeId: " << ToUnderlying(nodeId)
                             << " in local meta map");
        if (NotifyAndEraseLeadingFSyncs(it->second)) {
            shard.Meta.erase(it);
        }
    }
    return future;
}

TFuture<NProto::TError> TFSyncQueue::WaitForDataRequests(TRequestId reqId)
{
    return WaitForGlobalData(reqId);
}

TFuture<NProto::TError> TFSyncQueue::WaitForDataRequests(
    TRequestId reqId,
    TNodeId nodeId,
    THandle handle)
{
    TRequest request{.ReqId = reqId, .NodeId = nodeId, .Handle = handle};

    STORAGE_TRACE(
        LogTag << " FSync request was received " << request << " data");

    if (!nodeId) {
        return WaitForGlobalData(reqId);
    }

    TFuture<NProto::TError> future;
    auto& shard = AccessShard(nodeId);
    with_lock (shard.Lock) {
        auto nodeIt = shard.Data.find(nodeId);
        if (nodeIt == shard.Data.end()) {
            return MakeFuture(MakeError(S_OK));
        }

        auto& nodeMap = nodeIt->second;
        auto handleIt = nodeMap.find(handle);
        if (handleIt == nodeMap.end()) {
            return MakeFuture(MakeError(S_OK));
        }

        TItem item{
            .Request = request,
            .Promise = NewPromise<NProto::TError>(),
        };
        future = item.Promise.GetFuture();
        const bool emplaced =
            handleIt->second.emplace(reqId, std::move(item)).second;
        STORAGE_VERIFY_C(
            emplaced,
            TWellKnownEntityTypes::FILESYSTEM,
            FileSystemId,
            TStringBuilder() << "Duplicate requestId: " << reqId
                             << " for nodeId: " << ToUnderlying(nodeId)
                             << " and handle: " << ToUnderlying(handle)
                             << " in local data map");
        if (NotifyAndEraseLeadingFSyncs(handleIt->second)) {
            nodeMap.erase(handleIt);
            if (nodeMap.empty()) {
                shard.Data.erase(nodeIt);
            }
        }
    }
    return future;
}

TFuture<NProto::TError> TFSyncQueue::WaitForShardMeta(
    TShard& shard,
    TRequestId reqId)
{
    TVector<TFuture<NProto::TError>> leafFutures;
    with_lock (shard.Lock) {
        leafFutures.reserve(shard.Meta.size());
        // Insert a leaf into every per-node meta map in the shard; drain
        // leading fsyncs that may sit at the head (out-of-order reqIds from
        // multi-threaded FUSE loops would otherwise cause the per-shard fsync
        // to wait on semantically-later requests).
        EraseNodesIf(
            shard.Meta,
            [&](auto& kv)
            {
                auto& [nodeId, map] = kv;
                TItem item{
                    .Request = {.ReqId = reqId, .NodeId = nodeId},
                    .Promise = NewPromise<NProto::TError>(),
                };
                leafFutures.push_back(item.Promise.GetFuture());
                const bool emplaced = map.emplace(reqId, std::move(item)).second;
                STORAGE_VERIFY_C(
                    emplaced,
                    TWellKnownEntityTypes::FILESYSTEM,
                    FileSystemId,
                    TStringBuilder() << "Duplicate requestId: " << reqId
                                     << " for nodeId: " << ToUnderlying(nodeId)
                                     << " in shard meta map");
                return NotifyAndEraseLeadingFSyncs(map);
            });
    }
    return WaitAll(leafFutures)
        .Apply([](const auto&) { return MakeError(S_OK); });
}

TFuture<NProto::TError> TFSyncQueue::WaitForShardData(
    TShard& shard,
    TRequestId reqId)
{
    TVector<TFuture<NProto::TError>> leafFutures;
    with_lock (shard.Lock) {
        EraseNodesIf(
            shard.Data,
            [&](auto& nodeKv)
            {
                auto& [nodeId, perHandle] = nodeKv;
                EraseNodesIf(
                    perHandle,
                    [&](auto& handleKv)
                    {
                        auto& [handle, map] = handleKv;
                        TItem item{
                            .Request =
                                {.ReqId = reqId,
                                 .NodeId = nodeId,
                                 .Handle = handle},
                            .Promise = NewPromise<NProto::TError>(),
                        };
                        leafFutures.push_back(item.Promise.GetFuture());
                        const bool emplaced =
                            map.emplace(reqId, std::move(item)).second;
                        STORAGE_VERIFY_C(
                            emplaced,
                            TWellKnownEntityTypes::FILESYSTEM,
                            FileSystemId,
                            TStringBuilder()
                                << "Duplicate requestId: " << reqId
                                << " for nodeId: " << ToUnderlying(nodeId)
                                << " and handle: " << ToUnderlying(handle)
                                << " in shard data map");
                        return NotifyAndEraseLeadingFSyncs(map);
                    });
                return perHandle.empty();
            });
    }
    return WaitAll(leafFutures)
        .Apply([](const auto&) { return MakeError(S_OK); });
}

TFuture<NProto::TError> TFSyncQueue::WaitForGlobalMeta(TRequestId reqId)
{
    // Each shard scan is the global fsync's ordering point for that shard.
    // Requests that enter an already-scanned shard later are concurrent from
    // this queue's point of view even if their fuse_req_unique is lower than
    // reqId.
    TVector<TFuture<NProto::TError>> shardFutures;
    shardFutures.reserve(Shards.size());
    for (auto& shard: Shards) {
        shardFutures.push_back(WaitForShardMeta(shard, reqId));
    }
    return WaitAll(shardFutures)
        .Apply([](const auto&) { return MakeError(S_OK); });
}

TFuture<NProto::TError> TFSyncQueue::WaitForGlobalData(TRequestId reqId)
{
    TVector<TFuture<NProto::TError>> shardFutures;
    shardFutures.reserve(Shards.size());
    for (auto& shard: Shards) {
        shardFutures.push_back(WaitForShardData(shard, reqId));
    }
    return WaitAll(shardFutures)
        .Apply([](const auto&) { return MakeError(S_OK); });
}

////////////////////////////////////////////////////////////////////////////////

void TFSyncQueueStub::Enqueue(TRequestId reqId, TNodeId nodeId, THandle handle)
{
    Y_UNUSED(reqId);
    Y_UNUSED(nodeId);
    Y_UNUSED(handle);
}

void TFSyncQueueStub::Dequeue(
    TRequestId reqId,
    const NProto::TError& error,
    TNodeId nodeId,
    THandle handle)
{
    Y_UNUSED(reqId);
    Y_UNUSED(error);
    Y_UNUSED(nodeId);
    Y_UNUSED(handle);
}

TFuture<NProto::TError> TFSyncQueueStub::WaitForRequests(
    TRequestId reqId,
    TNodeId nodeId)
{
    Y_UNUSED(reqId);
    Y_UNUSED(nodeId);
    return MakeFuture(MakeError(S_OK));
}

TFuture<NProto::TError> TFSyncQueueStub::WaitForDataRequests(TRequestId reqId)
{
    Y_UNUSED(reqId);
    return MakeFuture(MakeError(S_OK));
}

TFuture<NProto::TError> TFSyncQueueStub::WaitForDataRequests(
    TRequestId reqId,
    TNodeId nodeId,
    THandle handle)
{
    Y_UNUSED(reqId);
    Y_UNUSED(nodeId);
    Y_UNUSED(handle);
    return MakeFuture(MakeError(S_OK));
}

}   // namespace NCloud::NFileStore::NVFS

template <>
inline void Out<NCloud::NFileStore::NVFS::IFSyncQueue::TRequest>(
    IOutputStream& out,
    const NCloud::NFileStore::NVFS::IFSyncQueue::TRequest& request)
{
    out << "#" << ToUnderlying(request.NodeId) << " @"
        << ToUnderlying(request.Handle) << " id:" << request.ReqId;
}
