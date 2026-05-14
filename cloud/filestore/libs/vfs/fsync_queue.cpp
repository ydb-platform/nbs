#include "fsync_queue.h"

#include <util/generic/algorithm.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NVFS {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Completion tracker for one fanned-out global fsync. The originating
// WaitFor* inserts one fsync leaf per non empty shard map (keyed by
// nodeId for meta, (nodeId, handle) for data). Each fired leaf decrements
// Remaining. Promise fires when it reaches zero.
struct TGlobalFSyncState
{
    std::atomic<size_t> Remaining{0};
    TPromise<NProto::TError> Promise;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFSyncQueue::TFSyncQueue(
        const TString& fileSystemId,
        ILoggingServicePtr logging)
    : LogTag("[" + fileSystemId + "][FSYNC]")
    , Logging(std::move(logging))
    , Log(Logging->CreateLog("NFS_FUSE"))
{}

TFSyncQueue::TShard& TFSyncQueue::GetShard(TNodeId nodeId)
{
    return Shards[ToUnderlying(nodeId) % ShardCount];
}

bool TFSyncQueue::IsFSync(const TItem& item)
{
    return item.Promise.Initialized();
}

bool TFSyncQueue::NotifyAndEraseLatest(TRequestMap& map)
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
        promise.SetValue({});
    }
    return map.empty();
}

void TFSyncQueue::Enqueue(TRequestId reqId, TNodeId nodeId, THandle handle)
{
    Y_ABORT_UNLESS(nodeId);

    TRequest request{.ReqId = reqId, .NodeId = nodeId, .Handle = handle};

    STORAGE_TRACE(LogTag << " Request was started " << request);

    auto& shard = GetShard(nodeId);
    with_lock (shard.Lock) {
        TItem item{.Request = request};
        auto& metaMap = shard.Meta[nodeId];
        // There is no need to call NotifyAndEraseLatest here since the
        // head of any map (lowest reqId) is always non-fsync, because
        // Dequeue and WaitFor* drain leading fsyncs under the shard lock
        // before releasing it. A non-fsync insert cannot violate this. It
        // either becomes the new head (still non-fsync) or lands below the
        // existing head (head unchanged).
        Y_DEBUG_ABORT_UNLESS(
            metaMap.empty() || !IsFSync(metaMap.begin()->second),
            "FSync at head of meta map for nodeId=%lu before Enqueue",
            ToUnderlying(nodeId));
        metaMap.emplace(reqId, item);
        if (handle) {
            auto& dataMap = shard.Data[nodeId][handle];
            Y_DEBUG_ABORT_UNLESS(
                dataMap.empty() || !IsFSync(dataMap.begin()->second),
                "FSync at head of data map for nodeId=%lu handle=%lu"
                " before Enqueue",
                ToUnderlying(nodeId),
                ToUnderlying(handle));
            dataMap.emplace(reqId, item);
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
    Y_ABORT_UNLESS(nodeId);

    TRequest request{.ReqId = reqId, .NodeId = nodeId, .Handle = handle};

    STORAGE_TRACE(LogTag << " Request was finished " << request);

    auto& shard = GetShard(nodeId);
    with_lock (shard.Lock) {
        // Local meta map: always present.
        auto metaNodeIt = shard.Meta.find(nodeId);
        Y_ABORT_UNLESS(metaNodeIt != shard.Meta.end());
        if (!metaNodeIt->second.erase(reqId)) {
            Y_ABORT(
                "Cannot find requestId: %lu for nodeId: %lu in local meta map",
                reqId,
                ToUnderlying(nodeId));
        }
        if (NotifyAndEraseLatest(metaNodeIt->second)) {
            shard.Meta.erase(metaNodeIt);
        }

        if (handle) {
            // Local data map: present only for data requests.
            auto dataNodeIt = shard.Data.find(nodeId);
            Y_ABORT_UNLESS(dataNodeIt != shard.Data.end());
            auto handleIt = dataNodeIt->second.find(handle);
            Y_ABORT_UNLESS(handleIt != dataNodeIt->second.end());
            if (!handleIt->second.erase(reqId)) {
                Y_ABORT(
                    "Cannot find requestId: %lu for nodeId: %lu and handle: %lu"
                    " in local data map",
                    reqId,
                    ToUnderlying(nodeId),
                    ToUnderlying(handle));
            }
            if (NotifyAndEraseLatest(handleIt->second)) {
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

    TItem item{
        .Request = request,
        .Promise = NewPromise<NProto::TError>(),
    };
    auto future = item.Promise.GetFuture();

    auto& shard = GetShard(nodeId);
    with_lock (shard.Lock) {
        auto& map = shard.Meta[nodeId];
        map.emplace(reqId, std::move(item));
        if (NotifyAndEraseLatest(map)) {
            shard.Meta.erase(nodeId);
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

    TItem item{
        .Request = request,
        .Promise = NewPromise<NProto::TError>(),
    };
    auto future = item.Promise.GetFuture();

    auto& shard = GetShard(nodeId);
    with_lock (shard.Lock) {
        auto& nodeMap = shard.Data[nodeId];
        auto& map = nodeMap[handle];
        map.emplace(reqId, std::move(item));
        if (NotifyAndEraseLatest(map)) {
            nodeMap.erase(handle);
            if (nodeMap.empty()) {
                shard.Data.erase(nodeId);
            }
        }
    }
    return future;
}

TFuture<NProto::TError> TFSyncQueue::WaitForGlobalMeta(TRequestId reqId)
{
    // Decompose the global meta fsync into one leaf fsync per non-empty
    // (shard, nodeId) meta map. The shared promise fires when every leaf has
    // fired. A +1 placeholder prevents premature firing during fan-out.
    //
    // Each shard scan is the global fsync's ordering point for that shard.
    // Requests that enter an already scanned shard later are concurrent from
    // this queue's point of view even if their fuse_req_unique is lower than
    // reqId.
    auto state = std::make_shared<TGlobalFSyncState>();
    state->Promise = NewPromise<NProto::TError>();
    state->Remaining.store(1, std::memory_order_relaxed);
    auto future = state->Promise.GetFuture();

    TVector<TFuture<NProto::TError>> leafFutures;

    for (auto& shard: Shards) {
        with_lock (shard.Lock) {
            // Insert a leaf into each non-empty meta map; immediately drain
            // any leading fsyncs from that map. The leaf may land at the
            // head (out-of-order reqIds from multi-threaded FUSE loops) and
            // must fire right away — otherwise the global fsync would wait
            // for higher-id requests that semantically came *after* it.
            // Leaf SetValue here only marks the future as complete; the
            // decrement-Remaining callback is attached later in the
            // Subscribe loop and will run inline at that point.
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
                    state->Remaining.fetch_add(1, std::memory_order_relaxed);
                    map.emplace(reqId, std::move(item));
                    return NotifyAndEraseLatest(map);
                });
        }
    }

    for (auto& f: leafFutures) {
        f.Subscribe(
            [state](const auto&)
            {
                if (state->Remaining.fetch_sub(1, std::memory_order_acq_rel) ==
                    1) {
                    state->Promise.SetValue({});
                }
            });
    }

    // Release the placeholder; fires the promise immediately if no shards
    // had pending requests.
    if (state->Remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        state->Promise.SetValue({});
    }
    return future;
}

TFuture<NProto::TError> TFSyncQueue::WaitForGlobalData(TRequestId reqId)
{
    // Same contract as WaitForGlobalMeta: each shard is ordered at the moment
    // it is scanned, not by a single global fuse_req_unique barrier.
    auto state = std::make_shared<TGlobalFSyncState>();
    state->Promise = NewPromise<NProto::TError>();
    state->Remaining.store(1, std::memory_order_relaxed);
    auto future = state->Promise.GetFuture();

    TVector<TFuture<NProto::TError>> leafFutures;

    for (auto& shard: Shards) {
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
                                    {
                                        .ReqId = reqId,
                                        .NodeId = nodeId,
                                        .Handle = handle,
                                    },
                                .Promise = NewPromise<NProto::TError>(),
                            };
                            leafFutures.push_back(item.Promise.GetFuture());
                            state->Remaining.fetch_add(
                                1,
                                std::memory_order_relaxed);
                            map.emplace(reqId, std::move(item));
                            return NotifyAndEraseLatest(map);
                        });
                    return perHandle.empty();
                });
        }
    }

    for (auto& f: leafFutures) {
        f.Subscribe(
            [state](const auto&)
            {
                if (state->Remaining.fetch_sub(1, std::memory_order_acq_rel) ==
                    1) {
                    state->Promise.SetValue({});
                }
            });
    }

    if (state->Remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        state->Promise.SetValue({});
    }
    return future;
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
    return MakeFuture<NProto::TError>();
}

TFuture<NProto::TError> TFSyncQueueStub::WaitForDataRequests(TRequestId reqId)
{
    Y_UNUSED(reqId);
    return MakeFuture<NProto::TError>();
}

TFuture<NProto::TError> TFSyncQueueStub::WaitForDataRequests(
    TRequestId reqId,
    TNodeId nodeId,
    THandle handle)
{
    Y_UNUSED(reqId);
    Y_UNUSED(nodeId);
    Y_UNUSED(handle);
    return MakeFuture<NProto::TError>();
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
