#include "fsync_queue.h"

#include <util/generic/algorithm.h>

namespace NCloud::NFileStore::NVFS {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TFSyncCache::TFSyncCache(TString logTag, ILoggingServicePtr logging)
    : LogTag(std::move(logTag))
    , Logging(std::move(logging))
    // TODO: add vfs log component
    , Log(Logging->CreateLog("NFS_FUSE"))
{}

void TFSyncCache::AddRequest(const TRequest& request)
{
    Y_ABORT_UNLESS(request.NodeId);

    TItem item{ .Request = request };

    // Meta request.
    Meta[request.NodeId].emplace(request.ReqId, item);
    GlobalMeta.emplace(request.ReqId, item);

    STORAGE_TRACE(LogTag << " Request was added to meta maps " << request);

    if (request.Handle) {
        // Data request.
        Data[request.NodeId][request.Handle].emplace(request.ReqId, item);
        GlobalData.emplace(request.ReqId, item);

        STORAGE_TRACE(LogTag << " Request was added to data maps " << request);
    }

    CheckFSyncNotifications();
}

TFuture<NProto::TError> TFSyncCache::AddFSyncRequest(const TRequest& request)
{
    TItem item{ .Request = request, .Promise = NewPromise<NProto::TError>() };
    auto future = item.Promise.GetFuture();

    if (request.NodeId) {
        if (request.Handle) {
            // Node Data request.
            Data[request.NodeId][request.Handle]
                .emplace(request.ReqId, std::move(item));

            STORAGE_TRACE(LogTag
                << " FSync request was added to local data map " << request);
        } else {
            // Node Meta request.
            Meta[request.NodeId].emplace(request.ReqId, std::move(item));

            STORAGE_TRACE(LogTag
                << " FSync request was added to local meta map " << request);
        }
    } else {
        if (request.Handle) {
            // Global Data request.
            GlobalData.emplace(request.ReqId, std::move(item));

            STORAGE_TRACE(LogTag
                << " FSync request was added to global data map " << request);
        } else {
            // Global Meta request.
            GlobalMeta.emplace(request.ReqId, std::move(item));

            STORAGE_TRACE(LogTag
                << " FSync request was added to global meta map " << request);
        }
    }

    CheckFSyncNotifications();
    return future;
}

void TFSyncCache::RemoveRequest(const TRequest& request)
{
    Y_ABORT_UNLESS(request.NodeId);

    {
        // Meta request.
        auto nodeIt = Meta.find(request.NodeId);
        Y_ABORT_UNLESS(nodeIt != Meta.end());

        if (!nodeIt->second.erase(request.ReqId)) {
            Y_ABORT(
                "Cannot find requestId: %lu for nodeId: %lu in local meta map",
                request.ReqId,
                ToUnderlying(request.NodeId));
        }

        if (nodeIt->second.empty()) {
            Meta.erase(nodeIt);
        }

        STORAGE_TRACE(LogTag
            << " Request was removed from local meta map " << request);

        // Global meta request.
        if (!GlobalMeta.erase(request.ReqId)) {
            Y_ABORT(
                "Cannot find requestId: %lu in global meta map",
                request.ReqId);
        }

        STORAGE_TRACE(LogTag
            << " Request was removed from global meta map " << request);
    }

    if (request.Handle) {
        // Data request.
        auto nodeIt = Data.find(request.NodeId);
        Y_ABORT_UNLESS(nodeIt != Data.end());

        auto handleIt = nodeIt->second.find(request.Handle);
        Y_ABORT_UNLESS(handleIt != nodeIt->second.end());

        if (!handleIt->second.erase(request.ReqId)) {
            Y_ABORT(
                "Cannot find requestId: %lu for nodeId: %lu and handle: %lu"
                " in local data map",
                request.ReqId,
                ToUnderlying(request.NodeId),
                ToUnderlying(request.Handle));
        }

        if (handleIt->second.empty()) {
            nodeIt->second.erase(handleIt);
            if (nodeIt->second.empty()) {
                Data.erase(nodeIt);
            }
        }

        STORAGE_TRACE(LogTag
            << " Request was removed from local data map " << request);

        // Global data request.
        if (!GlobalData.erase(request.ReqId)) {
            Y_ABORT(
                "Cannot find requestId: %lu in global data map",
                request.ReqId);
        }

        STORAGE_TRACE(LogTag
            << " Request was removed from global data map " << request);
    }

    CheckFSyncNotifications();
}

void TFSyncCache::CheckFSyncNotifications()
{
    const auto notifyAndErase = [this] (auto& kv) {
        return NotifyAndEraseLatest(kv.second);
    };

    // Check in node meta map.
    EraseNodesIf(Meta, notifyAndErase);

    // Check in global meta map.
    NotifyAndEraseLatest(GlobalMeta);

    // Check in node data map.
    EraseNodesIf(Data, [&] (auto& kv) {
        auto& map = kv.second;
        EraseNodesIf(map, notifyAndErase);
        return map.empty();
    });

    // Check in global data map.
    NotifyAndEraseLatest(GlobalData);
}

bool TFSyncCache::NotifyAndEraseLatest(TRequestMap& map)
{
    while (!map.empty()) {
        auto it = map.begin();
        if (!IsFSync(it->second)) {
            break;
        }

        auto item = std::move(it->second);
        map.erase(it);

        Notify(item.Request, std::move(item.Promise));
    }

    return map.empty();
}

void TFSyncCache::Notify(
    const TRequest& request,
    TPromise<NProto::TError>&& promise)
{
    // TODO: Fsync or flush can finish not only with success
    // https://man7.org/linux/man-pages/man2/fsync.2.html
    // https://man7.org/linux/man-pages/man3/fflush.3.html
    promise.SetValue({});

    STORAGE_TRACE(LogTag
        << " FSync request was notified and erased " << request);
}

bool TFSyncCache::IsFSync(const TItem& item) const
{
    return item.Promise.Initialized();
}

////////////////////////////////////////////////////////////////////////////////

TFSyncQueue::TFSyncQueue(
        const TString& fileSystemId,
        ILoggingServicePtr logging)
    : LogTag("[" + fileSystemId + "][FSYNC]")
    , Logging(std::move(logging))
    , Log(Logging->CreateLog("NFS_FUSE"))
    , CurrentState(LogTag, Logging)
{}

void TFSyncQueue::Enqueue(TRequestId reqId, TNodeId nodeId, THandle handle)
{
    TRequest request{ .ReqId = reqId, .NodeId = nodeId, .Handle = handle };

    STORAGE_TRACE(LogTag << " Request was started " << request);

    with_lock (StateLock) {
        CurrentState.AddRequest(request);
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

    TRequest request{ .ReqId = reqId, .NodeId = nodeId, .Handle = handle };

    STORAGE_TRACE(LogTag << " Request was finished " << request);

    with_lock (StateLock) {
        CurrentState.RemoveRequest(request);
    }
}

TFuture<NProto::TError> TFSyncQueue::WaitForRequests(
    TRequestId reqId,
    TNodeId nodeId)
{
    TRequest request{ .ReqId = reqId, .NodeId = nodeId };

    STORAGE_TRACE(LogTag
        << " FSync request was received " << request << " meta");

    with_lock (StateLock) {
        return CurrentState.AddFSyncRequest(request);
    }
}

TFuture<NProto::TError> TFSyncQueue::WaitForDataRequests(TRequestId reqId)
{
    // Handle should not be equal to InvalidHandle for global data fsync.
    return WaitForDataRequests(reqId, TNodeId {InvalidNodeId}, THandle {~InvalidHandle});
}

TFuture<NProto::TError> TFSyncQueue::WaitForDataRequests(
    TRequestId reqId,
    TNodeId nodeId,
    THandle handle)
{
    TRequest request{ .ReqId = reqId, .NodeId = nodeId, .Handle = handle };

    STORAGE_TRACE(LogTag
        << " FSync request was received " << request << " data");

    with_lock (StateLock) {
        return CurrentState.AddFSyncRequest(request);
    }
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
inline void Out<NCloud::NFileStore::NVFS::TFSyncCache::TRequest>(
    IOutputStream& out,
    const NCloud::NFileStore::NVFS::TFSyncCache::TRequest& request)
{
    out
        << "#" << ToUnderlying(request.NodeId)
        << " @" << ToUnderlying(request.Handle)
        << " id:" << request.ReqId;
}
