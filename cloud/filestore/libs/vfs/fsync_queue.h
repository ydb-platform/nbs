#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <array>

namespace NCloud::NFileStore::NVFS {

////////////////////////////////////////////////////////////////////////////////

using TNodeId = TScopedHandle<ui64, InvalidNodeId, struct TNodeIdTag>;
using THandle = TScopedHandle<ui64, InvalidHandle, struct THandleTag>;

////////////////////////////////////////////////////////////////////////////////

class IFSyncQueue
{
public:
    using TRequestId = ui64;

    struct TRequest
    {
        TRequestId ReqId = 0;
        TNodeId NodeId;
        THandle Handle;
    };

    virtual ~IFSyncQueue() = default;

    virtual void
    Enqueue(TRequestId reqId, TNodeId nodeId, THandle handle = {}) = 0;
    virtual void Dequeue(
        TRequestId reqId,
        const NProto::TError& error,
        TNodeId nodeId,
        THandle handle = {}) = 0;

    // Meta requests.
    virtual NThreading::TFuture<NProto::TError> WaitForRequests(
        TRequestId reqId,
        TNodeId nodeId = {}) = 0;

    // Data requests.
    virtual NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId) = 0;
    virtual NThreading::TFuture<NProto::TError>
    WaitForDataRequests(TRequestId reqId, TNodeId nodeId, THandle handle) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFSyncQueue final: public IFSyncQueue
{
public:
    using TRequestId = IFSyncQueue::TRequestId;
    using TRequest = IFSyncQueue::TRequest;

private:
    // Sharding by NodeId. Hot-path Enqueue/Dequeue and per-node WaitFor*
    // acquire exactly one shard lock; no global lock exists. Global fsync
    // (WaitFor*(reqId) without nodeId) fans out across all shards.
    //
    // Enqueue is the ordering admission point. ReqIds order requests that
    // have already reached this queue, but multi-queue virtiofs assigns
    // fuse_req_unique before dispatching across queues, so lower unique ids
    // can be admitted to this code after higher ones by a different loop
    // thread. The reqId-keyed TMap re-sorts them. Global fsync therefore
    // waits for the per-map state visible when each shard is scanned; it
    // is not a global fuse_req_unique resequencer.
    static constexpr ui32 ShardCount = 16;

    struct TItem
    {
        TRequest Request;
        NThreading::TPromise<NProto::TError> Promise = {};
    };

    using TRequestMap = TMap<TRequestId, TItem>;

    struct alignas(64) TShard
    {
        TAdaptiveLock Lock;
        THashMap<TNodeId, TRequestMap> Meta;
        THashMap<TNodeId, THashMap<THandle, TRequestMap>> Data;
    };

    const TString LogTag;
    const ILoggingServicePtr Logging;
    TLog Log;

    std::array<TShard, ShardCount> Shards;

public:
    TFSyncQueue(const TString& fileSystemId, ILoggingServicePtr logging);

    void
    Enqueue(TRequestId reqId, TNodeId nodeId, THandle handle = {}) override;
    void Dequeue(
        TRequestId reqId,
        const NProto::TError& error,
        TNodeId nodeId,
        THandle handle = {}) override;

    // Meta requests.
    NThreading::TFuture<NProto::TError> WaitForRequests(
        TRequestId reqId,
        TNodeId nodeId = {}) override;

    // Data requests.
    NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId) override;
    NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle) override;

private:
    TShard& GetShard(TNodeId nodeId);

    static bool IsFSync(const TItem& item);

    static bool NotifyAndEraseLatest(TRequestMap& map);

    NThreading::TFuture<NProto::TError> WaitForGlobalMeta(TRequestId reqId);
    NThreading::TFuture<NProto::TError> WaitForGlobalData(TRequestId reqId);
};

////////////////////////////////////////////////////////////////////////////////

class TFSyncQueueStub final: public IFSyncQueue
{
public:
    void
    Enqueue(TRequestId reqId, TNodeId nodeId, THandle handle = {}) override;
    void Dequeue(
        TRequestId reqId,
        const NProto::TError& error,
        TNodeId nodeId,
        THandle handle = {}) override;

    // Meta requests.
    NThreading::TFuture<NProto::TError> WaitForRequests(
        TRequestId reqId,
        TNodeId nodeId = {}) override;

    // Data requests.
    NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId) override;
    NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle) override;
};

}   // namespace NCloud::NFileStore::NVFS
