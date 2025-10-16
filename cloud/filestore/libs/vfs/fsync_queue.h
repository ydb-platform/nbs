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
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NVFS {

////////////////////////////////////////////////////////////////////////////////

using TNodeId = TScopedHandle<ui64, InvalidNodeId, struct TNodeIdTag>;
using THandle = TScopedHandle<ui64, InvalidHandle, struct THandleTag>;

////////////////////////////////////////////////////////////////////////////////

class TFSyncCache
{
public:
    using TRequestId = ui64;

    struct TRequest
    {
        TRequestId ReqId = 0;
        TNodeId NodeId;
        THandle Handle;
    };

private:
    struct TItem
    {
        TRequest Request;
        NThreading::TPromise<NProto::TError> Promise = {};
    };

    using TRequestMap = TMap<TRequestId, TItem>;

    using TMetaMap = THashMap<TNodeId, TRequestMap>;
    using TDataMap = THashMap<TNodeId, THashMap<THandle, TRequestMap>>;

private:
    const TString LogTag;
    const ILoggingServicePtr Logging;
    TLog Log;

    TMetaMap Meta;
    TRequestMap GlobalMeta;

    TDataMap Data;
    TRequestMap GlobalData;

public:
    TFSyncCache(TString logTag, ILoggingServicePtr logging);

    void AddRequest(const TRequest& request);
    NThreading::TFuture<NProto::TError> AddFSyncRequest(const TRequest& request);

    void RemoveRequest(const TRequest& request);

private:
    void CheckFSyncNotifications();
    bool NotifyAndEraseLatest(TRequestMap& map);
    void Notify(
        const TRequest& request,
        NThreading::TPromise<NProto::TError>&& promise);

    bool IsFSync(const TItem& item) const;
};

struct IFSyncQueue
{
    using TRequestId = TFSyncCache::TRequestId;
    using TRequest = TFSyncCache::TRequest;

    virtual ~IFSyncQueue() = default;

    virtual void Enqueue(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle = {}) = 0;
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
    virtual NThreading::TFuture<NProto::TError> WaitForDataRequests(TRequestId reqId) = 0;
    virtual NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle) = 0;
};

class TFSyncQueue final
    : public IFSyncQueue
{
    using TRequestId = TFSyncCache::TRequestId;
    using TRequest = TFSyncCache::TRequest;

private:
    const TString LogTag;
    const ILoggingServicePtr Logging;
    TLog Log;

    TFSyncCache CurrentState;
    TAdaptiveLock StateLock;

public:
    TFSyncQueue(const TString& fileSystemId, ILoggingServicePtr logging);

    void Enqueue(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle = {}) override;
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
    NThreading::TFuture<NProto::TError> WaitForDataRequests(TRequestId reqId) override;
    NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle) override;
};

class TFSyncQueueStub final
    : public IFSyncQueue
{
    using TRequestId = TFSyncCache::TRequestId;
    using TRequest = TFSyncCache::TRequest;

public:
    void Enqueue(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle = {}) override;
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
    NThreading::TFuture<NProto::TError> WaitForDataRequests(TRequestId reqId) override;
    NThreading::TFuture<NProto::TError> WaitForDataRequests(
        TRequestId reqId,
        TNodeId nodeId,
        THandle handle) override;
};

}   // namespace NCloud::NFileStore::NVFS
