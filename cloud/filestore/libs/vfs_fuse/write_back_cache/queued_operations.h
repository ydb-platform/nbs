#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <functional>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TPendingWriteDataRequest;

////////////////////////////////////////////////////////////////////////////////

struct IQueuedOperationsProcessor
{
    virtual ~IQueuedOperationsProcessor() = default;

    virtual void ScheduleFlushNode(ui64 nodeId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Execute queued operations outside lock
class TQueuedOperations
{
private:
    struct TEvent;

    TAdaptiveLock Lock;
    TVector<TEvent> Events;

    // Non-owning pointers. The cache-state lifetime invariant keeps allocated
    // requests alive until this serialization batch completes.
    TVector<TPendingWriteDataRequest*> RequestsToSerialize;
    IQueuedOperationsProcessor& Processor;

    // Invoked under Lock after queued WriteData requests are serialized
    const std::function<void()> RequestsSerializedCallback;

public:
    TQueuedOperations(
        IQueuedOperationsProcessor& processor,
        std::function<void()> requestsSerializedCallback);

    ~TQueuedOperations();

    void Acquire();
    void Release();

    void ScheduleFlushNode(ui64 nodeId);

    void CompleteWriteDataPromise(
        NThreading::TPromise<NProto::TWriteDataResponse> promise);

    void FailWriteDataPromise(
        NThreading::TPromise<NProto::TWriteDataResponse> promise,
        const NCloud::NProto::TError& error);

    void CompleteFlushOrReleasePromise(
        NThreading::TPromise<NCloud::NProto::TError> promise);

    void FailFlushOrReleasePromise(
        NThreading::TPromise<NCloud::NProto::TError> promise,
        const NCloud::NProto::TError& error);

    void CompleteAcquireBarrierPromise(
        NThreading::TPromise<TResultOrError<ui64>> promise,
        ui64 barrierId);

    void FailAcquireBarrierPromise(
        NThreading::TPromise<TResultOrError<ui64>> promise,
        const NCloud::NProto::TError& error);

    void SerializeWriteDataRequest(TPendingWriteDataRequest* request);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
