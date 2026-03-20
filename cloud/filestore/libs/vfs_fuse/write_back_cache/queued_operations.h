#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

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
    IQueuedOperationsProcessor& Processor;

public:
    explicit TQueuedOperations(IQueuedOperationsProcessor& processor);
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
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
