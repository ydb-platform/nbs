#pragma once

#include "write_back_cache_lock.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>

#include <functional>
#include <memory>

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

    std::unique_ptr<IWriteBackCacheLock> Lock;
    TVector<TEvent> Events;

    // Non-owning pointers. Each request stays alive until it publishes
    // Serialized. Another thread may then destroy it, the serializer must not
    // dereference its pointer after publication.
    TVector<TPendingWriteDataRequest*> RequestsToSerialize;
    IQueuedOperationsProcessor& Processor;

    // Invoked under Lock after queued WriteData requests are serialized
    const std::function<void()> RequestsSerializedCallback;

public:
    TQueuedOperations(
        IQueuedOperationsProcessor& processor,
        std::unique_ptr<IWriteBackCacheLock> lock,
        std::function<void()> requestsSerializedCallback);

    ~TQueuedOperations();

    void Acquire() noexcept;

    // Called from TGuard's destructor through noexcept lock operations.
    // Event handlers (including synchronous promise subscribers) and
    // RequestsSerializedCallback must not throw; an escaping exception calls
    // std::terminate.
    void Release();

    void ScheduleFlushNode(ui64 nodeId);

    // Complete a promise associated with a WriteData request and delete the
    // request. The request must be unlinked from all lists.
    void CompleteWriteDataRequest(
        std::unique_ptr<TPendingWriteDataRequest> request);

    // Fail the promise with the supplied error and delete the WriteData
    // request. The request must be unlinked from all lists.
    void FailWriteDataRequest(
        std::unique_ptr<TPendingWriteDataRequest> request,
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
