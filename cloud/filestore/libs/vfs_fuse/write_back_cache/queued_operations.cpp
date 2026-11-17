#include "queued_operations.h"
#include "write_data_request.h"

#include <variant>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

void ValidateRetiredRequest(const TPendingWriteDataRequest& request)
{
    Y_ABORT_UNLESS(
        request.TIntrusiveListItem<TPendingWriteDataRequest>::Empty());

    Y_ABORT_UNLESS(
        (request.TIntrusiveListItem<TPendingWriteDataRequest, THandleStateTag>::
             Empty()));

    // Allocated requests must publish Serialized before retirement
    Y_ABORT_UNLESS(
        !request.HasAllocation() || request.IsSerialized(),
        "HasAllocation=%d, IsSerialized=%d",
        request.HasAllocation(),
        request.IsSerialized());
}

struct TWriteDataPromiseCompletedEvent
{
    std::unique_ptr<TPendingWriteDataRequest> Request;

    void Invoke()
    {
        auto promise = std::move(Request->AccessPromise());

        // SetValue runs subscribers synchronously; they may reply to FUSE and
        // submit more writes. Release request-owned memory first instead of
        // retaining it through those callbacks and the rest of the event batch.
        Request.reset();
        promise.SetValue({});
    }
};

struct TWriteDataPromiseFailedEvent
{
    std::unique_ptr<TPendingWriteDataRequest> Request;
    NCloud::NProto::TError Error;

    void Invoke()
    {
        NProto::TWriteDataResponse response;
        *response.MutableError() = std::move(Error);
        auto promise = std::move(Request->AccessPromise());
        // Free request-owned memory before subscribers can reply or retry.
        Request.reset();
        promise.SetValue(std::move(response));
    }
};

struct TFlushOrReleasePromiseCompletedEvent
{
    NThreading::TPromise<NCloud::NProto::TError> Promise;

    void Invoke()
    {
        Promise.SetValue({});
    }
};

struct TFlushOrReleasePromiseFailedEvent
{
    NThreading::TPromise<NCloud::NProto::TError> Promise;
    NCloud::NProto::TError Error;

    void Invoke()
    {
        Promise.SetValue(std::move(Error));
    }
};

struct TAcquireBarrierPromiseCompletedEvent
{
    NThreading::TPromise<TResultOrError<ui64>> Promise;
    ui64 BarrierId;

    void Invoke()
    {
        Promise.SetValue(BarrierId);
    }
};

struct TAcquireBarrierPromiseFailedEvent
{
    NThreading::TPromise<TResultOrError<ui64>> Promise;
    NCloud::NProto::TError Error;

    void Invoke()
    {
        Promise.SetValue(std::move(Error));
    }
};

struct TScheduleFlushEvent
{
    IQueuedOperationsProcessor& Processor;
    ui64 NodeId;

    void Invoke()
    {
        Processor.ScheduleFlushNode(NodeId);
    }
};

using TEventVariant = std::variant<
    TWriteDataPromiseCompletedEvent,
    TWriteDataPromiseFailedEvent,
    TFlushOrReleasePromiseCompletedEvent,
    TFlushOrReleasePromiseFailedEvent,
    TAcquireBarrierPromiseCompletedEvent,
    TAcquireBarrierPromiseFailedEvent,
    TScheduleFlushEvent>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TQueuedOperations::TEvent: public TEventVariant
{
    using TEventVariant::TEventVariant;
};

////////////////////////////////////////////////////////////////////////////////

TQueuedOperations::TQueuedOperations(
    IQueuedOperationsProcessor& processor,
    std::unique_ptr<IWriteBackCacheLock> lock,
    std::function<void()> requestsSerializedCallback)
    : Lock(std::move(lock))
    , Processor(processor)
    , RequestsSerializedCallback(std::move(requestsSerializedCallback))
{
    Y_ABORT_UNLESS(Lock);
}

TQueuedOperations::~TQueuedOperations() = default;

void TQueuedOperations::Acquire() noexcept
{
    Lock->Acquire();
}

void TQueuedOperations::Release()
{
    while (true) {
        auto events = std::exchange(Events, {});
        auto requestsToSerialize = std::exchange(RequestsToSerialize, {});

        Lock->Release();

        for (auto& event: events) {
            std::visit([](auto& ev) { ev.Invoke(); }, event);
        }

        // Release event-held state while unlocked
        events.clear();

        if (requestsToSerialize.empty()) {
            break;
        }

        // Process events first because they may complete futures and unblock
        // requests. Serialization is CPU-intensive, so postponing it minimizes
        // completion latency and lets us respond quickly.
        //
        // Serialization failures are fatal: requests have already been
        // validated, so any failure indicates an invariant violation.
        for (auto& request: requestsToSerialize) {
            request->SerializeToAllocation();
        }

        Lock->Acquire();

        // This call may enqueue new events
        RequestsSerializedCallback();
    }
}

void TQueuedOperations::ScheduleFlushNode(ui64 nodeId)
{
    Events.push_back(TScheduleFlushEvent{Processor, nodeId});
}

void TQueuedOperations::CompleteWriteDataRequest(
    std::unique_ptr<TPendingWriteDataRequest> request)
{
    ValidateRetiredRequest(*request);

    Events.push_back(TWriteDataPromiseCompletedEvent{std::move(request)});
}

void TQueuedOperations::FailWriteDataRequest(
    std::unique_ptr<TPendingWriteDataRequest> request,
    const NCloud::NProto::TError& error)
{
    ValidateRetiredRequest(*request);

    Events.push_back(TWriteDataPromiseFailedEvent{std::move(request), error});
}

void TQueuedOperations::CompleteFlushOrReleasePromise(
    NThreading::TPromise<NCloud::NProto::TError> promise)
{
    Events.push_back(TFlushOrReleasePromiseCompletedEvent{std::move(promise)});
}

void TQueuedOperations::FailFlushOrReleasePromise(
    NThreading::TPromise<NCloud::NProto::TError> promise,
    const NCloud::NProto::TError& error)
{
    Events.push_back(
        TFlushOrReleasePromiseFailedEvent{std::move(promise), error});
}

void TQueuedOperations::CompleteAcquireBarrierPromise(
    NThreading::TPromise<TResultOrError<ui64>> promise,
    ui64 barrierId)
{
    Events.push_back(
        TAcquireBarrierPromiseCompletedEvent{std::move(promise), barrierId});
}

void TQueuedOperations::FailAcquireBarrierPromise(
    NThreading::TPromise<TResultOrError<ui64>> promise,
    const NCloud::NProto::TError& error)
{
    Events.push_back(
        TAcquireBarrierPromiseFailedEvent{std::move(promise), error});
}

void TQueuedOperations::SerializeWriteDataRequest(
    TPendingWriteDataRequest* request)
{
    RequestsToSerialize.push_back(request);
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
