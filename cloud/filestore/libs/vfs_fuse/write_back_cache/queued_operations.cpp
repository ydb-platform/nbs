#include "queued_operations.h"
#include "write_data_request.h"

#include <variant>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TWriteDataPromiseCompletedEvent
{
    NThreading::TPromise<NProto::TWriteDataResponse> Promise;

    void Invoke()
    {
        Promise.SetValue({});
    }
};

struct TWriteDataPromiseFailedEvent
{
    NThreading::TPromise<NProto::TWriteDataResponse> Promise;
    NCloud::NProto::TError Error;

    void Invoke()
    {
        NProto::TWriteDataResponse response;
        *response.MutableError() = std::move(Error);
        Promise.SetValue(std::move(response));
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
    std::function<void()> requestsSerializedCallback)
    : Processor(processor)
    , RequestsSerializedCallback(std::move(requestsSerializedCallback))
{}

TQueuedOperations::~TQueuedOperations() = default;

void TQueuedOperations::Acquire()
{
    Lock.Acquire();
}

void TQueuedOperations::Release()
{
    while (true) {
        auto events = std::exchange(Events, {});
        auto requestsToSerialize = std::exchange(RequestsToSerialize, {});

        Lock.Release();

        for (auto& event: events) {
            std::visit([](auto& ev) { ev.Invoke(); }, event);
        }

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

        Lock.Acquire();

        // This call may enqueue new events
        RequestsSerializedCallback();
    }
}

void TQueuedOperations::ScheduleFlushNode(ui64 nodeId)
{
    Events.push_back(TScheduleFlushEvent{Processor, nodeId});
}

void TQueuedOperations::CompleteWriteDataPromise(
    NThreading::TPromise<NProto::TWriteDataResponse> promise)
{
    Events.push_back(TWriteDataPromiseCompletedEvent{std::move(promise)});
}

void TQueuedOperations::FailWriteDataPromise(
    NThreading::TPromise<NProto::TWriteDataResponse> promise,
    const NCloud::NProto::TError& error)
{
    Events.push_back(TWriteDataPromiseFailedEvent{std::move(promise), error});
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
