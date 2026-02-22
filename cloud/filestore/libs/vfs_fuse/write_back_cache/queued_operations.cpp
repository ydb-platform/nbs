#include "queued_operations.h"

#include <variant>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

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
    const NCloud::NProto::TError& Error;

    void Invoke()
    {
        NProto::TWriteDataResponse response;
        *response.MutableError() = Error;
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
    const NCloud::NProto::TError& Error;

    void Invoke()
    {
        Promise.SetValue(Error);
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
    TScheduleFlushEvent>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TQueuedOperations::TEvent: public TEventVariant
{
    using TEventVariant::TEventVariant;
};

////////////////////////////////////////////////////////////////////////////////

TQueuedOperations::TQueuedOperations(IQueuedOperationsProcessor& processor)
    : Processor(processor)
{}

TQueuedOperations::~TQueuedOperations() = default;

void TQueuedOperations::Acquire()
{
    Lock.Acquire();
}

void TQueuedOperations::Release()
{
    auto events = std::exchange(Events, {});
    Lock.Release();
    for (auto event: events) {
        std::visit([](auto& ev) { ev.Invoke(); }, event);
    }
}

void TQueuedOperations::ScheduleFlushNode(ui64 nodeId)
{
    Events.push_back(TScheduleFlushEvent{Processor, nodeId});
}

void TQueuedOperations::Complete(
    NThreading::TPromise<NProto::TWriteDataResponse> promise)
{
    Events.push_back(TWriteDataPromiseCompletedEvent{std::move(promise)});
}

void TQueuedOperations::Fail(
    NThreading::TPromise<NProto::TWriteDataResponse> promise,
    const NCloud::NProto::TError& error)
{
    Events.push_back(TWriteDataPromiseFailedEvent{std::move(promise), error});
}

void TQueuedOperations::Complete(
    NThreading::TPromise<NCloud::NProto::TError> promise)
{
    Events.push_back(TFlushOrReleasePromiseCompletedEvent{std::move(promise)});
}

void TQueuedOperations::Fail(
    NThreading::TPromise<NCloud::NProto::TError> promise,
    const NCloud::NProto::TError& error)
{
    Events.push_back(
        TFlushOrReleasePromiseFailedEvent{std::move(promise), error});
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
