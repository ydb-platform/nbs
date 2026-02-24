#include "queued_operations.h"

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

struct TFlushPromiseCompletedEvent
{
    NThreading::TPromise<void> Promise;

    void Invoke()
    {
        Promise.SetValue();
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
    TFlushPromiseCompletedEvent,
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
    for (auto& event: events) {
        std::visit([](auto& ev) { ev.Invoke(); }, event);
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

void TQueuedOperations::CompleteFlushPromise(NThreading::TPromise<void> promise)
{
    Events.push_back(TFlushPromiseCompletedEvent{std::move(promise)});
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
