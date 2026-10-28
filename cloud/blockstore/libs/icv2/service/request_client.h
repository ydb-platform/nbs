#pragma once

#include "peer_connections.h"

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TRequestClientConfig
{
    ui32 MaxPendingRequests = 1024;
    ui64 MaxPayloadBytes = 64 * 1024 * 1024;
};

struct TRequestClientResponse
{
    std::shared_ptr<NActors::IEventHandle> Event;
    std::weak_ptr<IRequestChannel> Channel;
};

using TRequestClientResult = TResultOrError<TRequestClientResponse>;
using TRequestClientFuture = NThreading::TFuture<TRequestClientResult>;
using TChannelProvider = std::function<TChannelFuture(const TString&)>;

// Shared deadline/admission owner for requests waiting for a connection.
// One subscription per key, one timer for all requests. Never resends events.
// Results and their continuations run on completions and stay in its budget.
// Provider and completions must remain operational through external Stop.
// Calls outside the running lifetime and unpublished results at shutdown get
// E_CANCELLED. Admission overload gets E_REJECTED.
struct IRequestClient: public IStartable
{
    virtual TRequestClientFuture Request(
        TString key,
        std::unique_ptr<NActors::IEventBase> event,
        TInstant deadline, ui64 payloadBytes) = 0;
    virtual void RequestStop() = 0;
    virtual NThreading::TFuture<void> WhenClosed() const = 0;
};

using IRequestClientPtr = std::shared_ptr<IRequestClient>;

TResultOrError<IRequestClientPtr> CreateRequestClient(
    TRequestClientConfig config, TChannelProvider provider, ITimerPtr timer,
    ISchedulerPtr scheduler, ITaskQueuePtr completions);

}   // namespace NCloud::NBlockStore::NIcV2
