#pragma once

#include "peer_connections.h"
#include "service.h"

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TPingClientConfig
{
    ui32 MaxPendingRequests = 1024;
    ui64 MaxPayloadBytes = 64 * 1024 * 1024;
};

// Internal owner of Ping deadlines and completions. Shares a single connection
// subscription per peer and a single scheduled timeout callback for all calls.
// Peers, scheduler and executor belong to the caller and must outlive Stop.
// Start once after the owner is ready. RequestStop is callback-safe; external
// Stop waits for admitted work and continuations. The owner must call Stop
// before releasing its last reference, outside result continuations.
struct IPingClient: public IStartable
{
    virtual TPingFuture
    Ping(TString cellId, TString payload, TDuration timeout) = 0;
    virtual void RequestStop() = 0;
};

using IPingClientPtr = std::shared_ptr<IPingClient>;

TResultOrError<IPingClientPtr> CreatePingClient(
    TPingClientConfig config,
    IPeerConnectionsPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions);

}   // namespace NCloud::NBlockStore::NIcV2
