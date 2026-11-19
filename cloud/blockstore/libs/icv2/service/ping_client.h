#pragma once

#include "public.h"

#include "service.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TPingPeerAddress
{
    TString Host;
    ui16 Port;
};

struct TPingClientConfig
{
    ui32 MaxPendingRequests = 1024;
    ui64 MaxPayloadBytes = 64 * 1024 * 1024;
    THashMap<TString, TVector<TPingPeerAddress>> Peers;
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
    IPeerManagerPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions);

}   // namespace NCloud::NBlockStore::NIcV2
