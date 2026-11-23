#pragma once

#include <cloud/blockstore/libs/icv2/data/endpoint.h>

#include <cloud/storage/core/libs/icv2/peer/peer_manager.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct THostDataEndpoint
{
    // Check connection status with Peer->GetStatus().
    NCloud::NIcV2::IPeerPtr Peer;
    IDataEndpointPtr Endpoint;
};

// Start the peer manager, scheduler and completion queue first. Use the
// manager's clock for timer and scheduler. Pass {} to omit notifications.
// The handler may run before this call returns.
//
// Reuse Endpoint for this host's disks. Transport failures return E_REJECTED.
// Wrap Endpoint in a durable client for retries. A retry may apply a write or
// zero twice.
//
// Call Endpoint->Stop() before stopping the peer manager. Then stop the shared
// scheduler and completion queue.
TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler,
    NCloud::NIcV2::IPeerManagerPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    TDataConfig dataConfig);

}   // namespace NCloud::NBlockStore::NIcV2
