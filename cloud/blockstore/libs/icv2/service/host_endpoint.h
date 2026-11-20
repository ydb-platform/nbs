#pragma once

#include <cloud/blockstore/libs/icv2/data/endpoint.h>

#include <cloud/storage/core/libs/icv2/peer/peer_manager.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct THostDataEndpoint
{
    NCloud::NIcV2::IPeerPtr Peer;
    IDataEndpointPtr Endpoint;
    // Wait after endpoint destruction before stopping the result executor.
    NThreading::TFuture<void> Closed;
};

// The caller selects one host; the manager returns its peer handle.
// Endpoint destruction closes this peer.
// Network failures, E_BADMSG, and requests interrupted by a channel protocol
// error return E_UNAVAILABLE. Attempt deadlines return E_TIMEOUT, admission
// overload returns E_REJECTED, and owner shutdown returns E_CANCELLED.
// Other storage error codes, messages, and flags pass through.
// Retries belong to the NBS durable client. A lost write/zero response leaves
// the outcome unknown: a retry can execute the operation again.
// IC V2 failures do not switch this binding to gRPC. Automatic fallback and
// recovery would need an explicit policy in the cell data router.
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
