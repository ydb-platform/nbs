#pragma once

#include "request_client.h"

#include <cloud/blockstore/libs/icv2/data/endpoint.h>

namespace NCloud::NBlockStore::NIcV2 {

struct THostDataEndpoint
{
    IPeerPtr Peer;
    IDataEndpointPtr Endpoint;
    // Wait after endpoint destruction before stopping the result executor.
    NThreading::TFuture<void> Closed;
};

// The caller selects one host; the manager returns its peer handle.
// Endpoint destruction unregisters only this binding.
// Connection loss, TCP errors and EOF during hello become E_UNAVAILABLE;
// attempt deadlines remain E_TIMEOUT, admission overload is E_REJECTED and
// owner shutdown is E_CANCELLED. Protocol/argument errors remain fatal.
// Storage response errors (including flags) are passed through unchanged.
// Retries belong to the NBS durable client. A lost write/zero response leaves
// the outcome unknown: a retry can execute the operation again.
// IC V2 failures do not switch this binding to gRPC. Automatic fallback and
// recovery would need an explicit policy in the cell data router.
TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    TString host,
    ui16 port,
    IClientEndpointHandlerPtr handler,
    IPeerManagerPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ITaskQueuePtr completions,
    TDataConfig dataConfig);

}   // namespace NCloud::NBlockStore::NIcV2
