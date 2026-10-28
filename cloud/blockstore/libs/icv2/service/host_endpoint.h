#pragma once

#include "request_client.h"

#include <cloud/blockstore/libs/icv2/data/data.h>

namespace NCloud::NBlockStore::NIcV2 {

struct THostDataEndpoint
{
    IDataEndpointPtr Endpoint;
    // Retained by the service even after the endpoint is destroyed: a result
    // queued on its executor must finish before that executor can stop.
    NThreading::TFuture<void> Closed;
};

// The caller resolves exactly one host and assigns a unique, never reused
// binding ID. Endpoint destruction unregisters only this binding.
// Connection loss, TCP errors and EOF during hello become E_UNAVAILABLE;
// attempt deadlines remain E_TIMEOUT, admission overload is E_REJECTED and
// owner shutdown is E_CANCELLED. Protocol/argument errors remain fatal.
// Storage response errors (including flags) are passed through unchanged.
// Retries belong to the NBS durable client. A lost write/zero response leaves
// the outcome unknown: a retry can execute the operation again.
// IC V2 failures do not switch this binding to gRPC. Automatic fallback and
// recovery would need an explicit policy in the cell data router.
TResultOrError<THostDataEndpoint> CreateHostDataEndpoint(
    const TString& cellId,
    ui64 bindingId,
    TVector<NInterconnect::TAddress> addresses,
    IPeerConnectionsPtr peers,
    ITimerPtr timer,
    ISchedulerPtr scheduler, ITaskQueuePtr completions, TDataConfig dataConfig);

}   // namespace NCloud::NBlockStore::NIcV2
