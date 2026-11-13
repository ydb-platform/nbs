#pragma once

#include "public.h"

#include "peer_connections.h"

#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/icv2/iface/data_endpoint_factory.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/library/actors/interconnect/address/interconnect_address.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TPingResponse
{
    TString PeerCellId;
    TString Payload;
    // Includes connection waiting and completion queue time, before
    // continuations.
    TDuration Duration;
};

using TPingResult = TResultOrError<TPingResponse>;
using TPingFuture = NThreading::TFuture<TPingResult>;

struct IService
    : public IStartable
    , public IDataEndpointFactory
{
    // The factory binds the listener without invoking callbacks. Start once
    // after the owner is ready. Stop is external-thread-only and idempotent:
    // joins accept/hello workers, closes channels, joins the IC V2 engine
    // and waits for result continuations before stopping their executor.
    virtual const NInterconnect::TAddress& GetAddress() const = 0;

    // A terminal listener failure closes admission and existing connections.
    // The error remains available until destruction; recovery requires a new
    // service. Healthy/stopped-without-error services return an empty error.
    virtual NProto::TError GetError() const = 0;

    // Callback-safe: closes admission and requests shutdown without joining
    // workers or waiting for continuations. External Stop completes shutdown.
    virtual void RequestStop() = 0;

    // One deadline covers connection waiting, hello, sending and the response.
    // Timeout checks use a 10 ms tick. A timeout does not cancel
    // remote execution of an already admitted send. Requests are never resent.
    // MaxPendingRequests also limits outstanding Ping calls;
    // MaxOutputQueueBytes limits their payload bytes and the transport's
    // shared output queue across all connections.
    // Admitted results are completed on the service's completion executor.
    // Continuations must not throw, call Stop or destroy the service; external
    // Stop waits for them. Calls before Start/after stopping return E_REJECTED.
    virtual TPingFuture Ping(TString cellId, TString payload,
                             TDuration timeout) = 0;
};

// Requires CellsEnabled and IcV2.Enabled. Allowed peer identities come from
// the existing cells configuration. Fails startup if IC V2 is unavailable.
TResultOrError<IServicePtr> CreateService(
    NCells::TCellsConfigConstPtr config, IBlockStorePtr dataService,
    ITimerPtr timer, ISchedulerPtr scheduler, ILoggingServicePtr logging,
    NMonitoring::TDynamicCountersPtr counters);

}   // namespace NCloud::NBlockStore::NIcV2
