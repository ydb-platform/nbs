#pragma once

#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/rdma/iface/client.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <functional>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct TTransportSwitcherConfig
{
    TDuration InitialRetryDelay = TDuration::Seconds(1);
    TDuration MaxRetryDelay = TDuration::Seconds(30);
    TDuration SettleTime = TDuration::Seconds(30);
};

// The handler has to reach the rdma client, and only the switcher can make it,
// so the factory is handed one rather than capturing it.
using TEndpointFactory = std::function<NThreading::TFuture<
    TResultOrError<IBlockStorePtr>>(
    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr)>;

////////////////////////////////////////////////////////////////////////////////

// Decides which transport the router points at, for as long as the connection
// lives. Data starts on the endpoint the router was created with and moves onto
// the preferred transport once the factory has produced it and it has stayed
// connected for SettleTime; a break moves the data straight back.
//
// Immediate on the way down and deliberate on the way up: at a break there is
// nothing to wait for since the requests are already aborted, while returning
// on the first reconnect would keep feeding a flapping link.
//
// The handler from GetEndpointHandler() must be given to the rdma client so it
// reports the endpoint state. It holds the switcher weakly, so the switcher's
// life is bound by whoever owns it and not by the endpoint.
struct ITransportSwitcher
{
    virtual ~ITransportSwitcher() = default;

    virtual NCloud::NStorage::NRdma::IClientEndpointHandlerPtr
        GetEndpointHandler() = 0;
};

using ITransportSwitcherPtr = std::shared_ptr<ITransportSwitcher>;

ITransportSwitcherPtr StartTransportSwitching(
    IEndpointRouterPtr router,
    IBlockStorePtr fallback,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString host,
    TTransportSwitcherConfig config);

}   // namespace NCloud::NBlockStore::NCells
