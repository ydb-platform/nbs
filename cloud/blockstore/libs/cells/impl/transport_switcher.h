#pragma once

#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/endpoint_handler.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <functional>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct TTransportSwitcherConfig
{
    TDuration SettleTime = TDuration::Seconds(10);
};

using TEndpointFactory = std::function<TResultOrError<IBlockStorePtr>(
    NCloud::IClientEndpointHandlerPtr)>;

////////////////////////////////////////////////////////////////////////////////

// Decides which transport the router points at, for as long as the connection
// lives. Data starts on the endpoint the router was created with and moves onto
// the preferred transport once it has connected and stayed connected for
// SettleTime; a break moves the data straight back.
struct ITransportSwitcher
{
    virtual ~ITransportSwitcher() = default;

    virtual NCloud::IClientEndpointHandlerPtr GetEndpointHandler() = 0;

    // Enables router updates after the prepared binding has been installed.
    virtual void Activate() = 0;

    // whether the data goes over the preferred transport now, rather than
    // the fallback
    virtual bool IsPreferredActive() = 0;
};

using ITransportSwitcherPtr = std::shared_ptr<ITransportSwitcher>;

// Creates the preferred endpoint without changing the router. Until Activate,
// endpoint callbacks only record its connection state.
TResultOrError<ITransportSwitcherPtr> PrepareTransportSwitching(
    ITransportTargetPtr target,
    IBlockStorePtr fallback,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString endpointName,
    TTransportSwitcherConfig config);

}   // namespace NCloud::NBlockStore::NCells
