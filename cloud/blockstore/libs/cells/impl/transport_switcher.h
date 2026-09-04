#pragma once

#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/public.h>

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
    TDuration InitialRetryDelay = TDuration::Seconds(1);
    TDuration MaxRetryDelay = TDuration::Seconds(30);
};

using TEndpointFactory =
    std::function<NThreading::TFuture<TResultOrError<IBlockStorePtr>>()>;

// Asks the factory for the endpoint of the preferred transport and switches
// the router over to it as soon as one is ready, retrying with a growing delay
// until it succeeds. Gives up for good once the router is gone, so a released
// connection stops the retries by itself.
//
// Switching is currently one-way and happens once: nothing here watches the
// transport afterwards. Health driven switching in both directions is meant to
// replace this driver, which is why the router itself carries no such
// assumption.
void StartTransportSwitching(
    IEndpointRouterPtr router,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString host,
    TTransportSwitcherConfig config);

}   // namespace NCloud::NBlockStore::NCells
