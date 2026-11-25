#pragma once

#include "service.h"

#include <cloud/storage/core/libs/icv2/tcp/listener_private.h>

namespace NCloud::NBlockStore::NIcV2::NPrivate {

////////////////////////////////////////////////////////////////////////////////

// Inject listener failures without exhausting process-wide resources.
TResultOrError<IServicePtr> CreateService(
    NCells::TCellsConfigPtr config,
    IBlockStorePtr dataService,
    NCloud::NIcV2::IAddressResolverPtr resolver,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    NMonitoring::TDynamicCountersPtr counters,
    NCloud::NIcV2::NPrivate::IListenerErrorInjectorPtr listenerErrorInjector);

}   // namespace NCloud::NBlockStore::NIcV2::NPrivate
