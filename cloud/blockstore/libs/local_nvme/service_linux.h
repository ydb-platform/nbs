#pragma once

#include "public.h"

#include "private.h"
#include "service.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor,
    ISysFsPtr sysFs);

}   // namespace NCloud::NBlockStore
