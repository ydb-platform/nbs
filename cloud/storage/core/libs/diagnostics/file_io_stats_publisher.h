#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// Periodically publishes the statistics of the file I/O backend instances
// registered in `registry` to the `component=file_io` subgroup of `counters`
IStatsHandlerPtr CreateFileIOStatsPublisher(
    ITimerPtr timer,
    TFileIOStatsRegistryPtr registry,
    NMonitoring::TDynamicCountersPtr counters);

}   // namespace NCloud
