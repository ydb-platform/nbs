#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

NCloud::IStatsHandlerPtr CreateTcMallocStatsHandler(
    NMonitoring::TDynamicCountersPtr rootCounters);

}   // namespace NCloud::NFileStore
