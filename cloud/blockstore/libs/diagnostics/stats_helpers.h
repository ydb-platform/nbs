#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/request_counters.h>
#include <cloud/storage/core/libs/diagnostics/histogram_counter_options.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TRequestCounters MakeRequestCounters(
    ITimerPtr timer,
    TRequestCounters::EOptions options,
    EHistogramCounterOptions histogramCounterOptions);

}   // namespace NCloud::NBlockStore
