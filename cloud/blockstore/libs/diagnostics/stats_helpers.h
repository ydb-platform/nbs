#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/request_counters.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TRequestCounters MakeRequestCounters(
    ITimerPtr timer,
    TRequestCounters::EOptions options);

}   // namespace NCloud::NBlockStore
