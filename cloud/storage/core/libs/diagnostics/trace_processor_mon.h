#pragma once

#include "public.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

ITraceProcessorPtr CreateTraceProcessorMon(
    IMonitoringServicePtr monitoring,
    ITraceProcessorPtr impl);

}   // namespace NCloud
