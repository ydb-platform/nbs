#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

ITraceProcessorPtr CreateTraceProcessorMon(
    IMonitoringServicePtr monitoring, ITraceProcessorPtr impl);

ITraceProcessorPtr CreateTraceProcessorMon(
    IMonitoringServicePtr monitoring,
    ITraceProcessorPtr impl, TString traceViewerUrlTemplate);

}   // namespace NCloud
