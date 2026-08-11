#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void SetVolumeCriticalEventsReportingMode(
    NProto::EVolumeCriticalEventsReportingMode reportingMode);

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);
void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

NCloud::IStatsHandlerPtr CreateCriticalEventsStatsHandler();

// For unit test purposes
void ResetVolumeCriticalEventsCounter();

}   // namespace NCloud::NBlockStore
