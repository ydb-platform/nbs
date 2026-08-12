/*******************************************************************************

This file is intended for initialization only and must not be included in
critical event reporting to avoid potential PEERDIR cyclic dependencies.

*******************************************************************************/

#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void InitVolumeCriticalEventsReportingMode(
    NProto::EVolumeCriticalEventsReportingMode reportingMode);

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);
void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

NCloud::IStatsHandlerPtr CreateCriticalEventsStatsHandler();

// For unit test purposes
void ResetVolumeCriticalEventsCounter();

}   // namespace NCloud::NBlockStore
