/*******************************************************************************

This file is intended for initialization only and must not be included in
critical event reporting to avoid potential PEERDIR cyclic dependencies.

*******************************************************************************/

#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Enable interval counters for AppCriticalEvents and AppImpossibleEvents.
// Call before startup work so events are retained before monitoring is ready.
void InitAppCriticalEventsReporting();

void InitVolumeCriticalEventsReportingMode(
    NProto::EVolumeCriticalEventsReportingMode reportingMode);

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);
void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

NCloud::IStatsHandlerPtr CreateCriticalEventsStatsHandler();

// Clear pending events and roots and restore default reporting for tests.
void ResetCriticalEventsCounter();

}   // namespace NCloud::NBlockStore
