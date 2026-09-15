/*******************************************************************************

This file is intended for initialization only and must not be included in
critical event reporting to avoid potential PEERDIR cyclic dependencies.

*******************************************************************************/

#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Start buffering AppCriticalEvents, AppImpossibleEvents and
// DiskAgentCriticalEvents.
// Call before startup work to retain event counts until monitoring is ready.
void InitProcessCriticalEventsReporting();

void InitVolumeCriticalEventsReportingMode(
    NProto::EVolumeCriticalEventsReportingMode reportingMode);

// Attach the process monitoring root and choose the counter type at startup.
// With RATE, add buffered counts to the cumulative counters immediately.
// With GAUGE, retain them until the first interval publication after
// initialization.
// On repeated GAUGE initialization, discard pending process counts and reset
// published counts if reusing the same root.
// Note: start buffering (InitProcessCriticalEventsReporting()) before selecting
// GAUGE.
void InitCriticalEventsCounter(
    NMonitoring::TDynamicCountersPtr counters,
    bool derivative = true);

// Attach the volume monitoring root and preserve startup counts on the first
// call. On a repeated call, discard pending volume counts and reset published
// counts if reusing the same root.
void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters);

NCloud::IStatsHandlerPtr CreateCriticalEventsStatsHandler();

// Clear pending events and roots and restore default reporting for tests.
void ResetCriticalEventsCounter();

}   // namespace NCloud::NBlockStore
