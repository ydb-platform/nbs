#include "critical_events.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
#define FILESTORE_INIT_CRITICAL_EVENT_COUNTER(name)                            \
    *counters->GetCounter("AppCriticalEvents/"#name, true) = 0;                \
// FILESTORE_INIT_CRITICAL_EVENT_COUNTER

    FILESTORE_CRITICAL_EVENTS(FILESTORE_INIT_CRITICAL_EVENT_COUNTER)
#undef FILESTORE_INIT_CRITICAL_EVENT_COUNTER

    NCloud::InitCriticalEventsCounter(std::move(counters));
}

#define FILESTORE_DEFINE_CRITICAL_EVENT_ROUTINE(name)                          \
    void Report##name()                                                        \
    {                                                                          \
        ReportCriticalEvent("AppCriticalEvents/"#name, "", false);             \
    }                                                                          \
// FILESTORE_DEFINE_CRITICAL_EVENT_ROUTINE

    FILESTORE_CRITICAL_EVENTS(FILESTORE_DEFINE_CRITICAL_EVENT_ROUTINE)
#undef FILESTORE_DEFINE_CRITICAL_EVENT_ROUTINE

}   // namespace NCloud::NFileStore
