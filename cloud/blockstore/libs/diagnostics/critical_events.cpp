#include "critical_events.h"

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NBlockStore {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
#define BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER(name)                           \
    *counters->GetCounter(GetCriticalEventFor##name(), true) = 0;              \
// BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER

    BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
    DISK_AGENT_CRITICAL_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
#undef BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER

    NCloud::InitCriticalEventsCounter(std::move(counters));
}

#define BLOCKSTORE_DEFINE_CRITICAL_EVENT_ROUTINE(name)                         \
    TString Report##name(const TString& message)                               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            message,                                                           \
            false);                                                            \
    }                                                                          \
                                                                               \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "AppCriticalEvents/"#name;                                      \
    }                                                                          \
// BLOCKSTORE_DEFINE_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_DEFINE_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DEFINE_DISK_AGENT_CRITICAL_EVENT_ROUTINE(name)              \
    TString Report##name(const TString& message)                               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            message,                                                           \
            false);                                                            \
    }                                                                          \
                                                                               \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "AppDiskAgentCriticalEvents/"#name;                             \
    }                                                                          \
// BLOCKSTORE_DEFINE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

    DISK_AGENT_CRITICAL_EVENTS(
        BLOCKSTORE_DEFINE_DISK_AGENT_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE(name)                       \
    TString Report##name(const TString& message)                               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            message,                                                           \
            true);                                                             \
    }                                                                          \
                                                                               \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "AppImpossibleEvents/"#name;                                    \
    }                                                                          \
// BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE

    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE

}   // namespace NCloud::NBlockStore
