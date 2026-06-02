#include "critical_events.h"

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename... Ts>
TStringBuilder& operator<<(TStringBuilder& sb, const std::variant<Ts...>& v)
{
    std::visit([&sb](const auto& arg) { sb << arg; }, v);
    return sb;
}

TString ComposeMessageWithSuffix(const TString& message, const TString& suffix)
{
    if (message.empty()) {
        return suffix;
    }
    if (suffix.empty()) {
        return message;
    }
    return message + "; " + suffix;
}
}   // namespace

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
#define BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER(name)                           \
    *counters->GetCounter(GetCriticalEventFor##name(), true) = 0;              \
// BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER

    BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
    BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
        BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
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
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues)               \
    {                                                                          \
        TString msg =                                                          \
            ComposeMessageWithSuffix(message, PrintKeyValue(keyValues));  \
        return ReportCriticalEvent(GetCriticalEventFor##name(), msg, false);   \
    }                                                                          \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues)               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            PrintKeyValue(keyValues),                                     \
            false);                                                            \
    }                                                                          \
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
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues)               \
    {                                                                          \
        TString msg =                                                          \
            ComposeMessageWithSuffix(message, PrintKeyValue(keyValues));  \
        return ReportCriticalEvent(GetCriticalEventFor##name(), msg, false);   \
    }                                                                          \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues)               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            PrintKeyValue(keyValues),                                     \
            false); /* verifyDebug */                                          \
    }                                                                          \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "DiskAgentCriticalEvents/"#name;                                \
    }                                                                          \
// BLOCKSTORE_DEFINE_DISK_AGENT_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
        BLOCKSTORE_DEFINE_DISK_AGENT_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_CRITICAL_EVENT_ROUTINE

#define BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE(name)                       \
    TString Report##name(const TString& message)                               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            message,                                                           \
            true);  /* verifyDebug */                                          \
    }                                                                          \
    TString Report##name(                                                      \
        const TString& message,                                                \
        const TCritEventParams& keyValues)               \
    {                                                                          \
        TString msg =                                                          \
            ComposeMessageWithSuffix(message, PrintKeyValue(keyValues));  \
        return ReportCriticalEvent(GetCriticalEventFor##name(), msg, false);   \
    }                                                                          \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues)               \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            PrintKeyValue(keyValues),                                     \
            true); /* verifyDebug */                                           \
    }                                                                          \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "AppImpossibleEvents/"#name;                                    \
    }                                                                          \
// BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE

    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE

}   // namespace NCloud::NBlockStore
