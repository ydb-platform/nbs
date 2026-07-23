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
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        TString msg =                                                          \
            ComposeMessageWithSuffix(message, PrintParams(keyValues));         \
        return ReportCriticalEvent(GetCriticalEventFor##name(), msg, false);   \
    }                                                                          \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            PrintParams(keyValues),                                            \
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
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        TString msg =                                                          \
            ComposeMessageWithSuffix(message, PrintParams(keyValues));         \
        return ReportCriticalEvent(GetCriticalEventFor##name(), msg, false);   \
    }                                                                          \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            PrintParams(keyValues),                                            \
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
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        TString msg =                                                          \
            ComposeMessageWithSuffix(message, PrintParams(keyValues));         \
        return ReportCriticalEvent(GetCriticalEventFor##name(), msg, false);   \
    }                                                                          \
    TString Report##name(                                                      \
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            PrintParams(keyValues),                                            \
            true); /* verifyDebug */                                           \
    }                                                                          \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "AppImpossibleEvents/"#name;                                    \
    }                                                                          \
// BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE

    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_IMPOSSIBLE_EVENT_ROUTINE

#define BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE(name)                  \
    TString Report##name(                                                         \
        const TString& diskId,                                                 \
        const TString& cloudId,                                                \
        const TString& folderId,                                               \
        const TString& message)                                                \
    {                                                                          \
        return Report##name(                                                   \
            diskId,                                                            \
            cloudId,                                                           \
            folderId,                                                          \
            message,                                                           \
            {});                                                               \
    }                                                                          \
    TString Report##name(                                                      \
        const TString& diskId,                                                 \
        const TString& cloudId,                                                \
        const TString& folderId,                                               \
        const TString& message,                                                \
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        /* deprecated: keeps existing AppCriticalEvents/ * metrics alive */    \
        ReportCriticalEvent(                                                   \
            GetDeprecatedCriticalEventFor##name(), message, false);            \
                                                                               \
        auto prefix = TCritEventParams{                                        \
            {"disk", diskId},                                                  \
            {"cloud", cloudId},                                                \
            {"folder", folderId}};                                             \
                                                                               \
        TString submsg;                                                        \
                                                                               \
        if (message.size() && keyValues.size()) {                              \
            submsg = ComposeMessageWithSuffix(message, PrintParams(keyValues));\
        }                                                                      \
        else if (message.size() && !keyValues.size()) {                        \
            submsg = message;                                                  \
        }                                                                      \
        else if (!message.size() && keyValues.size()) {                        \
            submsg = PrintParams(keyValues);                                   \
        }                                                                      \
        else {                                                                 \
            /* leave submsg empty */                                           \
        }                                                                      \
                                                                               \
        TString msg =                                                          \
            !submsg.empty()                                                    \
                ? ComposeMessageWithSuffix(PrintParams(prefix), submsg)        \
                : PrintParams(prefix);                                         \
                                                                               \
        auto labels = TCritEventLabels{                                        \
            {"volume", diskId},                                                \
            {"cloud", cloudId},                                                \
            {"folder", folderId}};                                             \
                                                                               \
        return ReportCriticalEvent(                                            \
            GetCriticalEventFor##name(),                                       \
            labels,                                                            \
            msg,                                                               \
            false);                                                            \
    }                                                                          \
    TString Report##name(                                                      \
        const TString& diskId,                                                 \
        const TString& cloudId,                                                \
        const TString& folderId,                                               \
        const TCritEventParams& keyValues)                                     \
    {                                                                          \
        return Report##name(                                                   \
            diskId,                                                            \
            cloudId,                                                           \
            folderId,                                                          \
            {},                                                                \
            keyValues);                                                        \
    }                                                                          \
    const TString GetCriticalEventFor##name()                                  \
    {                                                                          \
        return "VolumeCriticalEvents/"#name;                                   \
    }                                                                          \
    const TString GetDeprecatedCriticalEventFor##name()                        \
    {                                                                          \
        return "AppCriticalEvents/"#name;                                      \
    }                                                                          \
// BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_VOLUME_CRITICAL_EVENTS(\
        BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE

}   // namespace NCloud::NBlockStore
