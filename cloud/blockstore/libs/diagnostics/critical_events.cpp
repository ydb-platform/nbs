#include "critical_events.h"

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/str_stl.h>
#include <util/string/builder.h>

#include <tuple>
#include <type_traits>
#include <unordered_map>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////
// VolumeCriticalEvents
////////////////////////////////////////////////////////////////////////////////

/*
TVolumeCriticalEventCounter - per-interval CriticalEvent counter with
deferred export

Writing the number of CritEvents for an interval directly into the monitoring
counter (Exported) can lead to registered CriticalEvents being missed in
monitoring, because the 15s intervals (cycles) - the internal one and the
monitoring one - generally do not coincide:

1. End of the next monitoring interval - monitoring reads the current counter
   value (including 0, if no CriticalEvents have been registered in this
   internal interval yet)

2. End of the next internal interval -
   WriteVolumeCriticalEventCounters() resets the counter to 0.

3. If a CriticalEvent was registered between (1) and (2) (Report...() was
   called with an increment of the counter), that event will be lost and
   not reflected in monitoring

To exclude such a scenario:

- CriticalEvents for the current interval are accumulated in the Internal
  counter

- at the end of the interval, the Internal value is written into the Exported
  counter and held there until the end of the next interval, allowing
  monitoring to read the value in its own read cycle

Additionally:

- the separate use of Internal and Exported counters avoids losing
  CriticalEvents registered before module initialization (before
  TVolumeCriticalEvents::CountersRoot is set) - the value accumulated in
  Internal is not reset at the end of an interval when writing to Exported
  is not possible. At the end of the first interval after CountersRoot
  initialization, the value accumulated since startup in the Internal counter
  will be written into Exported
*/
struct TVolumeCriticalEventCounter
{
    // Per-interval CriticalEvents counter, not exported
    std::atomic<i64> Internal{0};
    // Per-interval CriticalEvents metrics counter, GAUGE.
    // Constructed lazily
    NMonitoring::TDynamicCounters::TCounterPtr Exported;
};

struct TVolumeCriticalEventKey
{
    TString Event;        // "VolumeCriticalEvent/<event>"
    TVolumeId VolumeId;   // exported as the 'volume', 'cloud' and 'folder'
                          // metric labels

    bool operator==(const TVolumeCriticalEventKey& rhs) const
    {
        return std::tie(Event, VolumeId) == std::tie(rhs.Event, rhs.VolumeId);
    }
};

}   // namespace
}   // namespace NCloud::NBlockStore

template <>
struct THash<NCloud::NBlockStore::TVolumeCriticalEventKey>
{
    size_t operator()(
        const NCloud::NBlockStore::TVolumeCriticalEventKey& val) const
    {
        const auto& a = std::tie(val.Event, val.VolumeId);
        return THash<std::decay_t<decltype(a)>>{}(a);
    }
};

namespace NCloud::NBlockStore {
namespace {

using TVolumeCriticalEventCounterMap = THashMap<
    TVolumeCriticalEventKey,
    std::shared_ptr<TVolumeCriticalEventCounter>>;

struct TVolumeCriticalEvents
{
    TRWMutex Lock;
    TVolumeCriticalEventCounterMap Counters;
    NMonitoring::TDynamicCountersPtr CountersRoot;
};

TVolumeCriticalEvents VolumeCriticalEvents;

void WriteVolumeCriticalEventCounters()
{
    TReadGuard guard(VolumeCriticalEvents.Lock);

    for (auto& [k, e]: VolumeCriticalEvents.Counters) {
        // NOTE: a single instance of TCriticalEventsStatsHandler is expected
        // (as the sole writer of e->Exported). This simplifies Lock usage
        // (e->Exported can be written under the read guard only).
        if (!e->Exported) {
            if (!VolumeCriticalEvents.CountersRoot) {
                // Root not initialized yet; keep accumulating in Internal,
                // see the first-fire branch in Report##name().
                continue;
            }
            // Root became available after the first fire (e.g. Report ran
            // before InitVolumeCriticalEventsCounter) - materialize the
            // exported GAUGE now so the accumulated Internal can be flushed.
            e->Exported = VolumeCriticalEvents.CountersRoot
                              ->GetSubgroup("volume", k.VolumeId.DiskId)
                              ->GetSubgroup("cloud", k.VolumeId.CloudId)
                              ->GetSubgroup("folder", k.VolumeId.FolderId)
                              ->GetCounter(k.Event, /*derivative=*/false);
        }
        auto v = e->Internal.exchange(0);
        *e->Exported = v;   // GAUGE set; sticky until next write
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TCriticalEventsStatsHandler: public NCloud::IStatsHandler
{
    void UpdateStats(bool updateIntervalFinished) override
    {
        if (updateIntervalFinished) {
            WriteVolumeCriticalEventCounters();
        }
    }
};

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

// deprecated: keeps existing AppCriticalEvents/ * for new
// VolumeCriticalEvents/ * metrics alive
#define BLOCKSTORE_INIT_DEPRECATED_CRITICAL_EVENT_COUNTER(name) \
    *counters->GetCounter(GetDeprecatedCriticalEventFor##name(), true) = 0;

    BLOCKSTORE_VOLUME_CRITICAL_EVENTS(
        BLOCKSTORE_INIT_DEPRECATED_CRITICAL_EVENT_COUNTER)

#undef BLOCKSTORE_INIT_DEPRECATED_CRITICAL_EVENT_COUNTER

    NCloud::InitCriticalEventsCounter(std::move(counters));
}

void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    TWriteGuard wguard(VolumeCriticalEvents.Lock);
    VolumeCriticalEvents.CountersRoot = counters;
}

NCloud::IStatsHandlerPtr CreateCriticalEventsStatsHandler()
{
    return std::make_shared<TCriticalEventsStatsHandler>();
}

// For unit test purposes
void ResetVolumeCriticalEventsCounter()
{
    TWriteGuard guard(VolumeCriticalEvents.Lock);
    VolumeCriticalEvents.Counters.clear();
    VolumeCriticalEvents.CountersRoot.Reset();
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

    // clang-format off
#define BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE(name)                  \
        TString Report##name(                                                  \
            const TString& diskId,                                             \
            const TString& cloudId,                                            \
            const TString& folderId,                                           \
            const TString& message)                                            \
        {                                                                      \
            return Report##name(diskId, cloudId, folderId, message, {});       \
        }                                                                      \
        TString Report##name(                                                  \
            const TString& diskId,                                             \
            const TString& cloudId,                                            \
            const TString& folderId,                                           \
            const TString& message,                                            \
            const TCritEventParams& keyValues)                                 \
        {                                                                      \
            /* deprecated: keeps existing AppCriticalEvents/ metrics alive */  \
            ReportCriticalEvent(                                               \
                GetDeprecatedCriticalEventFor##name(),                         \
                message,                                                       \
                false);                                                        \
                                                                               \
            auto prefix = TCritEventParams{                                    \
                {"disk", diskId},                                              \
                {"cloud", cloudId},                                            \
                {"folder", folderId}};                                         \
                                                                               \
            TString submsg;                                                    \
                                                                               \
            if (message.size() && keyValues.size()) {                          \
                submsg =                                                       \
                    ComposeMessageWithSuffix(message, PrintParams(keyValues)); \
            } else if (message.size() && !keyValues.size()) {                  \
                submsg = message;                                              \
            } else if (!message.size() && keyValues.size()) {                  \
                submsg = PrintParams(keyValues);                               \
            } else {                                                           \
                /* leave submsg empty */                                       \
            }                                                                  \
                                                                               \
            TString logMessage =                                               \
                !submsg.empty()                                                \
                    ? ComposeMessageWithSuffix(PrintParams(prefix), submsg)    \
                    : PrintParams(prefix);                                     \
                                                                               \
            /* Log immediatly */                                               \
            auto retMessage =                                                  \
                LogCriticalEvent(GetCriticalEventFor##name(), logMessage);     \
                                                                               \
            auto key = TVolumeCriticalEventKey{                                \
                .Event    = GetCriticalEventFor##name(),                       \
                .VolumeId = {                                                  \
                    .DiskId   = diskId,                                        \
                    .CloudId  = cloudId,                                       \
                    .FolderId = folderId}                                      \
            };                                                                 \
                                                                               \
            /* Hot path - counter already exists */                            \
            {                                                                  \
                TReadGuard guard(VolumeCriticalEvents.Lock);                   \
                if (auto it = VolumeCriticalEvents.Counters.find(key);         \
                    it != VolumeCriticalEvents.Counters.end())                 \
                {                                                              \
                    it->second->Internal++;                                    \
                    return retMessage;                                         \
                }                                                              \
            }                                                                  \
                                                                               \
            /* First fire - create the entry.                                  \
               The Exported GAUGE counter is materialized lazily               \
               by WriteVolumeCriticalEventCounters() on the publish tick.      \
               Here we only create and bump the Internal accumulator.          \
            */                                                                 \
            {                                                                  \
                TWriteGuard guard(VolumeCriticalEvents.Lock);                  \
                auto& e = VolumeCriticalEvents.Counters[key];                  \
                if (!e) {                                                      \
                    e = std::make_shared<TVolumeCriticalEventCounter>();       \
                }                                                              \
                e->Internal++;                                                 \
            }                                                                  \
                                                                               \
            return retMessage;                                                 \
        }                                                                      \
        TString Report##name(                                                  \
            const TString& diskId,                                             \
            const TString& cloudId,                                            \
            const TString& folderId,                                           \
            const TCritEventParams& keyValues)                                 \
        {                                                                      \
            return Report##name(diskId, cloudId, folderId, {}, keyValues);     \
        }                                                                      \
        const TString GetCriticalEventFor##name()                              \
        {                                                                      \
            return "VolumeCriticalEvents/" #name;                              \
        }                                                                      \
        const TString GetDeprecatedCriticalEventFor##name()                    \
        {                                                                      \
            return "AppCriticalEvents/" #name;                                 \
        }                                                                      \
        // BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_VOLUME_CRITICAL_EVENTS(\
        BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE
    // clang-format on

    }   // namespace NCloud::NBlockStore
