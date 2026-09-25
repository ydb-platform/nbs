#include "critical_events.h"

#include "public.h"

#include "critical_events_init.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/str_stl.h>
#include <util/string/builder.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <tuple>
#include <type_traits>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////
// AppCriticalEvents, AppImpossibleEvents and VolumeCriticalEvents
////////////////////////////////////////////////////////////////////////////////

/*
TCriticalEventCounter - per-interval critical event counter with
deferred publishing

Writing the number of critical events for an interval directly into the
monitoring counter (Published) can lead to registered critical events being
missed in monitoring, because the 15s intervals (cycles) - the internal one
and the monitoring one - generally do not coincide:

1. End of the next monitoring interval - monitoring reads the current counter
   value (including 0, if no critical events have been registered in this
   internal interval yet)

2. End of the next internal interval -
   PublishCriticalEventCounters() resets the counter to 0.

3. If a critical event was registered between (1) and (2) (Report...() was
   called with an increment of the counter), that event will be lost and
   not reflected in monitoring

To exclude such a scenario:

- critical events for the current interval are accumulated in the Unpublished
  counter

- at the end of the interval, the Unpublished value is written into the
  Published counter and held there until the end of the next interval, allowing
  monitoring to read the value in its own read cycle

Additionally:

- the separate use of Unpublished and Published counters avoids losing
  critical events registered before module initialization (before
  the corresponding counter root is set) - the value accumulated in Unpublished
  is not reset at the end of an interval when writing to Published
  is not possible. At the end of the first interval after counter root
  initialization, the value accumulated since startup in the Unpublished counter
  will be written into Published
*/
struct TCriticalEventCounter
{
    // Per-interval critical events counter, not published yet
    i64 Unpublished{0};
    // Per-interval critical events metrics counter, GAUGE.
    // Published and held until next publish interval.
    // Constructed lazily
    NMonitoring::TDynamicCounters::TCounterPtr Published;
};

// Sensor identity; empty volume labels select the process-wide application root.
struct TCriticalEventKey
{
    // Full sensor name, including its App or Volume event family.
    TString Event;
    // Volume labels; empty DiskId selects the application counter root.
    TVolumeLabels VolumeLabels;
};

inline bool operator==(
    const TCriticalEventKey& lhs,
    const TCriticalEventKey& rhs)
{
    return std::tie(lhs.Event, lhs.VolumeLabels) ==
           std::tie(rhs.Event, rhs.VolumeLabels);
}

}   // namespace

}   // namespace NCloud::NBlockStore

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NCloud::NBlockStore::TCriticalEventKey>
{
    size_t operator()(
        const NCloud::NBlockStore::TCriticalEventKey& val) const
    {
        const auto& a = std::tie(val.Event, val.VolumeLabels);
        return THash<std::decay_t<decltype(a)>>{}(a);
    }
};

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCriticalEventCounterMap =
    THashMap<TCriticalEventKey, TCriticalEventCounter>;

// Shared interval counters, protected by Lock and published by one stats handler.
struct TCriticalEvents
{
    TAdaptiveLock Lock;
    TCriticalEventCounterMap Counters;
    NMonitoring::TDynamicCountersPtr CountersRoot;
    // Application event subtree; null until the bootstrap attaches monitoring.
    NMonitoring::TDynamicCountersPtr AppCountersRoot;
};

NProto::EVolumeCriticalEventsReportingMode VolumeCriticalEventsReportingMode =
    NProto::EVolumeCriticalEventsReportingMode::APP_ONLY;
TCriticalEvents CriticalEvents;
bool AppCriticalEventsEnabled = false;

// Accumulate application events and leave other families on the default path.
bool AccumulateAppCriticalEvent(const TString& sensorName)
{
    // Preserve cumulative reporting for unrelated sensor families.
    if (!sensorName.StartsWith("AppCriticalEvents/") &&
        !sensorName.StartsWith("AppImpossibleEvents/"))
    {
        return false;
    }

    // Keep early events pending until the application counter root is attached.
    with_lock (CriticalEvents.Lock) {
        ++CriticalEvents.Counters[TCriticalEventKey{sensorName, {}}].Unpublished;
    }
    return true;
}

// Publish each completed interval and retain pending events without a root.
void PublishCriticalEventCounters()
{
    TGuard<TAdaptiveLock> guard(CriticalEvents.Lock);

    for (auto& [k, e]: CriticalEvents.Counters) {
        // Attach each counter lazily once its monitoring root is available.
        if (!e.Published) {
            auto counters = k.VolumeLabels.DiskId.empty()
                                ? CriticalEvents.AppCountersRoot
                                : CriticalEvents.CountersRoot;
            if (!counters) {
                continue;
            }
            if (!k.VolumeLabels.DiskId.empty()) {
                counters = counters->GetSubgroup("volume", k.VolumeLabels.DiskId)
                               ->GetSubgroup("cloud", k.VolumeLabels.CloudId)
                               ->GetSubgroup("folder", k.VolumeLabels.FolderId);
            }
            e.Published = counters->GetCounter(k.Event, /*derivative=*/false);
        }
        *e.Published = e.Unpublished;   // GAUGE set; held until next flush
        e.Unpublished = 0;
    }
}

struct TCriticalEventsStatsHandler: public NCloud::IStatsHandler
{
    void UpdateStats(bool updateIntervalFinished) override
    {
        if (updateIntervalFinished) {
            PublishCriticalEventCounters();
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

////////////////////////////////////////////////////////////////////////////////

// Enable the shared counter override before startup can report an app event.
void InitAppCriticalEventsReporting()
{
    AppCriticalEventsEnabled = true;
    SetCriticalEventReporter(AccumulateAppCriticalEvent);
}

void InitVolumeCriticalEventsReportingMode(
    NProto::EVolumeCriticalEventsReportingMode reportingMode)
{
    VolumeCriticalEventsReportingMode = reportingMode;
}

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    const bool derivative = !AppCriticalEventsEnabled;
    const auto initCounter = [&](const TString& name, bool rate)
    {
        auto counter = counters->GetCounter(name, rate);
        if (rate) {
            *counter = 0;
        }
    };
#define BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER(name)                           \
    initCounter(GetCriticalEventFor##name(), derivative);                      \
// BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER

    BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
#undef BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER

#define BLOCKSTORE_INIT_DISK_AGENT_CRITICAL_EVENT_COUNTER(name)                 \
    initCounter(GetCriticalEventFor##name(), true);

    BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
        BLOCKSTORE_INIT_DISK_AGENT_CRITICAL_EVENT_COUNTER)
#undef BLOCKSTORE_INIT_DISK_AGENT_CRITICAL_EVENT_COUNTER

// Keeps existing AppCriticalEvents/ * for new VolumeCriticalEvents/ * metrics
// alive
#define BLOCKSTORE_INIT_APP_CRITICAL_EVENT_COUNTER(name)                       \
    initCounter(GetAppCriticalEventFor##name(), derivative);

    if (VolumeCriticalEventsReportingMode !=
        NProto::EVolumeCriticalEventsReportingMode::VOLUME_ONLY)
    {
        BLOCKSTORE_VOLUME_CRITICAL_EVENTS(
            BLOCKSTORE_INIT_APP_CRITICAL_EVENT_COUNTER)
    }

#undef BLOCKSTORE_INIT_APP_CRITICAL_EVENT_COUNTER

    NCloud::InitCriticalEventsCounter(counters, derivative);
    if (AppCriticalEventsEnabled) {
        with_lock (CriticalEvents.Lock) {
            CriticalEvents.AppCountersRoot = std::move(counters);
            for (auto& [key, counter]: CriticalEvents.Counters) {
                if (key.VolumeLabels.DiskId.empty()) {
                    counter.Published.Reset();
                }
            }
        }
    }
}

void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    with_lock (CriticalEvents.Lock) {
        CriticalEvents.CountersRoot = counters;
    }
}

NCloud::IStatsHandlerPtr CreateCriticalEventsStatsHandler()
{
    return std::make_shared<TCriticalEventsStatsHandler>();
}

// Clear pending events and roots and restore default reporting for tests.
void ResetCriticalEventsCounter()
{
    with_lock (CriticalEvents.Lock) {
        CriticalEvents.Counters.clear();
        CriticalEvents.CountersRoot.Reset();
        CriticalEvents.AppCountersRoot.Reset();
    }
    AppCriticalEventsEnabled = false;
    SetCriticalEventReporter(nullptr);
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

#define BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE(name)              \
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
            TString retMessage;                                                \
                                                                               \
            /* Keep per-host AppCriticalEvents/ metrics alive */               \
            if (VolumeCriticalEventsReportingMode !=                           \
                NProto::EVolumeCriticalEventsReportingMode::VOLUME_ONLY)       \
            {                                                                  \
                TString params =                                               \
                    diskId.empty()                                             \
                        ? PrintParams(keyValues)                               \
                        : PrintParams(TCritEventParams{{"disk", diskId}}) +    \
                              " " + PrintParams(keyValues);                    \
                                                                               \
                retMessage = ReportCriticalEvent(                              \
                    GetAppCriticalEventFor##name(),                            \
                    ComposeMessageWithSuffix(message, params),                 \
                    /*verifyDebug=*/false);                                    \
            }                                                                  \
                                                                               \
            if (VolumeCriticalEventsReportingMode ==                           \
                NProto::EVolumeCriticalEventsReportingMode::APP_ONLY)          \
            {                                                                  \
                return retMessage;                                             \
            }                                                                  \
                                                                               \
            TString msg =                                                      \
                ComposeMessageWithSuffix(message, PrintParams(keyValues));     \
                                                                               \
            auto prefix = TCritEventParams{                                    \
                {"disk", diskId.empty() ? "<empty>" : diskId},                 \
                {"cloud", cloudId.empty() ? "<empty>" : cloudId},              \
                {"folder", folderId.empty() ? "<empty>" : folderId}};          \
                                                                               \
            TString logMessage =                                               \
                !msg.empty()                                                   \
                    ? ComposeMessageWithSuffix(PrintParams(prefix), msg)       \
                    : PrintParams(prefix);                                     \
                                                                               \
            /* Log immediately */                                              \
            retMessage = LogCriticalEvent(                                     \
                GetVolumeCriticalEventFor##name(),                             \
                logMessage);                                                   \
                                                                               \
            if (diskId.empty() || diskId == "<nullptr>") {                     \
                if (diskId.empty()) {                                          \
                    REPORT_BUG(Sprintf(                                        \
                        "empty diskId provided for %s report, "                \
                        "monitoring metrics will not be updated",              \
                        GetVolumeCriticalEventFor##name().c_str()));           \
                }                                                              \
                /* else - bug was reported earlier in                          \
                   Report##name(TVolumeLabelsConstPtr) caller overload */      \
                                                                               \
                /* No metric with an empty 'volume=""' label is created for    \
                   empty disk id */                                            \
                return retMessage;                                             \
            }                                                                  \
                                                                               \
            auto key = TCriticalEventKey{                                     \
                .Event = GetVolumeCriticalEventFor##name(),                    \
                .VolumeLabels = {                                              \
                    .DiskId = diskId,                                          \
                    .CloudId = cloudId,                                        \
                    .FolderId = folderId}};                                    \
                                                                               \
            with_lock (CriticalEvents.Lock) {                                 \
                /*                                                             \
                1. The Published GAUGE counter is materialized lazily          \
                   by PublishCriticalEventCounters() on the publish           \
                   tick. Here we only create and bump the Unpublished          \
                   accumulator.                                                \
                2. The footprint of the unbounded-lifetime                     \
                   VolumeCriticalEvents metric is not deemed a measurable      \
                   concern due to rare tablet migrations, rare critical        \
                   events, and periodic (release-based) process restarts.      \
                */                                                             \
                CriticalEvents.Counters[key].Unpublished++;                   \
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
        const TString GetVolumeCriticalEventFor##name()                        \
        {                                                                      \
            return "VolumeCriticalEvents/" #name;                              \
        }                                                                      \
        const TString GetAppCriticalEventFor##name()                           \
        {                                                                      \
            return "AppCriticalEvents/" #name;                                 \
        }                                                                      \
        // BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE

    BLOCKSTORE_VOLUME_CRITICAL_EVENTS(\
        BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE)
#undef BLOCKSTORE_DEFINE_VOLUME_CRITICAL_EVENT_ROUTINE

}   // namespace NCloud::NBlockStore
