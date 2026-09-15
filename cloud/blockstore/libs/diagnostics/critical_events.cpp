#include "critical_events.h"

#include "public.h"

#include "critical_events_init.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/algorithm.h>
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
// Process Critical Events (AppCriticalEvents, AppImpossibleEvents,
// DiskAgentCriticalEvents)
// and
// VolumeCriticalEvents
////////////////////////////////////////////////////////////////////////////////

/*
TCriticalEventCounter - event counts accumulated for interval GAUGE publication.

Each report increments Unpublished. At the end of a publication interval,
PublishCriticalEventCounters() assigns this count to Published and resets
Unpublished to zero. The GAUGE holds the assigned value until the next
publication; an empty interval publishes zero.

Monitoring collects the GAUGE on its own schedule. Holding the completed
interval's count allows monitoring to observe events reported near a
publication boundary. Collection must occur while that value is published.

While the monitoring root is unavailable, Unpublished retains event counts
across publication intervals. The first publication after the root is attached
includes all counts accumulated up to that publication.
*/
struct TCriticalEventCounter
{
    // Event count awaiting publication.
    i64 Unpublished{0};
    // GAUGE holding the event count from the last publication.
    // Null until publication attaches it to an initialized monitoring root.
    NMonitoring::TDynamicCounters::TCounterPtr Published;
};

// Sensor identity with an explicit process or volume scope.
struct TCriticalEventKey
{
    // Full sensor name, including its event family.
    TString Event;
    // Counter scope: true for volume events, false for process events.
    bool IsVolumeEvent;
    // Volume labels; used only for volume events.
    TVolumeLabels VolumeLabels;
};

inline bool operator==(
    const TCriticalEventKey& lhs,
    const TCriticalEventKey& rhs)
{
    return std::tie(lhs.Event, lhs.IsVolumeEvent, lhs.VolumeLabels) ==
           std::tie(rhs.Event, rhs.IsVolumeEvent, rhs.VolumeLabels);
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
        const auto& a =
            std::tie(val.Event, val.IsVolumeEvent, val.VolumeLabels);
        return THash<std::decay_t<decltype(a)>>{}(a);
    }
};

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TCriticalEventCounterMap =
    THashMap<TCriticalEventKey, TCriticalEventCounter>;

// Shared state for process and volume critical events.
// A single stats handler publishes the interval counts.
// Access to the fields under Lock.
struct TCriticalEvents
{
    TAdaptiveLock Lock;
    TCriticalEventCounterMap Counters;
    // Events subtrees; null until the bootstrap attaches monitoring.
    NMonitoring::TDynamicCountersPtr ProcessCountersRoot;
    NMonitoring::TDynamicCountersPtr VolumeCountersRoot;
    // Process buffering state. Access under Lock.
    // - InitProcessCriticalEventsReporting() enables buffering.
    // - InitCriticalEventsCounter() keeps it enabled in GAUGE mode. In RATE
    //   mode, it disables buffering after adding the buffered counts to the
    //   RATE counters.
    // - ResetCriticalEventsCounter() restores the initial value of false.
    bool BufferProcessEvents = false;
};

// Reporting destinations for volume events: application counters, per-volume
// counters, or both. Direct application events do not use this setting.
NProto::EVolumeCriticalEventsReportingMode VolumeCriticalEventsReportingMode =
    NProto::EVolumeCriticalEventsReportingMode::APP_ONLY;

// Shared process and volume counter state. Pending counts are kept until their
// monitoring root is attached and the stats handler publishes them.
TCriticalEvents CriticalEvents;

// Init the GAUGE root for one event scope.
void InitGaugeCountersRoot(
    NMonitoring::TDynamicCountersPtr counters,
    bool isVolumeRoot)
{
    auto& root = isVolumeRoot ? CriticalEvents.VolumeCountersRoot
                              : CriticalEvents.ProcessCountersRoot;

    // Preserve startup counts on the first attachment.
    // On a repeated call, discard pending counts and clear published values
    // when reusing the root.
    if (root) {
        const bool reuseRoot = (root.Get() == counters.Get());
        EraseNodesIf(
            CriticalEvents.Counters,
            [&](const auto& entry)
            {
                const auto& [key, counter] = entry;
                if (key.IsVolumeEvent != isVolumeRoot) {
                    return false;
                }
                if (reuseRoot && counter.Published) {
                    *counter.Published = 0;
                }
                return true;
            });
    }

    root = std::move(counters);
}

// Init the process RATE root.
void InitRateCountersRoot(const NMonitoring::TDynamicCountersPtr& counters)
{
    // Finish RATE initialization here for callers that skip
    // InitProcessCriticalEventsReporting(). They have no buffered
    // startup counts to add and no startup callback to clear.
    if (!CriticalEvents.BufferProcessEvents) {
        return;
    }

    // Add the buffered counts to the RATE counters, then switch to
    // direct increments.
    EraseNodesIf(
        CriticalEvents.Counters,
        [&](const auto& entry)
        {
            const auto& [key, counter] = entry;
            if (key.IsVolumeEvent) {
                return false;
            }
            *counters->GetCounter(key.Event, /*derivative=*/true) +=
                counter.Unpublished;
            return true;
        });
    CriticalEvents.BufferProcessEvents = false;
    SetCriticalEventReporter(nullptr);
}

// Accumulate process events and leave other families on the default path.
bool AccumulateProcessCriticalEvent(const TString& sensorName)
{
    // Use default counter increments for unrelated sensor families.
    if (!sensorName.StartsWith("AppCriticalEvents/") &&
        !sensorName.StartsWith("AppImpossibleEvents/") &&
        !sensorName.StartsWith("DiskAgentCriticalEvents/"))
    {
        return false;
    }

    with_lock (CriticalEvents.Lock) {
        // A reporting thread (which called Report..()) can load the callback
        // (this function) before RATE initialization via
        // InitCriticalEventsCounter() clears it. The flag lets that callback
        // fall back to a direct RATE increment after the buffered counts have
        // been added.
        if (!CriticalEvents.BufferProcessEvents) {
            return false;
        }

        // Keep early events pending until the corresponding counter root is
        // attached.
        auto key = TCriticalEventKey{
            .Event = sensorName,
            .IsVolumeEvent = false,
            .VolumeLabels = {}};
        ++CriticalEvents.Counters[key].Unpublished;
    }
    return true;
}

// Publish interval event counts once their monitoring root is available.
void PublishCriticalEventCounters()
{
    TGuard<TAdaptiveLock> guard(CriticalEvents.Lock);

    for (auto& [k, e]: CriticalEvents.Counters) {
        // Attach this entry to its counter once the monitoring root is available.
        if (!e.Published) {
            auto counters = k.IsVolumeEvent
                                ? CriticalEvents.VolumeCountersRoot
                                : CriticalEvents.ProcessCountersRoot;
            if (!counters) {
                // Root not initialized yet; keep accumulating in Unpublished
                continue;
            }

            // Reuse process counters created during initialization. Create
            // missing counters and volume label subgroups when publishing.
            if (k.IsVolumeEvent) {
                counters =
                    counters->GetSubgroup("volume", k.VolumeLabels.DiskId)
                        ->GetSubgroup("cloud", k.VolumeLabels.CloudId)
                        ->GetSubgroup("folder", k.VolumeLabels.FolderId);
            }
            e.Published = counters->GetCounter(k.Event, /*derivative=*/false);
        }
        *e.Published = e.Unpublished;   // Hold until the next publication.
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

void InitProcessCriticalEventsReporting()
{
    with_lock (CriticalEvents.Lock) {
        CriticalEvents.BufferProcessEvents = true;
        SetCriticalEventReporter(AccumulateProcessCriticalEvent);
    }
}

void InitVolumeCriticalEventsReportingMode(
    NProto::EVolumeCriticalEventsReportingMode reportingMode)
{
    VolumeCriticalEventsReportingMode = reportingMode;
}

void InitCriticalEventsCounter(
    NMonitoring::TDynamicCountersPtr counters,
    bool derivative)
{
    const auto initCounter = [&](const TString& name)
    {
        auto counter = counters->GetCounter(name, derivative);
        if (derivative) {
            *counter = 0;
        }
    };
#define BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER(name)                           \
    initCounter(GetCriticalEventFor##name());                                  \
// BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER

    BLOCKSTORE_CRITICAL_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
    BLOCKSTORE_IMPOSSIBLE_EVENTS(BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
    BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
        BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER)
#undef BLOCKSTORE_INIT_CRITICAL_EVENT_COUNTER

// Keeps existing AppCriticalEvents/ * for new VolumeCriticalEvents/ * metrics
// alive
#define BLOCKSTORE_INIT_APP_CRITICAL_EVENT_COUNTER(name)                       \
    initCounter(GetAppCriticalEventFor##name());

    if (VolumeCriticalEventsReportingMode !=
        NProto::EVolumeCriticalEventsReportingMode::VOLUME_ONLY)
    {
        BLOCKSTORE_VOLUME_CRITICAL_EVENTS(
            BLOCKSTORE_INIT_APP_CRITICAL_EVENT_COUNTER)
    }

#undef BLOCKSTORE_INIT_APP_CRITICAL_EVENT_COUNTER

    NCloud::InitCriticalEventsCounter(counters, derivative);

    TGuard<TAdaptiveLock> guard(CriticalEvents.Lock);

    if (!derivative) {
        InitGaugeCountersRoot(std::move(counters), /*isVolumeRoot=*/false);
    } else {
        InitRateCountersRoot(counters);
    }
}

void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    with_lock (CriticalEvents.Lock) {
        InitGaugeCountersRoot(std::move(counters), /*isVolumeRoot=*/true);
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
        CriticalEvents.ProcessCountersRoot.Reset();
        CriticalEvents.VolumeCountersRoot.Reset();
        CriticalEvents.BufferProcessEvents = false;
        SetCriticalEventReporter(nullptr);
    }
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
            auto key = TCriticalEventKey{                                      \
                .Event = GetVolumeCriticalEventFor##name(),                    \
                .IsVolumeEvent = true,                                         \
                .VolumeLabels = {                                              \
                    .DiskId = diskId,                                          \
                    .CloudId = cloudId,                                        \
                    .FolderId = folderId}};                                    \
                                                                               \
            with_lock (CriticalEvents.Lock) {                                  \
                /*                                                             \
                1. PublishCriticalEventCounters() creates the per-volume       \
                   GAUGE counter on the first publication. Reports create     \
                   an entry and increment Unpublished.                        \
                2. The footprint of the unbounded-lifetime                     \
                   VolumeCriticalEvents metric is not deemed a measurable      \
                   concern due to rare tablet migrations, rare critical        \
                   events, and periodic (release-based) process restarts.      \
                */                                                             \
                CriticalEvents.Counters[key].Unpublished++;                    \
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
