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
// AppCriticalEvents, AppImpossibleEvents, DiskAgentCriticalEvents and
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

// Shared interval counters, protected by Lock and published by one stats handler.
struct TCriticalEvents
{
    TAdaptiveLock Lock;
    TCriticalEventCounterMap Counters;
    // Events subtrees; null until the bootstrap attaches monitoring.
    NMonitoring::TDynamicCountersPtr AppCountersRoot;
    NMonitoring::TDynamicCountersPtr VolumeCountersRoot;
};

// Reporting destinations for volume events: application counters, per-volume
// counters, or both. Direct application events do not use this setting.
NProto::EVolumeCriticalEventsReportingMode VolumeCriticalEventsReportingMode =
    NProto::EVolumeCriticalEventsReportingMode::APP_ONLY;

// Shared process and volume counter state. Pending counts are kept until their
// monitoring root is attached and the stats handler publishes them.
TCriticalEvents CriticalEvents;

// Counter mode for AppCriticalEvents, AppImpossibleEvents and
// DiskAgentCriticalEvents: true selects GAUGE counters with early accumulation;
// false selects RATE counters.
// Enabled by early bootstrap together with the callback for counting events.
bool ProcessEventsGaugesEnabled = false;

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

    // Keep early events pending until the corresponding counter root is
    // attached.
    with_lock (CriticalEvents.Lock) {
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
            auto counters = k.IsVolumeEvent ? CriticalEvents.VolumeCountersRoot
                                            : CriticalEvents.AppCountersRoot;
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

// Enable accumulation of process event counts for GAUGE publication.
void InitProcessCriticalEventsReporting()
{
    ProcessEventsGaugesEnabled = true;
    SetCriticalEventReporter(AccumulateProcessCriticalEvent);
}

void InitVolumeCriticalEventsReportingMode(
    NProto::EVolumeCriticalEventsReportingMode reportingMode)
{
    VolumeCriticalEventsReportingMode = reportingMode;
}

void InitCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    const auto initCounter = [&](const TString& name)
    {
        auto counter = counters->GetCounter(name, !ProcessEventsGaugesEnabled);
        if (!ProcessEventsGaugesEnabled) {
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

    NCloud::InitCriticalEventsCounter(counters, !ProcessEventsGaugesEnabled);

    // Attach the process GAUGE root and clear cached counter pointers so the
    // next publication uses this root even after repeated initialization.
    if (ProcessEventsGaugesEnabled) {
        with_lock (CriticalEvents.Lock) {
            CriticalEvents.AppCountersRoot = std::move(counters);
            for (auto& [key, counter]: CriticalEvents.Counters) {
                if (!key.IsVolumeEvent) {
                    counter.Published.Reset();
                }
            }
        }
    }
}

void InitVolumeCriticalEventsCounter(NMonitoring::TDynamicCountersPtr counters)
{
    with_lock (CriticalEvents.Lock) {
        CriticalEvents.VolumeCountersRoot = counters;
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
        CriticalEvents.VolumeCountersRoot.Reset();
        CriticalEvents.AppCountersRoot.Reset();
    }
    ProcessEventsGaugesEnabled = false;
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
