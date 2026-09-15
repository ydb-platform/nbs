#include "critical_events.h"

#include "critical_events_init.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/system/event.h>

#include <atomic>
#include <numeric>
#include <thread>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NMonitoring;

constexpr TStringBuf AppCriticalEventsComponent = "server";
constexpr TStringBuf VolumeCriticalEventsComponent = "critical_events";

TIntrusivePtr<TDynamicCounters> FindNestedGroup(
    TDynamicCountersPtr root,
    std::initializer_list<std::pair<TString, TString>> path)
{
    auto group = root;
    for (const auto& [key, value] : path) {
        group = group->FindSubgroup(key, value);
        if (!group) {
            return nullptr;
        }
    }
    return group;
}

// Resolves the per-disk counter group under component=critical_events.
TIntrusivePtr<TDynamicCounters> FindVolumeGroup(
    TDynamicCountersPtr volumeCriticalEventsGroup,
    const TVolumeLabels& v)
{
    return FindNestedGroup(
        volumeCriticalEventsGroup,
        {{"volume", v.DiskId}, {"cloud", v.CloudId}, {"folder", v.FolderId}});
}

TString GetVolumeSensorName()
{
    return GetVolumeCriticalEventForBlockDigestMismatchInBlob();
}

TString GetAppSensorName()
{
    return GetAppCriticalEventForBlockDigestMismatchInBlob();
}

// Check first attachment and reinitialization of a GAUGE monitoring root.
template <typename TInit, typename TReport, typename TFindCounter>
void CheckGaugeCounterReinitialization(
    const TInit& init,
    const TReport& report,
    const TFindCounter& findCounter)
{
    auto root = MakeIntrusive<TDynamicCounters>();
    auto handler = CreateCriticalEventsStatsHandler();

    // Retain a startup event until the first root is attached.
    report();
    handler->UpdateStats(true);
    init(root);
    handler->UpdateStats(true);
    auto counter = findCounter(root);
    UNIT_ASSERT(counter);
    UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());

    // Reuse the root and clear both its visible count and the pending report.
    report();
    init(root);
    UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
    handler->UpdateStats(true);
    UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());

    // Replace the root while the old tree has a published value and pending
    // reports. Keep the published value in the old tree.
    report();
    handler->UpdateStats(true);
    UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
    report();
    report();
    auto newRoot = MakeIntrusive<TDynamicCounters>();
    init(newRoot);
    handler->UpdateStats(true);
    auto newCounter = findCounter(newRoot);
    UNIT_ASSERT_VALUES_EQUAL(0, newCounter ? newCounter->Val() : 0);
    UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());

    // Publish only the reports made after replacement to the new tree.
    report();
    report();
    handler->UpdateStats(true);
    newCounter = findCounter(newRoot);
    UNIT_ASSERT(newCounter);
    UNIT_ASSERT_VALUES_EQUAL(2, newCounter->Val());
    UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCriticalEventsTest)
{
    // Check startup retention, immediate logging and the published counts for
    // one process event in the selected mode. A null reporter exercises the
    // counter path for events whose wrappers abort in debug builds.
    void DoShouldCountEarlyAndRuntimeProcessEvents(
        const TString& sensorName,
        TString (*reporter)(const TString&),
        bool derivative)
    {
        // Capture the log and start collecting events before monitoring exists.
        ResetCriticalEventsCounter();
        TStringStream log;
        SetCriticalEventsLog(TLog(MakeHolder<TStreamLogBackend>(&log)));
        Y_DEFER
        {
            SetCriticalEventsLog(TLog());
            ResetCriticalEventsCounter();
        };
        InitProcessCriticalEventsReporting();
        auto handler = CreateCriticalEventsStatsHandler();
        const auto reportEvent = [&](const TString& message)
        {
            if (reporter) {
                const auto loggedMessage = reporter(message);
                UNIT_ASSERT_STRING_CONTAINS(log.Str(), loggedMessage);
            } else {
                ReportCriticalEventWithoutLogging(sensorName);
            }
        };

        // Retain the startup event across publication intervals without a root.
        reportEvent("startup event");
        const TString startupLog = log.Str();
        handler->UpdateStats(true);
        handler->UpdateStats(true);

        // Attach the counters and check when the startup count becomes visible.
        auto counters = MakeIntrusive<TDynamicCounters>();
        InitCriticalEventsCounter(counters, derivative);
        auto counter = counters->FindCounter(sensorName);
        UNIT_ASSERT_C(counter, sensorName);
        UNIT_ASSERT_VALUES_EQUAL_C(
            derivative,
            counter->ForDerivative(),
            sensorName);
        const auto checkCount = [&](i64 gaugeCount, i64 rateCount)
        {
            UNIT_ASSERT_VALUES_EQUAL_C(
                derivative ? rateCount : gaugeCount,
                counter->Val(),
                sensorName);
        };
        checkCount(0, 1);
        UNIT_ASSERT_VALUES_EQUAL(startupLog, log.Str());

        // Check that RATE counts the next event immediately and GAUGE waits for
        // a completed interval. Publishing the count must not log it again.
        reportEvent("current interval");
        checkCount(0, 2);
        const TString firstIntervalLog = log.Str();
        handler->UpdateStats(false);
        checkCount(0, 2);
        handler->UpdateStats(true);
        checkCount(2, 2);
        UNIT_ASSERT_VALUES_EQUAL(firstIntervalLog, log.Str());

        // Check that GAUGE holds the last interval and that RATE accumulates
        // across both nonempty and empty intervals.
        reportEvent("next interval");
        checkCount(2, 3);
        handler->UpdateStats(true);
        checkCount(1, 3);
        handler->UpdateStats(true);
        checkCount(0, 3);
    }

    // Exercise AppCriticalEvents from both BlockStore and storage core.
    void DoShouldCountEarlyAndRuntimeAppCriticalEvents(bool derivative)
    {
        DoShouldCountEarlyAndRuntimeProcessEvents(
            GetCriticalEventForGetConfigsFromCmsYamlParseError(),
            ReportGetConfigsFromCmsYamlParseError,
            derivative);
        DoShouldCountEarlyAndRuntimeProcessEvents(
            GetCriticalEventForRdmaError(),
            ReportRdmaError,
            derivative);
    }

    // Check early and runtime AppCriticalEvents.
    Y_UNIT_TEST(ShouldCountEarlyAndRuntimeAppCriticalEventsGauge)
    {
        DoShouldCountEarlyAndRuntimeAppCriticalEvents(/*derivative=*/false);
    }

    Y_UNIT_TEST(ShouldCountEarlyAndRuntimeAppCriticalEventsRate)
    {
        DoShouldCountEarlyAndRuntimeAppCriticalEvents(/*derivative=*/true);
    }

    // Exercise AppImpossibleEvents from both BlockStore and storage core.
    void DoShouldCountEarlyAndRuntimeAppImpossibleEvents(bool derivative)
    {
        // Pass nullptr in debug builds so the helper counts AppImpossibleEvents
        // through ReportCriticalEventWithoutLogging. The AppImpossibleEvents
        // reporting wrappers abort in debug builds, so these tests cover only
        // the counters there. The release tests also exercise the wrappers and
        // check their logging.

#ifdef NDEBUG
        TString (*reportBug)(const TString&) = ReportBug;
        TString (*reportUnexpectedEvent)(const TString&) =
            ReportUnexpectedEvent;
#else
        TString (*reportBug)(const TString&) = nullptr;
        TString (*reportUnexpectedEvent)(const TString&) = nullptr;
#endif
        DoShouldCountEarlyAndRuntimeProcessEvents(
            GetCriticalEventForBug(),
            reportBug,
            derivative);
        DoShouldCountEarlyAndRuntimeProcessEvents(
            GetImpossibleEventForUnexpectedEvent(),
            reportUnexpectedEvent,
            derivative);
    }

    // Check early and runtime AppImpossibleEvents.
    Y_UNIT_TEST(ShouldCountEarlyAndRuntimeAppImpossibleEventsGauge)
    {
        DoShouldCountEarlyAndRuntimeAppImpossibleEvents(/*derivative=*/false);
    }

    Y_UNIT_TEST(ShouldCountEarlyAndRuntimeAppImpossibleEventsRate)
    {
        DoShouldCountEarlyAndRuntimeAppImpossibleEvents(/*derivative=*/true);
    }

    // Exercise DiskAgent configuration and session cache errors.
    void DoShouldCountEarlyAndRuntimeDiskAgentCriticalEvents(bool derivative)
    {
        DoShouldCountEarlyAndRuntimeProcessEvents(
            GetCriticalEventForDiskAgentConfigMismatch(),
            ReportDiskAgentConfigMismatch,
            derivative);
        DoShouldCountEarlyAndRuntimeProcessEvents(
            GetCriticalEventForDiskAgentSessionCacheRestoreError(),
            ReportDiskAgentSessionCacheRestoreError,
            derivative);
    }

    // Check early and runtime DiskAgentCriticalEvents.
    Y_UNIT_TEST(ShouldCountEarlyAndRuntimeDiskAgentCriticalEventsGauge)
    {
        DoShouldCountEarlyAndRuntimeDiskAgentCriticalEvents(
            /*derivative=*/false);
    }

    Y_UNIT_TEST(ShouldCountEarlyAndRuntimeDiskAgentCriticalEventsRate)
    {
        DoShouldCountEarlyAndRuntimeDiskAgentCriticalEvents(
            /*derivative=*/true);
    }

    // Check the total across concurrent reporting, counter initialization and
    // periodic publication.
    void DoShouldCountConcurrentAppEventsExactlyOnce(bool derivative)
    {
        // Start buffering before any producer reports an event.
        ResetCriticalEventsCounter();
        Y_DEFER
        {
            ResetCriticalEventsCounter();
        };
        InitProcessCriticalEventsReporting();
        auto counters = MakeIntrusive<TDynamicCounters>();
        auto handler = CreateCriticalEventsStatsHandler();
        const auto sensor =
            GetCriticalEventForGetConfigsFromCmsYamlParseError();
        constexpr ui32 workerCount = 4;
        constexpr ui32 runtimeEventsPerWorker = 1000;
        TVector<i64> reportCounts(workerCount);
        TDynamicCounters::TCounterPtr counter;
        i64 published = 0;

        {
            // Keep each producer active during initialization and send a
            // separate batch after the counters are attached.
            TManualEvent started;
            std::atomic<ui32> starting{workerCount};
            std::atomic<ui32> remaining{workerCount};
            std::atomic<bool> countersAttached{false};
            TVector<std::thread> workers;
            Y_DEFER
            {
                countersAttached.store(true);
                for (auto& worker: workers) {
                    worker.join();
                }
            };
            for (ui32 i = 0; i != workerCount; ++i) {
                workers.emplace_back(
                    [&, i]
                    {
                        const auto reportEvent = [&]
                        {
                            ReportCriticalEventWithoutLogging(sensor);
                            ++reportCounts[i];
                        };
                        reportEvent();
                        if (--starting == 0) {
                            started.Signal();
                        }
                        while (!countersAttached.load()) {
                            reportEvent();
                            std::this_thread::yield();
                        }
                        for (ui32 j = 0; j != runtimeEventsPerWorker; ++j) {
                            reportEvent();
                        }
                        --remaining;
                    });
            }

            // Attach the counters after each producer has reported at least
            // one event. Publish intervals while producers finish reporting.
            started.WaitI();
            InitCriticalEventsCounter(counters, derivative);
            counter = counters->FindCounter(sensor);
            UNIT_ASSERT_C(counter, sensor);
            countersAttached.store(true);
            while (remaining.load()) {
                handler->UpdateStats(true);
                if (!derivative) {
                    published += counter->Val();
                }
                std::this_thread::yield();
            }
        }

        // Include the final interval in GAUGE mode and read the cumulative
        // total in RATE mode after all producers have finished.
        handler->UpdateStats(true);
        const i64 value = counter->Val();
        const i64 total =
            std::accumulate(reportCounts.begin(), reportCounts.end(), i64{0});
        UNIT_ASSERT_VALUES_EQUAL(
            total,
            derivative ? value : published + value);
    }

    // Check that concurrent AppCriticalEvents are counted exactly once.
    Y_UNIT_TEST(ShouldCountConcurrentAppEventsExactlyOnceGauge)
    {
        DoShouldCountConcurrentAppEventsExactlyOnce(/*derivative=*/false);
    }

    Y_UNIT_TEST(ShouldCountConcurrentAppEventsExactlyOnceRate)
    {
        DoShouldCountConcurrentAppEventsExactlyOnce(/*derivative=*/true);
    }

    // Check that the first process GAUGE attachment preserves startup counts
    // and reinitialization discards pending counts.
    Y_UNIT_TEST(ShouldResetProcessCountersOnReinitializationGauge)
    {
        // Start collecting process events before a root is attached.
        ResetCriticalEventsCounter();
        Y_DEFER { ResetCriticalEventsCounter(); };
        InitProcessCriticalEventsReporting();
        const auto sensor = GetCriticalEventForGetConfigsFromCmsYamlParseError();

        // Exercise reinitialization through the process counter API.
        CheckGaugeCounterReinitialization(
            [](const TDynamicCountersPtr& root)
            {
                InitCriticalEventsCounter(root, /*derivative=*/false);
            },
            [&] { ReportCriticalEventWithoutLogging(sensor); },
            [&](const TDynamicCountersPtr& root)
            {
                return root->FindCounter(sensor);
            });
    }

    // Check that storage callers without BlockStore opt-in keep RATE counters.
    Y_UNIT_TEST(ShouldPreserveDefaultStorageReporting)
    {
        ResetCriticalEventsCounter();
        auto counters = MakeIntrusive<TDynamicCounters>();
        NCloud::InitCriticalEventsCounter(counters);

        // Exercise the same shared entry points used outside BlockStore.
        for (const auto& sensor: {
                 GetCriticalEventForGetConfigsFromCmsYamlParseError(),
                 GetImpossibleEventForUnexpectedEvent()})
        {
            ReportCriticalEventWithoutLogging(sensor);
            auto counter = counters->FindCounter(sensor);
            UNIT_ASSERT(counter->ForDerivative());
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
            CreateCriticalEventsStatsHandler()->UpdateStats(true);
            UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        }
    }

    // Check process and volume counters share a publisher in every volume mode.
    Y_UNIT_TEST(ShouldPublishProcessAndVolumeEventsTogether)
    {
        for (const auto mode: {
                 NProto::APP_ONLY,
                 NProto::ALL,
                 NProto::VOLUME_ONLY})
        {
            ResetCriticalEventsCounter();
            Y_DEFER { ResetCriticalEventsCounter(); };
            InitProcessCriticalEventsReporting();
            InitVolumeCriticalEventsReportingMode(mode);
            auto app = MakeIntrusive<TDynamicCounters>();
            auto volume = MakeIntrusive<TDynamicCounters>();
            InitCriticalEventsCounter(app, /*derivative=*/false);
            InitVolumeCriticalEventsCounter(volume);

            // Report App, DiskAgent and volume events before their common tick.
            ReportGetConfigsFromCmsYamlParseError("config event");
            ReportDiskAgentConfigMismatch("agent event");
            const TVolumeLabels labels{"disk", "cloud", "folder"};
            ReportBlockDigestMismatchInBlob(labels, "disk event");
            CreateCriticalEventsStatsHandler()->UpdateStats(true);

            // Check process event counts and the selected volume destinations.
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                app->FindCounter(
                       GetCriticalEventForGetConfigsFromCmsYamlParseError())
                    ->Val());
            auto agent = app->FindCounter(
                GetCriticalEventForDiskAgentConfigMismatch());
            UNIT_ASSERT(agent);
            UNIT_ASSERT(!agent->ForDerivative());
            UNIT_ASSERT_VALUES_EQUAL(1, agent->Val());
            auto compatibility = app->FindCounter(GetAppSensorName());
            UNIT_ASSERT_VALUES_EQUAL(mode != NProto::VOLUME_ONLY, !!compatibility);
            if (compatibility) {
                UNIT_ASSERT(!compatibility->ForDerivative());
                UNIT_ASSERT_VALUES_EQUAL(1, compatibility->Val());
            }
            auto group = FindVolumeGroup(volume, labels);
            UNIT_ASSERT_VALUES_EQUAL(mode != NProto::APP_ONLY, !!group);
            if (group) {
                UNIT_ASSERT_VALUES_EQUAL(
                    1,
                    group->FindCounter(GetVolumeSensorName())->Val());
            }
        }
    }

    void DoShouldEagerlyInitCriticalEventsCounters(
        NProto::EVolumeCriticalEventsReportingMode reportingMode)
    {
        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(reportingMode);

        InitCriticalEventsCounter(criticalEventsGroup);

        // No Report has been called yet.

        auto assertInitialized = [&](const TString& sensorName)
        {
            auto msg = Sprintf(
                "reportingMode=%s, sensor=%s",
                NProto::EVolumeCriticalEventsReportingMode_Name(reportingMode)
                    .c_str(),
                sensorName.c_str());

            auto counter = criticalEventsGroup->FindCounter(sensorName);

            UNIT_ASSERT_C(counter, msg);
            UNIT_ASSERT_VALUES_EQUAL_C(0, counter->Val(), msg);
        };

#define ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED(name)                        \
    assertInitialized(GetCriticalEventFor##name());
        // ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED

        BLOCKSTORE_CRITICAL_EVENTS(ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED)
        BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
            ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED)
        BLOCKSTORE_IMPOSSIBLE_EVENTS(ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED)

#undef ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED
    }

    // InitCriticalEventsCounter eagerly initializes own
    // AppCriticalEvents/<event> counters (value 0) despite
    // TDiagnosticsConfig::VolumeCriticalEventsReportingMode
    // so they keep show up in monitoring before any event is reported.
    Y_UNIT_TEST(ShouldEagerlyInitCriticalEventsCounters)
    {
        DoShouldEagerlyInitCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::APP_ONLY);
        DoShouldEagerlyInitCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        DoShouldEagerlyInitCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::VOLUME_ONLY);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeCriticalEventsTest)
{
    void DoShouldEagerlyInitAppCriticalEventsCounters(
        NProto::EVolumeCriticalEventsReportingMode reportingMode,
        bool shouldInit)
    {
        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(reportingMode);
        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        // No Report has been called yet.

        auto assertInitialized = [&](const TString& sensorName)
        {
            auto msg = Sprintf(
                "reportingMode=%s, sensor=%s",
                NProto::EVolumeCriticalEventsReportingMode_Name(reportingMode)
                    .c_str(),
                sensorName.c_str());

            auto counter = criticalEventsGroup->FindCounter(sensorName);
            if (shouldInit) {
                UNIT_ASSERT_C(counter, msg);
                UNIT_ASSERT_VALUES_EQUAL_C(0, counter->Val(), msg);
            } else {
                UNIT_ASSERT_C(!counter, msg);
            }
        };

#define ASSERT_APP_CRITICAL_EVENT_COUNTER_INITIALIZED(name)                    \
    assertInitialized(GetAppCriticalEventFor##name());
        // ASSERT_APP_CRITICAL_EVENT_COUNTER_INITIALIZED

        BLOCKSTORE_VOLUME_CRITICAL_EVENTS(
            ASSERT_APP_CRITICAL_EVENT_COUNTER_INITIALIZED)

#undef ASSERT_APP_CRITICAL_EVENT_COUNTER_INITIALIZED
    }

    // InitCriticalEventsCounter/InitVolumeCriticalEventsCounter should
    // eagerly initialize the per-host AppCriticalEvents/<event>
    // counters (value 0) for disk critical events only with
    // TDiagnosticsConfig::VolumeCriticalEventsReportingMode != VOLUME_ONLY
    // so they keep show up in monitoring before any event is reported.
    Y_UNIT_TEST(ShouldEagerlyInitAppCriticalEventsCounters)
    {
        DoShouldEagerlyInitAppCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::APP_ONLY,
            /*shouldInit=*/true);
        DoShouldEagerlyInitAppCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::ALL,
            /*shouldInit=*/true);
        DoShouldEagerlyInitAppCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::VOLUME_ONLY,
            /*shouldInit=*/false);
    }

    void DoShouldReportCriticalEventsAccordingToReportingMode(
        NProto::EVolumeCriticalEventsReportingMode reportingMode,
        bool shouldReportApp,
        bool shouldReportVolume)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(reportingMode);
        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeLabels v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        ReportBlockDigestMismatchInBlob(v, "some msg");

        auto msg = Sprintf(
            "reportingMode=%s",
            NProto::EVolumeCriticalEventsReportingMode_Name(reportingMode)
                .c_str());

        auto appCounter = criticalEventsGroup->FindCounter(GetAppSensorName());
        if (shouldReportApp) {
            UNIT_ASSERT_C(appCounter, msg);
            UNIT_ASSERT_VALUES_EQUAL_C(1, appCounter->Val(), msg);
        } else {
            UNIT_ASSERT_C(!appCounter, msg);
        }

        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        auto volumeCounter =
            volumeGroup ? volumeGroup->FindCounter(GetVolumeSensorName())
                        : nullptr;
        if (shouldReportVolume) {
            UNIT_ASSERT_C(volumeCounter, msg);
            UNIT_ASSERT_VALUES_EQUAL_C(1, volumeCounter->Val(), msg);
        } else {
            UNIT_ASSERT_C(!volumeCounter, msg);
        }
    }

    Y_UNIT_TEST(ShouldReportCriticalEventsAccordingToReportingMode)
    {
        DoShouldReportCriticalEventsAccordingToReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::APP_ONLY,
            /*shouldReportApp=*/true,
            /*shouldReportVolume=*/false);
        DoShouldReportCriticalEventsAccordingToReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL,
            /*shouldReportApp=*/true,
            /*shouldReportVolume=*/true);
        DoShouldReportCriticalEventsAccordingToReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::VOLUME_ONLY,
            /*shouldReportApp=*/false,
            /*shouldReportVolume=*/true);
    }

    // Check App RATE increments and interval Volume GAUGE publication.
    Y_UNIT_TEST(ShouldEmitPerDiskCountersForVolumeCriticalEvents)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeLabels v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        auto ret1 = ReportBlockDigestMismatchInBlob(v, "some msg");

        ReportBlockDigestMismatchInBlob(v, "some msg");

        // The returned log line carries the per-disk prefix.
        UNIT_ASSERT_STRING_CONTAINS(ret1, "disk-1");
        UNIT_ASSERT_STRING_CONTAINS(ret1, "cloud-1");
        UNIT_ASSERT_STRING_CONTAINS(ret1, "folder-1");

        // Check that counts accumulate before the Volume GAUGE is created.
        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(
            !volumeGroup || !volumeGroup->FindCounter(GetVolumeSensorName()));

        // Check immediate increments of the App RATE counter.
        auto appCounter = criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(appCounter);

        UNIT_ASSERT_VALUES_EQUAL(2, appCounter->Val());

        // Publish the accumulated count in the new Volume GAUGE counter.
        handler->UpdateStats(true);

        volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);

        UNIT_ASSERT_VALUES_EQUAL(2, volumeCounter->Val());

        // Per-host counter is not changed after update/publish.
        UNIT_ASSERT_VALUES_EQUAL(2, appCounter->Val());
    }

    // The publish only runs when updateIntervalFinished is true
    Y_UNIT_TEST(ShouldPublishOnlyOnIntervalFinished)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeLabels v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        ReportBlockDigestMismatchInBlob(v, "some msg");

        // Tick without the interval finished -> no publish.
        handler->UpdateStats(false);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(
            !volumeGroup || !volumeGroup->FindCounter(GetVolumeSensorName()));

        // Interval finished -> publish writes 1.
        handler->UpdateStats(true);

        volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);
        UNIT_ASSERT_VALUES_EQUAL(1, volumeCounter->Val());
    }

    // Check that publication creates counters for reported Volume events.
    Y_UNIT_TEST(ShouldNotCreateUnaffectedEventsMetricsOnPublish)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeLabels v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        ReportBlockDigestMismatchInBlob(v, "some msg");

        // Interval finished -> publish affected metric
        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);
        UNIT_ASSERT_VALUES_EQUAL(1, volumeCounter->Val());

        // Check that counter creation follows the set of reported events.
        UNIT_ASSERT(!volumeGroup->FindCounter(
            GetVolumeCriticalEventForMigrationFailed()));
        UNIT_ASSERT(!volumeGroup->FindCounter(
            GetVolumeCriticalEventForMirroredDiskMajorityChecksumMismatch()));
        UNIT_ASSERT(!volumeGroup->FindCounter(
            GetVolumeCriticalEventForOverlappingRequestsDetected()));
    }

    // Check that publication sets the GAUGE to zero for an empty interval.
    Y_UNIT_TEST(ShouldResetToZeroAfterFlushWithNoNewEvents)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeLabels v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        ReportBlockDigestMismatchInBlob(v, "some msg");
        ReportBlockDigestMismatchInBlob(v, "some msg");

        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);

        UNIT_ASSERT_VALUES_EQUAL(2, volumeCounter->Val());

        // No new events -> Unpublished is 0 -> GAUGE set back to 0.
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(0, volumeCounter->Val());
    }

    // Counters are distinct per disk.
    // Per-host counters contain summary
    Y_UNIT_TEST(ShouldKeepDistinctCountersPerDisk)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeLabels v1{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        const TVolumeLabels v2{
            .DiskId = "disk-2",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        ReportBlockDigestMismatchInBlob(v1, "some msg 1");
        ReportBlockDigestMismatchInBlob(v1, "some msg 1");
        ReportBlockDigestMismatchInBlob(v2, "some msg 2");

        handler->UpdateStats(true);

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            FindVolumeGroup(volumeCriticalEventsGroup, v1)
                ->FindCounter(GetVolumeSensorName())
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            FindVolumeGroup(volumeCriticalEventsGroup, v2)
                ->FindCounter(GetVolumeSensorName())
                ->Val());

        // Per-host counter should contain summary
        auto appCounter = criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(appCounter);
        UNIT_ASSERT_VALUES_EQUAL(3, appCounter->Val());
    }

    // Events fired before VolumeCountersRoot is set must accumulate in
    // Unpublished and be published once the root becomes available
    Y_UNIT_TEST(ShouldAccumulateEventsBeforeCountersRootInitialized)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        // NOTE: InitVolumeCriticalEventsCounter is intentionally deferred.
        InitCriticalEventsCounter(criticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeLabels v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        // Accumulate in Unpublished until VolumeCountersRoot is initialized.
        for (int i = 0; i < 3; ++i) {
            ReportBlockDigestMismatchInBlob(v, "some msg");
        }

        // Attach the root and publish the accumulated count in a new GAUGE.
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);
        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);

        UNIT_ASSERT_VALUES_EQUAL(3, volumeCounter->Val());

        // Per-host counter reflects the dual emission (3 synchronous Inc()'s).
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            criticalEventsGroup->FindCounter(GetAppSensorName())->Val());

        // Accumulate another event and publish its count in the existing GAUGE.
        ReportBlockDigestMismatchInBlob(v, "some msg");
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, volumeCounter->Val());
    }

    // Check that the first volume attachment preserves startup counts and
    // reinitialization discards pending counts.
    Y_UNIT_TEST(ShouldResetVolumeCountersOnReinitialization)
    {
        // Enable only per-volume reporting for this scenario.
        ResetCriticalEventsCounter();
        InitVolumeCriticalEventsReportingMode(NProto::VOLUME_ONLY);
        Y_DEFER
        {
            ResetCriticalEventsCounter();
            InitVolumeCriticalEventsReportingMode(NProto::APP_ONLY);
        };
        const TVolumeLabels labels{"disk", "cloud", "folder"};
        const auto sensor = GetVolumeSensorName();

        // Exercise reinitialization through the volume counter API.
        CheckGaugeCounterReinitialization(
            InitVolumeCriticalEventsCounter,
            [&] { ReportBlockDigestMismatchInBlob(labels, "test event"); },
            [&](const TDynamicCountersPtr& root)
            {
                const auto group = FindVolumeGroup(root, labels);
                return group ? group->FindCounter(sensor)
                             : TDynamicCounters::TCounterPtr{};
            });
    }

    // The next two tests report AppImpossibleEvents, which causes crash in
    // debug builds ('debug', 'relwithdebinfo'), but they shall be executed
    // within sanitizer builds (which are 'release' builds)
#ifdef NDEBUG
    // A null labels pointer should report the bug and preserve the original
    // critical event.
    Y_UNIT_TEST(ShouldGracefullyReportNullVolumeLabels)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        auto bugCounter =
            criticalEventsGroup->FindCounter(GetCriticalEventForBug());
        UNIT_ASSERT(bugCounter);
        bugCounter->Set(0);
        UNIT_ASSERT_VALUES_EQUAL(0, bugCounter->Val());

        const TVolumeLabelsConstPtr volumeLabels;
        const auto logMessage =
            ReportBlockDigestMismatchInBlob(volumeLabels, "some msg");

        UNIT_ASSERT_VALUES_EQUAL(1, bugCounter->Val());

        auto appCounter = criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(appCounter);
        UNIT_ASSERT_VALUES_EQUAL(1, appCounter->Val());

        UNIT_ASSERT_STRING_CONTAINS(logMessage, "disk:<nullptr>");
        UNIT_ASSERT_STRING_CONTAINS(logMessage, "cloud:<empty>");
        UNIT_ASSERT_STRING_CONTAINS(logMessage, "folder:<empty>");

        handler->UpdateStats(true);

        // No metrics must be created for empty diskId
        UNIT_ASSERT(volumeCriticalEventsGroup->ReadSnapshot().empty());
    }

    // An empty diskId should report the bug and preserve the original critical
    // event.
    Y_UNIT_TEST(ShouldGracefullyReportEmptyDiskId)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        auto bugCounter =
            criticalEventsGroup->FindCounter(GetCriticalEventForBug());
        UNIT_ASSERT(bugCounter);
        bugCounter->Set(0);
        UNIT_ASSERT_VALUES_EQUAL(0, bugCounter->Val());

        const TVolumeLabels volumeLabels;
        const auto logMessage =
            ReportBlockDigestMismatchInBlob(volumeLabels, "some msg");

        UNIT_ASSERT_VALUES_EQUAL(1, bugCounter->Val());

        auto appCounter = criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(appCounter);
        UNIT_ASSERT_VALUES_EQUAL(1, appCounter->Val());

        UNIT_ASSERT_STRING_CONTAINS(logMessage, "disk:<empty>");
        UNIT_ASSERT_STRING_CONTAINS(logMessage, "cloud:<empty>");
        UNIT_ASSERT_STRING_CONTAINS(logMessage, "folder:<empty>");

        handler->UpdateStats(true);

        // No metrics must be created for empty diskId
        UNIT_ASSERT(volumeCriticalEventsGroup->ReadSnapshot().empty());
    }
#endif   // NDEBUG

    // All Report...() overloads works
    Y_UNIT_TEST(ShouldProperlyImplementAllReportOverloads)
    {
        ResetCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup =
            root->GetSubgroup("component", AppCriticalEventsComponent.data());
        auto volumeCriticalEventsGroup = root->GetSubgroup(
            "component",
            VolumeCriticalEventsComponent.data());

        InitVolumeCriticalEventsReportingMode(
            NProto::EVolumeCriticalEventsReportingMode::ALL);
        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TString diskId = "disk-1";
        const TString cloudId = "cloud-1";
        const TString folderId = "folder-1";

        const TString msg = "some msg";
        const auto params = TCritEventParams{{"a", "1"}, {"b", "2"}};

        // Report...(TString, TString, TString, ...)
        {
            ReportBlockDigestMismatchInBlob(diskId, cloudId, folderId);
            ReportBlockDigestMismatchInBlob(diskId, cloudId, folderId, msg);
            ReportBlockDigestMismatchInBlob(diskId, cloudId, folderId, params);
            ReportBlockDigestMismatchInBlob(
                diskId,
                cloudId,
                folderId,
                msg,
                params);
        }

        // Report...(TVolumeLabels, ...)
        {
            const auto v = TVolumeLabels{
                .DiskId = diskId,
                .CloudId = cloudId,
                .FolderId = folderId};

            ReportBlockDigestMismatchInBlob(v);
            ReportBlockDigestMismatchInBlob(v, msg);
            ReportBlockDigestMismatchInBlob(v, params);
            ReportBlockDigestMismatchInBlob(v, msg, params);
        }

        // Report...(TVolumeLabelsConstPtr, ...)
        {
            const auto v = MakeVolumeLabels(diskId, cloudId, folderId);

            ReportBlockDigestMismatchInBlob(v);
            ReportBlockDigestMismatchInBlob(v, msg);
            ReportBlockDigestMismatchInBlob(v, params);
            ReportBlockDigestMismatchInBlob(v, msg, params);
        }

        const auto v = TVolumeLabels{
            .DiskId = diskId,
            .CloudId = cloudId,
            .FolderId = folderId};

        // Per-host counter should contain summary immediately.
        auto appCounter = criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(appCounter);
        UNIT_ASSERT_VALUES_EQUAL(12, appCounter->Val());

        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);

        UNIT_ASSERT_VALUES_EQUAL(12, volumeCounter->Val());

        // Per-host counter is not changed after update/publish.
        UNIT_ASSERT_VALUES_EQUAL(12, appCounter->Val());
    }
}

}   // namespace NCloud::NBlockStore
