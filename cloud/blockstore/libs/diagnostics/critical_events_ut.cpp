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

#include <atomic>
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCriticalEventsTest)
{
    // Check that GAUGE values are held until the next interval publication.
    void DoShouldPublishProcessEventsPerInterval(
        const TString& sensorName,
        TString (*report)(const TString&))
    {
        ResetCriticalEventsCounter();
        Y_DEFER { ResetCriticalEventsCounter(); };
        InitProcessCriticalEventsReporting();
        auto counters = MakeIntrusive<TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto handler = CreateCriticalEventsStatsHandler();
        auto counter = counters->FindCounter(sensorName);
        UNIT_ASSERT_C(counter, sensorName);
        UNIT_ASSERT(!counter->ForDerivative());

        // Accumulate event counts until the publication interval ends.
        report("first event");
        report("second event");
        handler->UpdateStats(false);
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());

        // Publish the event count and retain it while new events accumulate.
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(2, counter->Val());
        report("next interval");
        UNIT_ASSERT_VALUES_EQUAL(2, counter->Val());

        // Publish the next interval's count, then zero for an empty interval.
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
    }

    // Check interval publication through both BlockStore and storage wrappers.
    Y_UNIT_TEST(ShouldPublishAppCriticalEventsPerInterval)
    {
        DoShouldPublishProcessEventsPerInterval(
            GetCriticalEventForRdmaError(),
            ReportRdmaError);
        DoShouldPublishProcessEventsPerInterval(
            GetCriticalEventForGetConfigsFromCmsYamlParseError(),
            ReportGetConfigsFromCmsYamlParseError);
    }

#ifdef NDEBUG
    // Check interval GAUGE publication for impossible events.
    Y_UNIT_TEST(ShouldPublishAppImpossibleEventsPerInterval)
    {
        DoShouldPublishProcessEventsPerInterval(
            GetCriticalEventForBug(),
            ReportBug);
        DoShouldPublishProcessEventsPerInterval(
            GetImpossibleEventForUnexpectedEvent(),
            ReportUnexpectedEvent);
    }
#endif

    // Check that early event counts survive repeated counter initialization.
    void DoShouldKeepEarlyProcessEvents(
        const TString& sensorName,
        TString (*report)(const TString&))
    {
        ResetCriticalEventsCounter();
        TStringStream log;
        SetCriticalEventsLog(TLog(MakeHolder<TStreamLogBackend>(&log)));
        Y_DEFER {
            SetCriticalEventsLog(TLog());
            ResetCriticalEventsCounter();
        };
        InitProcessCriticalEventsReporting();
        auto handler = CreateCriticalEventsStatsHandler();

        // Log immediately and retain the count until monitoring is initialized.
        const auto message = report("startup event");
        UNIT_ASSERT_STRING_CONTAINS(log.Str(), message);
        const TString logged = log.Str();
        handler->UpdateStats(true);
        handler->UpdateStats(true);

        // Attach monitoring and publish the accumulated event count.
        auto counters = MakeIntrusive<TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        InitCriticalEventsCounter(counters);
        handler->UpdateStats(true);
        auto counter = counters->FindCounter(sensorName);
        UNIT_ASSERT(counter);
        UNIT_ASSERT(!counter->ForDerivative());
        UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        UNIT_ASSERT_VALUES_EQUAL(logged, log.Str());

        // Preserve the published value on init, then publish an empty interval.
        InitCriticalEventsCounter(counters);
        UNIT_ASSERT_VALUES_EQUAL(1, counter->Val());
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(0, counter->Val());
    }

    // Check early accumulation for the shared CMS event and a BlockStore event.
    Y_UNIT_TEST(ShouldKeepEarlyAppCriticalEvents)
    {
        DoShouldKeepEarlyProcessEvents(
            GetCriticalEventForGetConfigsFromCmsYamlParseError(),
            ReportGetConfigsFromCmsYamlParseError);
        DoShouldKeepEarlyProcessEvents(
            GetCriticalEventForRdmaError(),
            ReportRdmaError);
    }

#ifdef NDEBUG
    // Check early accumulation for impossible events from both event lists.
    Y_UNIT_TEST(ShouldKeepEarlyAppImpossibleEvents)
    {
        DoShouldKeepEarlyProcessEvents(GetCriticalEventForBug(), ReportBug);
        DoShouldKeepEarlyProcessEvents(
            GetImpossibleEventForUnexpectedEvent(),
            ReportUnexpectedEvent);
    }
#endif

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

    // Check interval publication for DiskAgent configuration and session errors.
    Y_UNIT_TEST(ShouldPublishDiskAgentCriticalEventsPerInterval)
    {
        DoShouldPublishProcessEventsPerInterval(
            GetCriticalEventForDiskAgentConfigMismatch(),
            ReportDiskAgentConfigMismatch);
        DoShouldPublishProcessEventsPerInterval(
            GetCriticalEventForDiskAgentSessionCacheRestoreError(),
            ReportDiskAgentSessionCacheRestoreError);
    }

    // Check that early DiskAgent event counts survive counter initialization.
    Y_UNIT_TEST(ShouldKeepEarlyDiskAgentCriticalEvents)
    {
        DoShouldKeepEarlyProcessEvents(
            GetCriticalEventForDiskAgentConfigMismatch(),
            ReportDiskAgentConfigMismatch);
        DoShouldKeepEarlyProcessEvents(
            GetCriticalEventForDiskAgentSessionCacheRestoreError(),
            ReportDiskAgentSessionCacheRestoreError);
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
            InitCriticalEventsCounter(app);
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

    // Check event counts across concurrent reporting and interval publication.
    Y_UNIT_TEST(ShouldCountConcurrentAppEventsExactlyOnce)
    {
        ResetCriticalEventsCounter();
        Y_DEFER { ResetCriticalEventsCounter(); };
        InitProcessCriticalEventsReporting();
        auto counters = MakeIntrusive<TDynamicCounters>();
        InitCriticalEventsCounter(counters);
        auto handler = CreateCriticalEventsStatsHandler();
        const auto sensor = GetCriticalEventForGetConfigsFromCmsYamlParseError();
        auto counter = counters->FindCounter(sensor);

        // Race multiple producers with the single periodic publisher.
        std::atomic<ui32> remaining{4};
        TVector<std::thread> workers;
        for (ui32 i = 0; i != 4; ++i) {
            workers.emplace_back([&] {
                for (ui32 j = 0; j != 1000; ++j) {
                    ReportCriticalEventWithoutLogging(sensor);
                }
                --remaining;
            });
        }
        ui64 published = 0;
        while (remaining.load()) {
            handler->UpdateStats(true);
            published += counter->Val();
            std::this_thread::yield();
        }

        // Include the final partial interval after all producers have stopped.
        for (auto& worker: workers) {
            worker.join();
        }
        handler->UpdateStats(true);
        published += counter->Val();
        UNIT_ASSERT_VALUES_EQUAL(4000, published);
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
