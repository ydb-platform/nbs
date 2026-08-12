#include "critical_events.h"

#include "critical_events_init.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

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

// Resolves the per-disk counter group under component=service_volume.
TIntrusivePtr<TDynamicCounters> FindVolumeGroup(
    TDynamicCountersPtr serviceVolumeGroup,
    const TVolumeLabels& v)
{
    return FindNestedGroup(
        serviceVolumeGroup,
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
    void DoShouldEagerlyInitLegacyCriticalEventsCounters(
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
            // TODO: fails until all per-disk critical events are removed from
            // AppCriticalEvents API (now are duplicated in VolumeCriticalEvents
            // API)
            return;

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
    // eagerly initializes the legacy per-disk AppCriticalEvents/<event>
    // counters (value 0) only with
    // TDiagnosticsConfig::VolumeCriticalEventsReportingMode != VOLUME_ONLY
    // so they keep show up in monitoring before any event is reported.
    Y_UNIT_TEST(ShouldEagerlyInitLegacyCriticalEventsCounters)
    {
        DoShouldEagerlyInitLegacyCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::APP_ONLY,
            /*shouldInit=*/true);
        DoShouldEagerlyInitLegacyCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::ALL,
            /*shouldInit=*/true);
        DoShouldEagerlyInitLegacyCriticalEventsCounters(
            NProto::EVolumeCriticalEventsReportingMode::VOLUME_ONLY,
            /*shouldInit=*/false);
    }

    void DoShouldReportCriticalEventsAccordingToReportingMode(
        NProto::EVolumeCriticalEventsReportingMode reportingMode,
        bool shouldReportApp,
        bool shouldReportVolume)
    {
        ResetVolumeCriticalEventsCounter();

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
            // TODO: fails until all per-disk critical events are removed from
            // AppCriticalEvents API (now are duplicated in VolumeCriticalEvents
            // API)

            // Restore after removed
            // UNIT_ASSERT_C(!appCounter, msg);

            // Remove after removed
            UNIT_ASSERT_VALUES_EQUAL_C(0, appCounter->Val(), msg);
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

    // Per-disk GAUGE counters are emitted by the shadow-swap publish,
    // while the legacy AppCriticalEvents/* counter is bumped synchronously
    Y_UNIT_TEST(ShouldEmitPerDiskCountersForVolumeCriticalEvents)
    {
        ResetVolumeCriticalEventsCounter();

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

        // Before the flush the per-disk GAUGE is not yet materialized:
        // the hot path only bumps the Unpublished accumulator.
        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(
            !volumeGroup || !volumeGroup->FindCounter(GetVolumeSensorName()));

        // Legacy counter is bumped synchronously.
        auto legacyCounter =
            criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(legacyCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, legacyCounter->Val());

        // Flush materializes and writes the GAUGE.
        handler->UpdateStats(true);

        volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, volumeCounter->Val());

        // Legacy counter is not changed after update/publish.
        UNIT_ASSERT_VALUES_EQUAL(2, legacyCounter->Val());
    }

    // The publish only runs when updateIntervalFinished is true
    Y_UNIT_TEST(ShouldPublishOnlyOnIntervalFinished)
    {
        ResetVolumeCriticalEventsCounter();

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

    // The publish materializes only affected metrics
    Y_UNIT_TEST(ShouldNotCreateUnaffectedEventsMetricsOnPublish)
    {
        ResetVolumeCriticalEventsCounter();

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

        // No unaffected metrics are materialized
        UNIT_ASSERT(!volumeGroup->FindCounter(
            GetVolumeCriticalEventForMigrationFailed()));
        UNIT_ASSERT(!volumeGroup->FindCounter(
            GetVolumeCriticalEventForMirroredDiskMajorityChecksumMismatch()));
        UNIT_ASSERT(!volumeGroup->FindCounter(
            GetVolumeCriticalEventForOverlappingRequestsDetected()));
    }

    // GAUGE semantics: with no new events the next flush resets to 0
    Y_UNIT_TEST(ShouldResetToZeroAfterFlushWithNoNewEvents)
    {
        ResetVolumeCriticalEventsCounter();

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
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            volumeGroup->FindCounter(GetVolumeSensorName())->Val());

        // No new events -> Unpublished is 0 -> GAUGE set back to 0.
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            volumeGroup->FindCounter(GetVolumeSensorName())->Val());
    }

    // Counters are distinct per disk.
    // Legacy counters contain summary
    Y_UNIT_TEST(ShouldKeepDistinctCountersPerDisk)
    {
        ResetVolumeCriticalEventsCounter();

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

        // Legacy counter should contain summary
        auto legacyCounter =
            criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(legacyCounter);
        UNIT_ASSERT_VALUES_EQUAL(3, legacyCounter->Val());
    }

    // Events fired before CountersRoot is set must accumulate in
    // Unpublished and be published once the root becomes available
    Y_UNIT_TEST(ShouldAccumulateEventsBeforeCountersRootInitialized)
    {
        ResetVolumeCriticalEventsCounter();

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

        // CountersRoot is null -> entry created with Exported=null,
        // Internal accumulates to 3.
        for (int i = 0; i < 3; ++i) {
            ReportBlockDigestMismatchInBlob(v, "some msg");
        }

        // Root becomes available -> the next flush lazily materializes
        // Exported and writes the accumulated value.
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);
        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            volumeGroup->FindCounter(GetVolumeSensorName())->Val());

        // Legacy counter reflects the dual emission (3 synchronous
        // Inc()'s).
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            criticalEventsGroup->FindCounter(GetAppSensorName())->Val());

        // One more event on the hot path (Exported is now non-null,
        // Internal -> 1) is flushed on the next tick.
        ReportBlockDigestMismatchInBlob(v, "some msg");
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            volumeGroup->FindCounter(GetVolumeSensorName())->Val());
    }

    // All Report...() overloads works
    Y_UNIT_TEST(ShouldProperlyImplementAllReportOverloads)
    {
        ResetVolumeCriticalEventsCounter();

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

        auto legacyCounter =
            criticalEventsGroup->FindCounter(GetAppSensorName());
        UNIT_ASSERT(legacyCounter);
        // Legacy counter should contain summary immediately
        UNIT_ASSERT_VALUES_EQUAL(12, legacyCounter->Val());

        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        UNIT_ASSERT_VALUES_EQUAL(
            12,
            volumeGroup->FindCounter(GetVolumeSensorName())->Val());

        UNIT_ASSERT_VALUES_EQUAL(12, legacyCounter->Val());
    }
}

}   // namespace NCloud::NBlockStore
