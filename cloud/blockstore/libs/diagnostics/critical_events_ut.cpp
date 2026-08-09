#include "critical_events.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NMonitoring;

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
    const TVolumeId& v)
{
    return FindNestedGroup(
        serviceVolumeGroup,
        {{"volume", v.DiskId}, {"cloud", v.CloudId}, {"folder", v.FolderId}});
}

TString GetVolumeSensorName()
{
    return GetVolumeCriticalEventForBlockDigestMismatchInBlob();
}

TString GetDeprecatedSensorName()
{
    return GetDeprecatedCriticalEventForBlockDigestMismatchInBlob();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCriticalEventsTest)
{
    // InitCriticalEventsCounter eagerly initializes the deprecated
    // AppCriticalEvents/<event> counters (value 0) so they keep show up in
    // monitoring before any event is reported.
    Y_UNIT_TEST(ShouldEagerlyInitCriticalEventsCounters)
    {
        auto root = MakeIntrusive<TDynamicCounters>();
        auto serverGroup = root->GetSubgroup("component", "server");

        // No Report has been called yet.
        InitCriticalEventsCounter(serverGroup);

        auto assertInitialized = [&](const TString& sensorName)
        {
            auto counter = serverGroup->FindCounter(sensorName);
            UNIT_ASSERT_C(counter, sensorName);
            UNIT_ASSERT_VALUES_EQUAL_C(0, counter->Val(), sensorName);
        };

#define ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED(name) \
    assertInitialized(GetCriticalEventFor##name());
        // ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED

        BLOCKSTORE_CRITICAL_EVENTS(ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED)
        BLOCKSTORE_DISK_AGENT_CRITICAL_EVENTS(
            ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED)
        BLOCKSTORE_IMPOSSIBLE_EVENTS(ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED)

#undef ASSERT_CRITICAL_EVENT_COUNTER_INITIALIZED

#define ASSERT_DEPRECATED_CRITICAL_EVENT_COUNTER_INITIALIZED(name) \
    assertInitialized(GetDeprecatedCriticalEventFor##name());
        // ASSERT_DEPRECATED_CRITICAL_EVENT_COUNTER_INITIALIZED

        // deprecated: keeps existing AppCriticalEvents/ * for new
        // VolumeCriticalEvents/ * metrics alive
        BLOCKSTORE_VOLUME_CRITICAL_EVENTS(
            ASSERT_DEPRECATED_CRITICAL_EVENT_COUNTER_INITIALIZED)

#undef ASSERT_DEPRECATED_CRITICAL_EVENT_COUNTER_INITIALIZED
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeCriticalEventsTest)
{
    // Per-disk GAUGE counters are emitted by the shadow-swap publish,
    // while the deprecated AppCriticalEvents/* counter is bumped synchronously
    Y_UNIT_TEST(ShouldEmitPerDiskCountersForVolumeCriticalEvents)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup = root->GetSubgroup("component", "server");
        auto volumeCriticalEventsGroup =
            root->GetSubgroup("component", "server");

        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeId v{
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

        // Deprecated counter is bumped synchronously.
        auto deprecatedCounter =
            criticalEventsGroup->FindCounter(GetDeprecatedSensorName());
        UNIT_ASSERT(deprecatedCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, deprecatedCounter->Val());

        // Flush materializes and writes the GAUGE.
        handler->UpdateStats(true);

        volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetVolumeSensorName());
        UNIT_ASSERT(volumeCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, volumeCounter->Val());

        // Deprecated counter is not changed after update/publish.
        UNIT_ASSERT_VALUES_EQUAL(2, deprecatedCounter->Val());
    }

    // The publish only runs when updateIntervalFinished is true
    Y_UNIT_TEST(ShouldFlushOnlyOnIntervalFinished)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup = root->GetSubgroup("component", "server");
        auto volumeCriticalEventsGroup =
            root->GetSubgroup("component", "server");

        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeId v{
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

    // GAUGE semantics: with no new events the next flush resets to 0
    Y_UNIT_TEST(ShouldResetToZeroAfterFlushWithNoNewEvents)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup = root->GetSubgroup("component", "server");
        auto volumeCriticalEventsGroup =
            root->GetSubgroup("component", "server");

        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeId v{
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

    // Counters are distinct per disk
    Y_UNIT_TEST(ShouldKeepDistinctCountersPerDisk)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup = root->GetSubgroup("component", "server");
        auto volumeCriticalEventsGroup =
            root->GetSubgroup("component", "server");

        InitCriticalEventsCounter(criticalEventsGroup);
        InitVolumeCriticalEventsCounter(volumeCriticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeId v1{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        const TVolumeId v2{
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

        // Deprecated counter should contain summary
        auto deprecatedCounter =
            criticalEventsGroup->FindCounter(GetDeprecatedSensorName());
        UNIT_ASSERT(deprecatedCounter);
        UNIT_ASSERT_VALUES_EQUAL(3, deprecatedCounter->Val());
    }

    // Events fired before CountersRoot is set must accumulate in
    // Unpublished and be published once the root becomes available
    Y_UNIT_TEST(ShouldAccumulateEventsBeforeCountersRootInitialized)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto criticalEventsGroup = root->GetSubgroup("component", "server");
        auto volumeCriticalEventsGroup =
            root->GetSubgroup("component", "server");

        // NOTE: InitVolumeCriticalEventsCounter is intentionally deferred.
        InitCriticalEventsCounter(criticalEventsGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeId v{
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

        // Deprecated counter reflects the dual emission (3 synchronous
        // Inc()'s).
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            criticalEventsGroup->FindCounter(GetDeprecatedSensorName())->Val());

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
        auto criticalEventsGroup = root->GetSubgroup("component", "server");
        auto volumeCriticalEventsGroup =
            root->GetSubgroup("component", "server");

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

        // Report...(TVolumeId, ...)
        {
            const auto v = TVolumeId{
                .DiskId = diskId,
                .CloudId = cloudId,
                .FolderId = folderId};

            ReportBlockDigestMismatchInBlob(v);
            ReportBlockDigestMismatchInBlob(v, msg);
            ReportBlockDigestMismatchInBlob(v, params);
            ReportBlockDigestMismatchInBlob(v, msg, params);
        }

        // Report...(TVolumeIdConstPtr, ...)
        {
            const auto v = MakeVolumeId(diskId, cloudId, folderId);

            ReportBlockDigestMismatchInBlob(v);
            ReportBlockDigestMismatchInBlob(v, msg);
            ReportBlockDigestMismatchInBlob(v, params);
            ReportBlockDigestMismatchInBlob(v, msg, params);
        }

        const auto v = TVolumeId{
            .DiskId = diskId,
            .CloudId = cloudId,
            .FolderId = folderId};

        auto deprecatedCounter =
            criticalEventsGroup->FindCounter(GetDeprecatedSensorName());
        UNIT_ASSERT(deprecatedCounter);
        // Deprecated counter should contain summary immediately
        UNIT_ASSERT_VALUES_EQUAL(12, deprecatedCounter->Val());

        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(volumeCriticalEventsGroup, v);
        UNIT_ASSERT(volumeGroup);
        UNIT_ASSERT_VALUES_EQUAL(
            12,
            volumeGroup->FindCounter(GetVolumeSensorName())->Val());

        UNIT_ASSERT_VALUES_EQUAL(12, deprecatedCounter->Val());
    }
}

}   // namespace NCloud::NBlockStore
