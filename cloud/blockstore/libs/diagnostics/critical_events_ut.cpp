#include "critical_events.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

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

TString GetSensorName()
{
    return GetCriticalEventForBlockDigestMismatchInBlob();
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
    // Per-disk GAUGE counters are emitted by the shadow-swap flush,
    // while the deprecated AppCriticalEvents/* counter (under
    // component=server) is bumped synchronously
    Y_UNIT_TEST(ShouldEmitPerDiskCountersForVolumeCriticalEvents)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto serverGroup = root->GetSubgroup("component", "server");
        auto serviceVolumeGroup =
            root->GetSubgroup("component", "service_volume");

        InitCriticalEventsCounter(serverGroup);
        InitVolumeCriticalEventsCounter(serviceVolumeGroup);

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
        // the hot path only bumps the Internal accumulator.
        auto volumeGroup = FindVolumeGroup(serviceVolumeGroup, v);
        UNIT_ASSERT(!volumeGroup || !volumeGroup->FindCounter(GetSensorName()));

        // Deprecated counter is bumped synchronously.
        auto deprecatedCounter =
            serverGroup->FindCounter(GetDeprecatedSensorName());
        UNIT_ASSERT(deprecatedCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, deprecatedCounter->Val());

        // Flush materializes and writes the GAUGE.
        handler->UpdateStats(true);

        volumeGroup = FindVolumeGroup(serviceVolumeGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetSensorName());
        UNIT_ASSERT(volumeCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, volumeCounter->Val());

        // Deprecated counter is not changed after update/flush.
        UNIT_ASSERT_VALUES_EQUAL(2, deprecatedCounter->Val());
    }

    // The flush only runs when updateIntervalFinished is true
    Y_UNIT_TEST(ShouldFlushOnlyOnIntervalFinished)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto serverGroup = root->GetSubgroup("component", "server");
        auto serviceVolumeGroup =
            root->GetSubgroup("component", "service_volume");

        InitCriticalEventsCounter(serverGroup);
        InitVolumeCriticalEventsCounter(serviceVolumeGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeId v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        ReportBlockDigestMismatchInBlob(v, "some msg");

        // Tick without the interval finished -> no flush.
        handler->UpdateStats(false);

        auto volumeGroup = FindVolumeGroup(serviceVolumeGroup, v);
        UNIT_ASSERT(!volumeGroup || !volumeGroup->FindCounter(GetSensorName()));

        // Interval finished -> flush writes 1.
        handler->UpdateStats(true);

        volumeGroup = FindVolumeGroup(serviceVolumeGroup, v);
        UNIT_ASSERT(volumeGroup);
        auto volumeCounter = volumeGroup->FindCounter(GetSensorName());
        UNIT_ASSERT(volumeCounter);
        UNIT_ASSERT_VALUES_EQUAL(1, volumeCounter->Val());
    }

    // GAUGE semantics: with no new events the next flush resets to 0
    Y_UNIT_TEST(ShouldResetToZeroAfterFlushWithNoNewEvents)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto serverGroup = root->GetSubgroup("component", "server");
        auto serviceVolumeGroup =
            root->GetSubgroup("component", "service_volume");

        InitCriticalEventsCounter(serverGroup);
        InitVolumeCriticalEventsCounter(serviceVolumeGroup);

        auto handler = CreateCriticalEventsStatsHandler();

        const TVolumeId v{
            .DiskId = "disk-1",
            .CloudId = "cloud-1",
            .FolderId = "folder-1"};

        ReportBlockDigestMismatchInBlob(v, "some msg");

        ReportBlockDigestMismatchInBlob(v, "some msg");

        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(serviceVolumeGroup, v);
        UNIT_ASSERT(volumeGroup);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            volumeGroup->FindCounter(GetSensorName())->Val());

        // No new events -> Internal is 0 -> GAUGE set back to 0.
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            volumeGroup->FindCounter(GetSensorName())->Val());
    }

    // Counters are distinct per disk
    Y_UNIT_TEST(ShouldKeepDistinctCountersPerDisk)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto serverGroup = root->GetSubgroup("component", "server");
        auto serviceVolumeGroup =
            root->GetSubgroup("component", "service_volume");

        InitCriticalEventsCounter(serverGroup);
        InitVolumeCriticalEventsCounter(serviceVolumeGroup);

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
            FindVolumeGroup(serviceVolumeGroup, v1)
                ->FindCounter(GetSensorName())
                ->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            FindVolumeGroup(serviceVolumeGroup, v2)
                ->FindCounter(GetSensorName())
                ->Val());

        // Deprecated counter should contain summary
        auto deprecatedCounter =
            serverGroup->FindCounter(GetDeprecatedSensorName());
        UNIT_ASSERT(deprecatedCounter);
        UNIT_ASSERT_VALUES_EQUAL(3, deprecatedCounter->Val());
    }

    // Events fired before CountersRoot is set must accumulate in
    // Internal and be flushed once the root becomes available
    Y_UNIT_TEST(ShouldAccumulateEventsBeforeCountersRootInitialized)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto serverGroup = root->GetSubgroup("component", "server");
        auto serviceVolumeGroup =
            root->GetSubgroup("component", "service_volume");

        // NOTE: InitVolumeCriticalEventsCounter is intentionally deferred.
        InitCriticalEventsCounter(serverGroup);

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
        InitVolumeCriticalEventsCounter(serviceVolumeGroup);
        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(serviceVolumeGroup, v);
        UNIT_ASSERT(volumeGroup);
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            volumeGroup->FindCounter(GetSensorName())->Val());

        // Deprecated counter reflects the dual emission (3 synchronous
        // Inc()'s).
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            serverGroup->FindCounter(GetDeprecatedSensorName())->Val());

        // One more event on the hot path (Exported is now non-null,
        // Internal -> 1) is flushed on the next tick.
        ReportBlockDigestMismatchInBlob(v, "some msg");
        handler->UpdateStats(true);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            volumeGroup->FindCounter(GetSensorName())->Val());
    }

    // All Report...() overloads works
    Y_UNIT_TEST(ShouldProperlyImplementAllReportOverloads)
    {
        ResetVolumeCriticalEventsCounter();

        auto root = MakeIntrusive<TDynamicCounters>();
        auto serverGroup = root->GetSubgroup("component", "server");
        auto serviceVolumeGroup =
            root->GetSubgroup("component", "service_volume");

        InitCriticalEventsCounter(serverGroup);
        InitVolumeCriticalEventsCounter(serviceVolumeGroup);

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
            serverGroup->FindCounter(GetDeprecatedSensorName());
        UNIT_ASSERT(deprecatedCounter);
        // Deprecated counter should contain summary immediatly
        UNIT_ASSERT_VALUES_EQUAL(12, deprecatedCounter->Val());

        handler->UpdateStats(true);

        auto volumeGroup = FindVolumeGroup(serviceVolumeGroup, v);
        UNIT_ASSERT(volumeGroup);
        UNIT_ASSERT_VALUES_EQUAL(
            12,
            volumeGroup->FindCounter(GetSensorName())->Val());

        UNIT_ASSERT_VALUES_EQUAL(12, deprecatedCounter->Val());
    }
}

}   // namespace NCloud::NBlockStore
