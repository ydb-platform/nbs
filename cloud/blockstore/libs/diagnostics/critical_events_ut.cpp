#include "critical_events.h"

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCriticalEventsTest)
{
    Y_UNIT_TEST(ShouldEmitPerDiskCountersForVolumeCriticalEvents)
    {
        auto root = MakeIntrusive<TDynamicCounters>();

        auto serverGroup = root->GetSubgroup("component", "server");
        auto serviceVolumeGroup =
            root->GetSubgroup("component", "service_volume");

        InitCriticalEventsCounter(serverGroup);
        InitVolumeCriticalEventsCounter(root);

        const auto sensorName =
            GetCriticalEventForMirroredDiskChecksumMismatchUponRead();
        const auto deprecatedSensorName =
            GetDeprecatedCriticalEventForMirroredDiskChecksumMismatchUponRead();

        // Fire the report twice with a specific disk.
        ReportMirroredDiskChecksumMismatchUponRead(
            "disk-1",
            "cloud-1",
            "folder-1",
            "msg");
        ReportMirroredDiskChecksumMismatchUponRead(
            "disk-1",
            "cloud-1",
            "folder-1",
            "msg");

        // Per-disk counter reads 2.
        auto perDiskGroup = FindNestedGroup(
            root,
            {{"component", "service_volume"},
             {"host", "cluster"},
             {"volume", "disk-1"},
             {"cloud", "cloud-1"},
             {"folder", "folder-1"}});
        UNIT_ASSERT(perDiskGroup);
        auto perDiskCounter = perDiskGroup->FindCounter(sensorName);
        UNIT_ASSERT(perDiskCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, perDiskCounter->Val());

        // Deprecated counter under component=server reads 2.
        auto deprecatedCounter =
            serverGroup->FindCounter(deprecatedSensorName);
        UNIT_ASSERT(deprecatedCounter);
        UNIT_ASSERT_VALUES_EQUAL(2, deprecatedCounter->Val());
    }
}

}   // namespace NCloud::NBlockStore
