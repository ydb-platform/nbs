#include "directory_entry_version_cache.h"

#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectoryEntryVersionCacheTest)
{
    Y_UNIT_TEST(ShouldNotRememberChangesWithoutRegisteredHandle)
    {
        TDirectoryEntryVersionCache cache(1, CreateDirectoryHandleStatsStub());

        cache.UnregisterHandle(1);
        cache.AdvanceVersion(1, "child", 10);

        UNIT_ASSERT_VALUES_EQUAL(0, cache.GetVersion(1, "child"));

        cache.RegisterHandle(1);

        UNIT_ASSERT_VALUES_EQUAL(0, cache.GetVersion(1, "child"));
    }

    Y_UNIT_TEST(ShouldKeepLargestVersionForRegisteredEntry)
    {
        TDirectoryEntryVersionCache cache(1, CreateDirectoryHandleStatsStub());
        cache.RegisterHandle(1);

        cache.AdvanceVersion(1, "child", 10);
        UNIT_ASSERT_VALUES_EQUAL(10, cache.GetVersion(1, "child"));

        cache.AdvanceVersion(1, "child", 5);
        UNIT_ASSERT_VALUES_EQUAL(10, cache.GetVersion(1, "child"));

        cache.AdvanceVersion(1, "child", 15);
        UNIT_ASSERT_VALUES_EQUAL(15, cache.GetVersion(1, "child"));
    }

    Y_UNIT_TEST(ShouldKeepVersionsUntilLastHandleIsUnregistered)
    {
        TDirectoryEntryVersionCache cache(1, CreateDirectoryHandleStatsStub());
        cache.RegisterHandle(1);
        cache.RegisterHandle(1);
        cache.AdvanceVersion(1, "child", 10);

        cache.UnregisterHandle(1);
        UNIT_ASSERT_VALUES_EQUAL(10, cache.GetVersion(1, "child"));

        cache.UnregisterHandle(1);
        UNIT_ASSERT_VALUES_EQUAL(0, cache.GetVersion(1, "child"));

        cache.RegisterHandle(1);
        UNIT_ASSERT_VALUES_EQUAL(0, cache.GetVersion(1, "child"));
    }

    Y_UNIT_TEST(ShouldTrackDirectoriesIndependently)
    {
        TDirectoryEntryVersionCache cache(2, CreateDirectoryHandleStatsStub());
        cache.RegisterHandle(1);
        cache.RegisterHandle(2);

        cache.AdvanceVersion(1, "child", 10);
        cache.AdvanceVersion(1, "other", 20);
        cache.AdvanceVersion(2, "child", 30);

        UNIT_ASSERT_VALUES_EQUAL(10, cache.GetVersion(1, "child"));
        UNIT_ASSERT_VALUES_EQUAL(20, cache.GetVersion(1, "other"));
        UNIT_ASSERT_VALUES_EQUAL(30, cache.GetVersion(2, "child"));
        UNIT_ASSERT_VALUES_EQUAL(0, cache.GetVersion(2, "other"));
    }

    Y_UNIT_TEST(ShouldDecreaseEntryCountOnDestroy)
    {
        auto timer = CreateWallClockTimer();
        auto stats = CreateDirectoryHandleStats(timer, nullptr);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto metricsRegistry = NMetrics::CreateMetricsRegistry({}, counters);
        stats->RegisterCounters(
            metricsRegistry,
            NMetrics::CreateMetricsRegistryStub());

        auto maxEntryVersionCacheEntryCount =
            counters->FindCounter("MaxEntryVersionCacheEntryCount");
        UNIT_ASSERT(maxEntryVersionCacheEntryCount);

        {
            TDirectoryEntryVersionCache cache(1, stats);
            cache.RegisterHandle(1);
            cache.AdvanceVersion(1, "child", 10);
            UNIT_ASSERT_VALUES_EQUAL(0, maxEntryVersionCacheEntryCount->Val());

            stats->UpdateStats(timer->Now());
            metricsRegistry->Update(timer->Now());

            UNIT_ASSERT_VALUES_EQUAL(1, maxEntryVersionCacheEntryCount->Val());
        }

        for (size_t i = 0; i <= DirectoryHandleMaxBucketCount; ++i) {
            stats->UpdateStats(timer->Now());
            metricsRegistry->Update(timer->Now());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, maxEntryVersionCacheEntryCount->Val());
    }
}

}   // namespace NCloud::NFileStore::NFuse
