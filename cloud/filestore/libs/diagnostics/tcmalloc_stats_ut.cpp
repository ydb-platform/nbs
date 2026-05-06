#include "tcmalloc_stats.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr const char* CacheCounterNames[] = {
    "tcmalloc.page_heap_free",
    "tcmalloc.central_cache_free",
    "tcmalloc.transfer_cache_free",
    "tcmalloc.sharded_transfer_cache_free",
    "tcmalloc.cpu_free",
    "tcmalloc.thread_cache_free",
};

TDynamicCountersPtr GetTcMallocCounters(TDynamicCountersPtr rootCounters)
{
    auto counters = rootCounters->FindSubgroup("counters", "utils");
    UNIT_ASSERT(counters);

    counters = counters->FindSubgroup("component", "tcmalloc");
    UNIT_ASSERT(counters);

    return counters;
}

TDynamicCounters::TCounterPtr FindCounter(
    TDynamicCountersPtr counters,
    const TString& name)
{
    auto counter = counters->FindCounter(name);
    UNIT_ASSERT_C(counter, name);
    return counter;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTcMallocStatsTest)
{
    Y_UNIT_TEST(ShouldUpdateAllocatorCounters)
    {
        auto rootCounters = MakeIntrusive<TDynamicCounters>();
        auto handler = CreateTcMallocStatsHandler(rootCounters);

        handler->UpdateStats(false);

        auto counters = GetTcMallocCounters(rootCounters);
        auto physicalMemoryUsed =
            FindCounter(counters, "generic.physical_memory_used");

        UNIT_ASSERT_C(
            physicalMemoryUsed->GetAtomic() > 0,
            physicalMemoryUsed->GetAtomic());
    }

    Y_UNIT_TEST(ShouldComputeCachesBytesFromCacheCounters)
    {
        auto rootCounters = MakeIntrusive<TDynamicCounters>();
        auto handler = CreateTcMallocStatsHandler(rootCounters);

        handler->UpdateStats(false);

        auto counters = GetTcMallocCounters(rootCounters);
        ui64 expectedCachesSize = 0;
        for (auto name: CacheCounterNames) {
            expectedCachesSize += FindCounter(counters, name)->GetAtomic();
        }

        UNIT_ASSERT_VALUES_EQUAL(
            expectedCachesSize,
            FindCounter(counters, "tcmalloc.caches_bytes")->GetAtomic());
    }
}

}   // namespace NCloud::NFileStore
