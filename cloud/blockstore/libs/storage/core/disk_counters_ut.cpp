#include "disk_counters.h"

#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskCountersTests)
{
    Y_UNIT_TEST(ShouldResetAfterPublish)
    {
        TVolumeSelfCounters counters(
            EPublishingPolicy::All,
            EHistogramCounterOption::ReportMultipleCounters);
        NMonitoring::TDynamicCountersPtr dynCounters =
            new NMonitoring::TDynamicCounters();
        counters.Register(dynCounters, false);

        counters.Simple.MaxUsedQuota.Increment(100);
        counters.Cumulative.UsedQuota.Increment(200);
        counters.RequestCounters.ReadBlocks.Increment(1);

        UNIT_ASSERT_VALUES_EQUAL(100, counters.Simple.MaxUsedQuota.Value);
        UNIT_ASSERT_VALUES_EQUAL(200, counters.Cumulative.UsedQuota.Value);
        auto buckets = counters.RequestCounters.ReadBlocks.GetBuckets();
        UNIT_ASSERT_VALUES_EQUAL(1, buckets[0].second);

        counters.Publish(TInstant::Now());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Simple.MaxUsedQuota.Value);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Cumulative.UsedQuota.Value);
        buckets = counters.RequestCounters.ReadBlocks.GetBuckets();
        UNIT_ASSERT_VALUES_EQUAL(0, buckets[0].second);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
