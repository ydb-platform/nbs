#include "histogram.h"

#include "spdk.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkHistogramTest)
{
    Y_UNIT_TEST(ShouldCollectEmptyBuckets)
    {
        auto histogram = CreateHistogram();

        auto buckets = CollectBuckets(*histogram, 1);

        UNIT_ASSERT_EQUAL(buckets.size(), 0);
    }

    Y_UNIT_TEST(ShouldCollectBucketsWithoutOverflow)
    {
        auto histogram = CreateHistogram();

        const ui64 rate = 1ull << 62;
        const ui64 delta = 1ull << 63;
        const ui32 mcsValue = delta / rate * 1'000'000;

        spdk_histogram_data_tally(histogram.get(), delta);

        auto buckets = CollectBuckets(*histogram, rate);

        UNIT_ASSERT_EQUAL(buckets.size(), 1);

        auto [mcs, count] = buckets.front();

        UNIT_ASSERT_EQUAL(count, 1);
        UNIT_ASSERT_LT(mcsValue, mcs);
    }
}

}   // namespace NCloud::NBlockStore::NSpdk
