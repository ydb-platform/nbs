#include "histogram.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <tuple>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THistogrmTest)
{
    Y_UNIT_TEST(ShouldCollectEmptyBuckets)
    {
        THistogram<> hist;

        int count = 0;
        hist.IterateBuckets([&count](auto, auto, auto) { ++count; });

        UNIT_ASSERT_VALUES_EQUAL(0, count);
    }

    Y_UNIT_TEST(ShouldCollectBucketsWithoutOverflow)
    {
        using TBucket = std::tuple<ui64, ui64, ui64>;

        const ui64 dt = 1ull << 63;

        THistogram<> hist;

        hist.Increment(dt);

        TVector<TBucket> buckets;

        hist.IterateBuckets([&](ui64 start, ui64 end, ui64 count)
                            { buckets.emplace_back(start, end, count); });

        UNIT_ASSERT_VALUES_EQUAL(1, buckets.size());

        auto [s, e, count] = buckets.front();

        UNIT_ASSERT_VALUES_EQUAL(1, count);
        UNIT_ASSERT_GE(dt, s);
        UNIT_ASSERT_LT(dt, e);
    }

    Y_UNIT_TEST(ShouldCollectBuckets)
    {
        using TBucket = std::tuple<ui64, ui64, ui64>;

        THistogram<> hist;

        for (int i = 0; i != 1000; ++i) {
            hist.Increment(1000);
            hist.Increment(1);
        }

        hist.Increment(1'000'000'000);

        TVector<TBucket> buckets;

        hist.IterateBuckets([&](ui64 start, ui64 end, ui64 count)
                            { buckets.emplace_back(start, end, count); });

        UNIT_ASSERT_VALUES_EQUAL(3, buckets.size());

        {
            auto [s, e, n] = buckets[0];

            UNIT_ASSERT_GE(1.0, s);
            UNIT_ASSERT_LT(1.0, e);
            UNIT_ASSERT_VALUES_EQUAL(1000, n);
        }

        {
            auto [s, e, n] = buckets[1];

            UNIT_ASSERT_GE(1000.0, s);
            UNIT_ASSERT_LT(1000.0, e);
            UNIT_ASSERT_VALUES_EQUAL(1000, n);
        }

        {
            auto [s, e, n] = buckets[2];

            UNIT_ASSERT_GE(1'000'000'000.0, s);
            UNIT_ASSERT_LT(1'000'000'000.0, e);
            UNIT_ASSERT_VALUES_EQUAL(1, n);
        }
    }

    Y_UNIT_TEST(ShouldIterateDiffBuckets)
    {
        using TBucket = std::pair<ui64, ui64>;

        THistogram<> hist;

        hist.Increment(1);
        hist.Increment(1_KB, 2);
        hist.Increment(1_MB, 3);
        hist.Increment(1_GB, 4);

        {
            TVector<TBucket> buckets;
            hist.IterateDiffBuckets(
                {},
                [&](ui64 start, ui64 end, ui64 count)
                {
                    Y_UNUSED(end);
                    buckets.emplace_back(start, count);
                });

            UNIT_ASSERT_VALUES_EQUAL(4, buckets.size());
            UNIT_ASSERT_EQUAL(TBucket(1, 1), buckets[0]);
            UNIT_ASSERT_EQUAL(TBucket(1_KB, 2), buckets[1]);
            UNIT_ASSERT_EQUAL(TBucket(1_MB, 3), buckets[2]);
            UNIT_ASSERT_EQUAL(TBucket(1_GB, 4), buckets[3]);
        }

        THistogram<> newHist = hist;

        {
            TVector<TBucket> buckets;
            newHist.IterateDiffBuckets(
                hist,
                [&](ui64 start, ui64 end, ui64 count)
                {
                    Y_UNUSED(end);
                    buckets.emplace_back(start, count);
                });

            UNIT_ASSERT_VALUES_EQUAL(0, buckets.size());
        }

        newHist.Increment(8_KB, 10);
        newHist.Increment(1_MB, 7);

        {
            TVector<TBucket> buckets;
            newHist.IterateDiffBuckets(
                hist,
                [&](ui64 start, ui64 end, ui64 count)
                {
                    Y_UNUSED(end);
                    buckets.emplace_back(start, count);
                });

            UNIT_ASSERT_VALUES_EQUAL(2, buckets.size());
            UNIT_ASSERT_EQUAL(TBucket(8_KB, 10), buckets[0]);
            UNIT_ASSERT_EQUAL(TBucket(1_MB, 7), buckets[1]);
        }
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
