#include "blocks_bloom_filter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlocksBloomFilterTest)
{
    Y_UNIT_TEST(TestEmptyFilter)
    {
        TBlocksBloomFilter filter(1000, 0.01);

        UNIT_ASSERT(!filter.Test(0));
        UNIT_ASSERT(!filter.Test(42));
        UNIT_ASSERT(!filter.Test(999));
    }

    Y_UNIT_TEST(TestAddedBlocksAreFound)
    {
        TBlocksBloomFilter filter(100, 0.01);

        const TVector<ui32> blocks = {0, 7, 42, 99};

        for (ui32 blockIndex : blocks) {
            filter.Add(blockIndex);
        }

        for (ui32 blockIndex : blocks) {
            UNIT_ASSERT(filter.Test(blockIndex));
        }
    }

    Y_UNIT_TEST(TestIdempotentAdd)
    {
        TBlocksBloomFilter filter(10, 0.01);

        filter.Add(5);
        filter.Add(5);
        filter.Add(5);

        UNIT_ASSERT(filter.Test(5));
    }

    Y_UNIT_TEST(TestNoFalseNegatives)
    {
        constexpr ui32 elementsCount = 1000;
        constexpr double errorRate = 0.01;

        TBlocksBloomFilter filter(elementsCount, errorRate);

        for (ui32 blockIndex = 0; blockIndex < elementsCount; ++blockIndex) {
            filter.Add(blockIndex);
        }

        for (ui32 blockIndex = 0; blockIndex < elementsCount; ++blockIndex) {
            UNIT_ASSERT_C(
                filter.Test(blockIndex),
                "Block " << blockIndex << " must be present");
        }
    }

    Y_UNIT_TEST(TestFalsePositiveRate)
    {
        constexpr ui32 elementsCount = 10000;
        constexpr double errorRate = 0.01;
        constexpr ui32 probeCount = 100000;
        constexpr ui32 probeOffset = elementsCount + 1;

        TBlocksBloomFilter filter(elementsCount, errorRate);

        for (ui32 blockIndex = 0; blockIndex < elementsCount; ++blockIndex) {
            filter.Add(blockIndex);
        }

        ui32 falsePositives = 0;

        for (ui32 i = 0; i < probeCount; ++i) {
            if (filter.Test(probeOffset + i)) {
                ++falsePositives;
            }
        }

        const double actualRate = double(falsePositives) / probeCount;

        UNIT_ASSERT_C(
            actualRate < errorRate * 1.3,
            "False positive rate " << actualRate << " exceeds "
                << errorRate * 1.3);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
