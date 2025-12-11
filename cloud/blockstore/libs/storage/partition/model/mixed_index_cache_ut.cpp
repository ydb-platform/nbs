#include "mixed_index_cache.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMixedIndexCache)
{
    Y_UNIT_TEST(ShouldPruneRanges)
    {
        constexpr ui32 MaxSize = 3;
        TMixedIndexCache cache{MaxSize};

        UNIT_ASSERT_VALUES_EQUAL(
            TMixedIndexCache::ERangeTemperature::Cold,
            cache.GetRangeTemperature(1));

        cache.RaiseRangeTemperature(1);
        cache.RaiseRangeTemperature(2);
        cache.RaiseRangeTemperature(3);

        for (ui32 rangeIndex = 1; rangeIndex <= 3; ++rangeIndex) {
            UNIT_ASSERT_VALUES_EQUAL(
                TMixedIndexCache::ERangeTemperature::Warm,
                cache.GetRangeTemperature(rangeIndex));
        }

        // warm -> hot
        cache.RaiseRangeTemperature(1);

        // kick 2
        cache.RaiseRangeTemperature(4);

        UNIT_ASSERT_VALUES_EQUAL(
            TMixedIndexCache::ERangeTemperature::Hot,
            cache.GetRangeTemperature(1));

        UNIT_ASSERT_VALUES_EQUAL(
            TMixedIndexCache::ERangeTemperature::Cold,
            cache.GetRangeTemperature(2));

        UNIT_ASSERT_VALUES_EQUAL(
            TMixedIndexCache::ERangeTemperature::Warm,
            cache.GetRangeTemperature(3));

        UNIT_ASSERT_VALUES_EQUAL(
            TMixedIndexCache::ERangeTemperature::Warm,
            cache.GetRangeTemperature(4));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition

template <>
inline void Out<NCloud::NBlockStore::NStorage::NPartition::TMixedIndexCache::
                    ERangeTemperature>(
    IOutputStream& out,
    NCloud::NBlockStore::NStorage::NPartition::TMixedIndexCache::
        ERangeTemperature t)
{
    out << ToString(t);
}
