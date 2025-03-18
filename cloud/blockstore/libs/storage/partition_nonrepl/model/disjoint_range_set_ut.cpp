#include "disjoint_range_set.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDisjointRangeSetTest)
{
    Y_UNIT_TEST(BasicTest)
    {
        TDisjointRangeSet set;

        auto range1 = TBlockRange64::WithLength(0, 4_KB);
        auto range2 = TBlockRange64::WithLength(4_KB, 4_KB);
        auto range3 = TBlockRange64::WithLength(4_KB * 2, 4_KB);
        auto range4 = TBlockRange64::WithLength(4_KB * 3, 2_KB);

        UNIT_ASSERT(set.Empty());
        UNIT_ASSERT(set.TryInsert(range2));
        UNIT_ASSERT_VALUES_EQUAL(range2, set.LeftmostRange());
        UNIT_ASSERT(!set.Empty());

        UNIT_ASSERT(set.TryInsert(range1));
        UNIT_ASSERT_VALUES_EQUAL(range1, set.LeftmostRange());

        UNIT_ASSERT(set.TryInsert(range3));
        UNIT_ASSERT_VALUES_EQUAL(range1, set.LeftmostRange());

        UNIT_ASSERT(set.TryInsert(range4));
        UNIT_ASSERT_VALUES_EQUAL(range1, set.LeftmostRange());

        // Already inserted.
        UNIT_ASSERT(!set.TryInsert(range1));
        // Intersects.
        UNIT_ASSERT(!set.TryInsert(TBlockRange64::WithLength(2_KB, 4_KB)));

        UNIT_ASSERT(set.Remove(range1));
        UNIT_ASSERT_VALUES_EQUAL(range2, set.LeftmostRange());

        UNIT_ASSERT(set.Remove(range2));
        UNIT_ASSERT_VALUES_EQUAL(range3, set.LeftmostRange());

        UNIT_ASSERT(set.Remove(range3));
        UNIT_ASSERT_VALUES_EQUAL(range4, set.LeftmostRange());

        UNIT_ASSERT(set.Remove(range4));
        UNIT_ASSERT(set.Empty());
    }

    Y_UNIT_TEST(IteratorTest)
    {
        TDisjointRangeSet set;

        for (int i = 0; i < 10; i++) {
            UNIT_ASSERT(
                set.TryInsert(TBlockRange64::WithLength(4_KB * i, 4_KB)));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TBlockRange64::WithLength(0, 4_KB),
            set.LeftmostRange());

        TDisjointRangeSetIterator it(set);
        int count = 0;
        while (it.HasNext()) {
            auto range = it.Next();
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(4_KB * count, 4_KB),
                range);
            count++;
        }
        UNIT_ASSERT_VALUES_EQUAL(10, count);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
