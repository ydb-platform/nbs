#include "range_map.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRangeMapTest)
{
    Y_UNIT_TEST(ShouldReturnBlock)
    {
        {
            TRangeMap store(TBlockRange64(0, 99));
            auto r = store.GetBlock(TBlockRange64(0, 10));
            UNIT_ASSERT(r.Defined() && store.Size() == 1);
        }

        {
            TRangeMap store(TBlockRange64(0, 99));
            auto r = store.GetBlock(TBlockRange64(0, 100));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64(0, 100));
            auto r = store.GetBlock(TBlockRange64(0, 200));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64(0, 99));
            auto r1 = store.GetBlock(TBlockRange64(0, 50));
            UNIT_ASSERT(r1.Defined() && store.Size() == 1);
            auto r2 = store.GetBlock(TBlockRange64(0, 50));
            UNIT_ASSERT(r2.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64(0, 99));
            auto r = store.GetBlock(TBlockRange64(0, 99));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64(0, 100));
            auto r = store.GetBlock(TBlockRange64(0, 200));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64(0, 100));
            auto r = store.GetBlock(TBlockRange64(200, 200));
            UNIT_ASSERT(r.Defined() && !store.Empty());
        }

        {
            TRangeMap store(TBlockRange64(0, 99));
            auto r1 = store.GetBlock(TBlockRange64(0, 49));
            UNIT_ASSERT(r1.Defined() && store.Size() == 1);
            auto r2 = store.GetBlock(TBlockRange64(50, 99));
            UNIT_ASSERT(r2.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64(0, 9999));
            auto r1 = store.GetBlock(TBlockRange64(9253, 9315));
            UNIT_ASSERT(r1.Defined() && store.Size() == 2);
        }

    }

    Y_UNIT_TEST(ShouldPutBlock)
    {
        {
            TRangeMap store(TBlockRange64(0, 99));
            store.PutBlock(TBlockRange64(101,120));
            UNIT_ASSERT(store.Size() == 2);
        }

        {
            TRangeMap store(TBlockRange64(0, 100));
            store.PutBlock(TBlockRange64(101, 200));
            UNIT_ASSERT(store.Size() == 1);
        }

        {
            TRangeMap store(TBlockRange64(50, 100));
            store.PutBlock(TBlockRange64(0, 49));
            UNIT_ASSERT(store.Size() == 1);
        }

        {
            TRangeMap store(TBlockRange64(0, 50));
            store.PutBlock(TBlockRange64(101, 200));
            UNIT_ASSERT(store.Size() == 2);
            store.PutBlock(TBlockRange64(51, 100));
            UNIT_ASSERT(store.Size() == 1);
        }
    }
}

}   // namespace NCloud::NBlockStore::NLoadTest
