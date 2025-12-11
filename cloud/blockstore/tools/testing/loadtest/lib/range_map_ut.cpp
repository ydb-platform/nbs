#include "range_map.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRangeMapTest)
{
    Y_UNIT_TEST(ShouldReturnBlock)
    {
        {
            TRangeMap store(TBlockRange64::WithLength(0, 100));
            auto r = store.GetBlock(TBlockRange64::WithLength(0, 11));
            UNIT_ASSERT(r.Defined() && store.Size() == 1);
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 100));
            auto r = store.GetBlock(TBlockRange64::WithLength(0, 101));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 101));
            auto r = store.GetBlock(TBlockRange64::WithLength(0, 201));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 100));
            auto r1 = store.GetBlock(TBlockRange64::WithLength(0, 51));
            UNIT_ASSERT(r1.Defined() && store.Size() == 1);
            auto r2 = store.GetBlock(TBlockRange64::WithLength(0, 51));
            UNIT_ASSERT(r2.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 100));
            auto r = store.GetBlock(TBlockRange64::WithLength(0, 100));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 101));
            auto r = store.GetBlock(TBlockRange64::WithLength(0, 201));
            UNIT_ASSERT(r.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 101));
            auto r = store.GetBlock(TBlockRange64::MakeOneBlock(200));
            UNIT_ASSERT(r.Defined() && !store.Empty());
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 100));
            auto r1 = store.GetBlock(TBlockRange64::WithLength(0, 50));
            UNIT_ASSERT(r1.Defined() && store.Size() == 1);
            auto r2 = store.GetBlock(TBlockRange64::WithLength(50, 50));
            UNIT_ASSERT(r2.Defined() && store.Empty());
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 10000));
            auto r1 =
                store.GetBlock(TBlockRange64::MakeClosedInterval(9253, 9315));
            UNIT_ASSERT(r1.Defined() && store.Size() == 2);
        }
    }

    Y_UNIT_TEST(ShouldPutBlock)
    {
        {
            TRangeMap store(TBlockRange64::WithLength(0, 100));
            store.PutBlock(TBlockRange64::WithLength(101, 20));
            UNIT_ASSERT(store.Size() == 2);
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 101));
            store.PutBlock(TBlockRange64::WithLength(101, 100));
            UNIT_ASSERT(store.Size() == 1);
        }

        {
            TRangeMap store(TBlockRange64::WithLength(50, 51));
            store.PutBlock(TBlockRange64::WithLength(0, 50));
            UNIT_ASSERT(store.Size() == 1);
        }

        {
            TRangeMap store(TBlockRange64::WithLength(0, 51));
            store.PutBlock(TBlockRange64::WithLength(101, 100));
            UNIT_ASSERT(store.Size() == 2);
            store.PutBlock(TBlockRange64::WithLength(51, 50));
            UNIT_ASSERT(store.Size() == 1);
        }
    }
}

}   // namespace NCloud::NBlockStore::NLoadTest
