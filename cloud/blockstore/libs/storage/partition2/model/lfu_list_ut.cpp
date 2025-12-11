#include "lfu_list.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TValue: TLFUListItemBase<TValue>
{
    ui32 X;

    TValue(ui32 x)
        : X(x)
    {}

    ui32 Weight() const
    {
        return X;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLFUListTest)
{
    Y_UNIT_TEST(ShouldTrackUseCount)
    {
        TLFUList<TValue> l(TDefaultAllocator::Instance());
        TVector<TValue*> values = {
            new TValue(0),
            new TValue(1),
            new TValue(2),
            new TValue(3),
            new TValue(4),
            new TValue(5),
            new TValue(6),
        };

        l.Add(values[0]);
        l.Add(values[1]);
        l.Use(values[0]);
        l.Add(values[2]);
        l.Use(values[2]);
        l.Use(values[2]);
        l.Use(values[2]);
        l.Add(values[3]);
        l.Add(values[4]);
        l.Use(values[4]);
        l.Use(values[4]);
        l.Use(values[4]);
        l.Use(values[4]);
        l.Add(values[5]);
        l.Use(values[5]);
        l.Add(values[6]);
        l.Use(values[6]);

        UNIT_ASSERT_VALUES_EQUAL(7, l.GetCount());
        UNIT_ASSERT_VALUES_EQUAL(21, l.GetWeight());
        UNIT_ASSERT_VALUES_EQUAL(2, values[0]->Group->UseCount);
        UNIT_ASSERT_VALUES_EQUAL(1, values[1]->Group->UseCount);
        UNIT_ASSERT_VALUES_EQUAL(4, values[2]->Group->UseCount);
        UNIT_ASSERT_VALUES_EQUAL(1, values[3]->Group->UseCount);
        UNIT_ASSERT_VALUES_EQUAL(5, values[4]->Group->UseCount);
        UNIT_ASSERT_VALUES_EQUAL(2, values[5]->Group->UseCount);
        UNIT_ASSERT_VALUES_EQUAL(2, values[6]->Group->UseCount);

        l.Remove(values[5]);
        UNIT_ASSERT_VALUES_EQUAL(6, l.GetCount());
        UNIT_ASSERT_VALUES_EQUAL(16, l.GetWeight());

        UNIT_ASSERT_VALUES_EQUAL(3, l.Prune()->X);
        UNIT_ASSERT_VALUES_EQUAL(1, l.Prune()->X);
        UNIT_ASSERT_VALUES_EQUAL(6, l.Prune()->X);
        UNIT_ASSERT_VALUES_EQUAL(0, l.Prune()->X);
        UNIT_ASSERT_VALUES_EQUAL(2, l.Prune()->X);
        UNIT_ASSERT_VALUES_EQUAL(4, l.Prune()->X);
        UNIT_ASSERT_VALUES_EQUAL(0, l.GetCount());
        UNIT_ASSERT_VALUES_EQUAL(0, l.GetWeight());

        for (auto* v: values) {
            delete v;
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
