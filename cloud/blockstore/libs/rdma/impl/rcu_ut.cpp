#include "rcu.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(
    TRcuListTest){Y_UNIT_TEST(ShouldAddItems){TRCUList<int> rcuList;
for (int i = 0; i < 10; ++i) {
    rcuList.Add(i);
}

auto list = rcuList.Get();
UNIT_ASSERT_EQUAL(list->size(), 10);

for (int i = 0; i < 10; ++i) {
    UNIT_ASSERT_EQUAL(list->at(i), i);
}
}   // namespace NCloud::NBlockStore::NRdma

Y_UNIT_TEST(ShouldDeleteItems)
{
    TRCUList<int> rcuList;
    for (int i = 0; i < 10; ++i) {
        rcuList.Add(i);
    }

    rcuList.Delete([](auto other) { return other % 2 == 0; });

    auto list = rcuList.Get();
    UNIT_ASSERT_EQUAL(list->size(), 5);

    for (int i = 0; i < 5; ++i) {
        UNIT_ASSERT_EQUAL(list->at(i), i * 2 + 1);
    }

    rcuList.Delete([](auto) { return true; });

    UNIT_ASSERT_EQUAL(rcuList.Get()->size(), 0);
}

Y_UNIT_TEST(ShouldNotModifyAccessedList)
{
    TRCUList<int> rcuList;
    for (int i = 0; i < 10; ++i) {
        rcuList.Add(i);
    }

    auto list = rcuList.Get();
    rcuList.Delete([](auto) { return true; });

    UNIT_ASSERT_EQUAL(list->size(), 10);

    for (int i = 0; i < 10; ++i) {
        UNIT_ASSERT_EQUAL(list->at(i), i);
    }
}
}
;

}   // namespace NCloud::NBlockStore::NRdma
