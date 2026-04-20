#include "rcu.h"

#include <library/cpp/testing/gtest/gtest.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

TEST(TRcuListTest, ShouldAddItems)
{
    TRCUList<int> rcuList;
    for (int i = 0; i < 10; ++i) {
        rcuList.Add(i);
    }

    auto list = rcuList.Get();
    ASSERT_EQ(list->size(), 10u);

    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(list->at(i), i);
    }
}

TEST(TRcuListTest, ShouldDeleteItems)
{
    TRCUList<int> rcuList;
    for (int i = 0; i < 10; ++i) {
        rcuList.Add(i);
    }

    rcuList.Delete([](auto other) {
        return other % 2 == 0;
    });

    auto list = rcuList.Get();
    ASSERT_EQ(list->size(), 5u);

    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(list->at(i), i * 2 + 1);
    }

    rcuList.Delete([](auto) {
        return true;
    });

    ASSERT_EQ(rcuList.Get()->size(), 0u);
}

TEST(TRcuListTest, ShouldNotModifyAccessedList)
{
    TRCUList<int> rcuList;
    for (int i = 0; i < 10; ++i) {
        rcuList.Add(i);
    }

    auto list = rcuList.Get();
    rcuList.Delete([](auto) {
        return true;
    });

    ASSERT_EQ(list->size(), 10u);

    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(list->at(i), i);
    }
}

}   // namespace NCloud::NBlockStore::NRdma
