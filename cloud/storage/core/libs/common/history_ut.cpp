#include "history.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THistoryTest)
{
    Y_UNIT_TEST(ShouldReplaceItems)
    {
        {
            constexpr int count = 10;
            auto history = THistory<int>(count);

            for (int i = 0; i < count; i++) {
                history.Put(i);
                UNIT_ASSERT_VALUES_EQUAL(history.Contains(i), true);
            }
            for (int i = count; i < count + count; i++) {
                history.Put(i);
                UNIT_ASSERT_VALUES_EQUAL(history.Contains(i), true);
                UNIT_ASSERT_VALUES_EQUAL(history.Contains(i - count), false);
            }
        }
    }

    Y_UNIT_TEST(ShouldHandleDuplicates)
    {
        {
            auto history = THistory<int>(2);

            history.Put(0);
            history.Put(0);

            history.Put(0);   // replaces 0 with 0
            UNIT_ASSERT_VALUES_EQUAL(history.Contains(0), true);

            history.Put(1);   // replaces 0 with 1
            UNIT_ASSERT_VALUES_EQUAL(history.Contains(0), true);
            UNIT_ASSERT_VALUES_EQUAL(history.Contains(1), true);

            history.Put(1);   // replaces last 0 with 1
            UNIT_ASSERT_VALUES_EQUAL(history.Contains(0), false);
            UNIT_ASSERT_VALUES_EQUAL(history.Contains(1), true);
        }
    }
}

}   // namespace NCloud
