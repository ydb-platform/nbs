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
}

}   // namespace NCloud
