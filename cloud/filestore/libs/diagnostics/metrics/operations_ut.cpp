#include "operations.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOperationsTest)
{
    Y_UNIT_TEST(ShouldLoad)
    {
        {
            std::atomic<i64> value{42};
            UNIT_ASSERT_VALUES_EQUAL(42, Load(value));
        }

        {
            TAtomic value{322};
            UNIT_ASSERT_VALUES_EQUAL(322, Load(value));
        }
    }

    Y_UNIT_TEST(ShouldStore)
    {
        {
            std::atomic<i64> value{0};
            UNIT_ASSERT_VALUES_EQUAL(0, value.load());

            Store(value, 42);
            UNIT_ASSERT_VALUES_EQUAL(42, value.load());
        }

        {
            TAtomic value{0};
            UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(value));

            Store(value, 322);
            UNIT_ASSERT_VALUES_EQUAL(322, AtomicGet(value));
        }
    }

    Y_UNIT_TEST(ShouldInc)
    {
        {
            std::atomic<i64> value{0};
            UNIT_ASSERT_VALUES_EQUAL(0, value.load());

            UNIT_ASSERT_VALUES_EQUAL(0, Inc(value));
            UNIT_ASSERT_VALUES_EQUAL(1, value.load());

            UNIT_ASSERT_VALUES_EQUAL(1, Inc(value));
            UNIT_ASSERT_VALUES_EQUAL(2, value.load());
        }

        {
            TAtomic value{0};
            UNIT_ASSERT_VALUES_EQUAL(0, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(0, Inc(value));
            UNIT_ASSERT_VALUES_EQUAL(1, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(1, Inc(value));
            UNIT_ASSERT_VALUES_EQUAL(2, AtomicGet(value));
        }
    }

    Y_UNIT_TEST(ShouldDec)
    {
        {
            std::atomic<i64> value{42};
            UNIT_ASSERT_VALUES_EQUAL(42, value.load());

            UNIT_ASSERT_VALUES_EQUAL(42, Dec(value));
            UNIT_ASSERT_VALUES_EQUAL(41, value.load());

            UNIT_ASSERT_VALUES_EQUAL(41, Dec(value));
            UNIT_ASSERT_VALUES_EQUAL(40, value.load());
        }

        {
            TAtomic value{322};
            UNIT_ASSERT_VALUES_EQUAL(322, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(322, Dec(value));
            UNIT_ASSERT_VALUES_EQUAL(321, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(321, Dec(value));
            UNIT_ASSERT_VALUES_EQUAL(320, AtomicGet(value));
        }
    }

    Y_UNIT_TEST(ShouldAdd)
    {
        {
            std::atomic<i64> value{42};
            UNIT_ASSERT_VALUES_EQUAL(42, value.load());

            UNIT_ASSERT_VALUES_EQUAL(42, Add(value, 5));
            UNIT_ASSERT_VALUES_EQUAL(47, value.load());

            UNIT_ASSERT_VALUES_EQUAL(47, Add(value, 3));
            UNIT_ASSERT_VALUES_EQUAL(50, value.load());
        }

        {
            TAtomic value{322};
            UNIT_ASSERT_VALUES_EQUAL(322, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(322, Add(value, 5));
            UNIT_ASSERT_VALUES_EQUAL(327, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(327, Add(value, 3));
            UNIT_ASSERT_VALUES_EQUAL(330, AtomicGet(value));
        }
    }

    Y_UNIT_TEST(ShouldSub)
    {
        {
            std::atomic<i64> value{42};
            UNIT_ASSERT_VALUES_EQUAL(42, value.load());

            UNIT_ASSERT_VALUES_EQUAL(42, Sub(value, 2));
            UNIT_ASSERT_VALUES_EQUAL(40, value.load());

            UNIT_ASSERT_VALUES_EQUAL(40, Sub(value, 5));
            UNIT_ASSERT_VALUES_EQUAL(35, value.load());
        }

        {
            TAtomic value{322};
            UNIT_ASSERT_VALUES_EQUAL(322, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(322, Sub(value, 2));
            UNIT_ASSERT_VALUES_EQUAL(320, AtomicGet(value));

            UNIT_ASSERT_VALUES_EQUAL(320, Sub(value, 5));
            UNIT_ASSERT_VALUES_EQUAL(315, AtomicGet(value));
        }
    }
}

}   // namespace NCloud::NFileStore::NMetrics
