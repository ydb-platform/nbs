#include "scoped_handle.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TScopedHandleTest)
{
    Y_UNIT_TEST(ShouldBeInvalidValueByDefault)
    {
        using TFoo = TScopedHandle<int, 42, struct TFooTag>;

        TFoo foo;

        UNIT_ASSERT(!foo);
        UNIT_ASSERT_VALUES_EQUAL(42, static_cast<int>(foo));

        foo = TFoo{0};
        UNIT_ASSERT(foo);
        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(foo));
    }

    Y_UNIT_TEST(ShouldBeIncompatible)
    {
        using TFoo = TScopedHandle<int, 0, struct TFooTag>;
        using TBar = TScopedHandle<int, 0, struct TBarTag>;

        static_assert(!std::is_convertible_v<TFoo, TBar>);
        static_assert(!std::is_convertible_v<TBar, TFoo>);
    }

    Y_UNIT_TEST(ShouldBeComparable)
    {
        using TFoo = TScopedHandle<int, 0, struct TFooTag>;

        TFoo foo1{1};
        TFoo foo2{1};
        TFoo foo3{5};

        UNIT_ASSERT(foo1);
        UNIT_ASSERT(foo2);
        UNIT_ASSERT(foo3);
        UNIT_ASSERT_EQUAL(foo1, foo2);
        UNIT_ASSERT_LT(foo1, foo3);
        UNIT_ASSERT_GT(foo3, foo1);
        UNIT_ASSERT_UNEQUAL(foo1, foo3);
    }
}

}   // namespace NCloud
