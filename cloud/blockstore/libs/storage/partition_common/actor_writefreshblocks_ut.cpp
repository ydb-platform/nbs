#include "actor_writefreshblocks.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteFreshBlocksActorTest)
{
    Y_UNIT_TEST(ShouldDisableFreshHardLimitsWhenZero)
    {
        UNIT_ASSERT(!HasError(CheckFreshHardLimits(0, 0, 0, 0)));
        UNIT_ASSERT(!HasError(CheckFreshHardLimits(100, 100, 0, 0)));

        // Disabling either limit must leave the other limit active.
        UNIT_ASSERT(!HasError(CheckFreshHardLimits(100, 100, 0, 101)));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            CheckFreshHardLimits(100, 100, 0, 100).GetCode());

        UNIT_ASSERT(!HasError(CheckFreshHardLimits(100, 100, 101, 0)));
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            CheckFreshHardLimits(100, 100, 100, 0).GetCode());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
