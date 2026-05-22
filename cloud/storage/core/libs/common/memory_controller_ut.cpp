#include "memory_controller.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMemoryControllerTest)
{
    Y_UNIT_TEST(ShouldReturnNullptrWhenMemoryLimitDisabled)
    {
        UNIT_ASSERT(!CreateMemoryController(
            {.MemoryLimit = 0, .TmpfsMemoryLimitPercent = 25}));
    }

    Y_UNIT_TEST(ShouldUseZeroLimitWhenTmpfsPercentDisabled)
    {
        auto controller = CreateMemoryController(
            {.MemoryLimit = 1024, .TmpfsMemoryLimitPercent = 0});
        UNIT_ASSERT(controller);

        UNIT_ASSERT(!controller->CanIncreaseFileMapUsage(1));
    }

    Y_UNIT_TEST(ShouldLimitMemoryConsumption)
    {
        auto controller = CreateMemoryController(
            {.MemoryLimit = 100, .TmpfsMemoryLimitPercent = 25});
        UNIT_ASSERT(controller);

        UNIT_ASSERT(controller->CanIncreaseFileMapUsage(25));
        UNIT_ASSERT(!controller->CanIncreaseFileMapUsage(26));

        controller->IncreaseFileMapUsage(20);
        UNIT_ASSERT(controller->CanIncreaseFileMapUsage(5));
        UNIT_ASSERT(!controller->CanIncreaseFileMapUsage(6));

        controller->DecreaseFileMapUsage(10);
        UNIT_ASSERT(controller->CanIncreaseFileMapUsage(15));
        UNIT_ASSERT(!controller->CanIncreaseFileMapUsage(16));
    }

    Y_UNIT_TEST(ShouldUseRoundedDownLimitForSmallPercent)
    {
        auto controller = CreateMemoryController(
            {.MemoryLimit = 1, .TmpfsMemoryLimitPercent = 1});
        UNIT_ASSERT(controller);

        UNIT_ASSERT(!controller->CanIncreaseFileMapUsage(1));
    }
}

}   // namespace NCloud
