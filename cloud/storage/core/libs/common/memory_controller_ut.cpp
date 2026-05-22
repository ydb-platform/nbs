#include "memory_controller.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMemoryControllerTest)
{
    Y_UNIT_TEST(ShouldReturnNullptrWhenTmpfsMemoryLimitDisabled)
    {
        UNIT_ASSERT(!CreateMemoryController({.TmpfsMemoryLimit = 0}));
    }

    Y_UNIT_TEST(ShouldLimitMemoryConsumption)
    {
        auto controller = CreateMemoryController({.TmpfsMemoryLimit = 25});
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
}

}   // namespace NCloud
