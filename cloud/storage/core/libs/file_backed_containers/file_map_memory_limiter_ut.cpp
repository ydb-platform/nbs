#include "file_map_memory_limiter.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileMapMemoryLimiterTest)
{
    Y_UNIT_TEST(ShouldCreateStubWhenFileMapMemoryLimitDisabled)
    {
        auto limiter = CreateFileMapMemoryLimiter({.FileMapMemoryLimit = 0});

        UNIT_ASSERT(limiter);
        UNIT_ASSERT(limiter->CanIncrease(42));

        limiter->Increase(42);
        limiter->Decrease(42);
    }

    Y_UNIT_TEST(ShouldCreateStubExplicitly)
    {
        auto limiter = CreateFileMapMemoryLimiterStub();

        UNIT_ASSERT(limiter);
        UNIT_ASSERT(limiter->CanIncrease(42));

        limiter->Increase(42);
        limiter->Decrease(42);
    }

    Y_UNIT_TEST(ShouldLimitMemoryConsumption)
    {
        auto limiter = CreateFileMapMemoryLimiter({.FileMapMemoryLimit = 25});
        UNIT_ASSERT(limiter);

        UNIT_ASSERT(limiter->CanIncrease(25));
        UNIT_ASSERT(!limiter->CanIncrease(26));

        limiter->Increase(20);
        UNIT_ASSERT(limiter->CanIncrease(5));
        UNIT_ASSERT(!limiter->CanIncrease(6));

        limiter->Decrease(10);
        UNIT_ASSERT(limiter->CanIncrease(15));
        UNIT_ASSERT(!limiter->CanIncrease(16));
    }
}

}   // namespace NCloud
