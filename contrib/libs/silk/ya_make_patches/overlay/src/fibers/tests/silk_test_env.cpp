#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

#include <gtest/gtest.h>

namespace {

class SilkFibersEnvironment : public ::testing::Environment
{
public:
    void SetUp() override
    {
        silk::initialize();
        silk::FiberScheduler::initialize();
    }
    void TearDown() override
    {
        silk::FiberScheduler::destroy();
        silk::destroy();
    }
};

[[maybe_unused]] auto * const env =
    ::testing::AddGlobalTestEnvironment(new SilkFibersEnvironment);

} // namespace
