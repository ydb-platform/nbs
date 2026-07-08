#include "silk_env.h"

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSilkEnv: public ::testing::Environment
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

::testing::Environment* MakeSilkTestEnv()
{
    return new TSilkEnv;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
