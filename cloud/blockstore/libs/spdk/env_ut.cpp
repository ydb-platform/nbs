#include "config.h"

#include "env_impl.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NSpdk {

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration WaitTimeout = TDuration::Seconds(5);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkEnvTest)
{
    Y_UNIT_TEST(ShouldExecuteRequestsInSpdkThread)
    {
        auto logging = CreateLoggingService("console");
        InitLogging(logging->CreateLog("SPDK"));

        auto config = std::make_shared<TSpdkEnvConfig>();
        auto env = std::make_shared<TSpdkEnv>(std::move(config));

        env->Start();

        bool executed = false;
        auto result = env->Execute([&] {
            executed = true;
            return true;
        }).GetValue(WaitTimeout);

        UNIT_ASSERT(executed);
        UNIT_ASSERT(result);

        env->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NSpdk
