#include "bootstrap.h"

#include <cloud/blockstore/libs/daemon/testlib/bootstrap_metrics.h>

namespace NCloud::NBlockStore::NServer {

Y_UNIT_TEST_SUITE(TDiskAgentBootstrapMetricsTest)
{
    Y_UNIT_TEST(ShouldExposeMetricsBeforeNodeRegistrationCompletes)
    {
        NTests::TBootstrapMetricsEnv env;
        TBootstrap bootstrap(
            std::make_shared<NKikimr::TModuleFactories>(),
            std::make_shared<TServerModuleFactories>());
        env.CheckRegistration(bootstrap);
    }
}

}   // namespace NCloud::NBlockStore::NServer
