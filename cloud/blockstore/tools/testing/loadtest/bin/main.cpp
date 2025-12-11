#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
#include <cloud/blockstore/tools/testing/loadtest/lib/app.h>
#include <cloud/blockstore/tools/testing/loadtest/lib/bootstrap.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    auto moduleFactories = std::make_shared<NLoadTest::TModuleFactories>();
    moduleFactories->SpdkFactory = [](NSpdk::TSpdkEnvConfigPtr config)
    {
        Y_UNUSED(config);
        return NLoadTest::TSpdkParts{
            .Env = NSpdk::CreateEnvStub(),
            .LogInitializer = {},
        };
    };

    return NLoadTest::DoMain(argc, argv, std::move(moduleFactories));
}
