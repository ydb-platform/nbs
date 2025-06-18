#include <cloud/blockstore/libs/disk_agent/bootstrap.h>
#include <cloud/blockstore/libs/rdma/impl/server.h>
#include <cloud/blockstore/libs/rdma/impl/verbs.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>

#include <cloud/storage/core/libs/daemon/app.h>

#include <contrib/ydb/core/driver_lib/run/factories.h>
#include <contrib/ydb/core/security/ticket_parser.h>

#include <util/generic/yexception.h>

////////////////////////////////////////////////////////////////////////////////
void dummy_use(void* ptr)
{
    asm volatile("" : : "r"(ptr) : "memory");   // Prevent optimization
}
int main(int argc, char** argv)
{
    void* leak = malloc(100);
    int* leak2 = new int[50];
    dummy_use(leak);
    dummy_use(leak2);

    using namespace NCloud::NBlockStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimr::CreateTicketParser;

    auto serverModuleFactories =
        std::make_shared<NServer::TServerModuleFactories>();
    serverModuleFactories->SpdkFactory = [] (
        NSpdk::TSpdkEnvConfigPtr config)
    {
        Y_UNUSED(config);
        return NServer::TSpdkParts {
            .Env = NSpdk::CreateEnvStub(),
            .LogInitializer = {},
        };
    };

    serverModuleFactories->RdmaServerFactory = [] (
        NCloud::ILoggingServicePtr logging,
        NCloud::IMonitoringServicePtr monitoring,
        NRdma::TServerConfigPtr config)
    {
        return NRdma::CreateServer(
            NRdma::NVerbs::CreateVerbs(),
            std::move(logging),
            std::move(monitoring),
            std::move(config));
    };

    NServer::TBootstrap bootstrap(
        std::move(moduleFactories),
        std::move(serverModuleFactories));
    return NCloud::DoMain(bootstrap, argc, argv);
}
