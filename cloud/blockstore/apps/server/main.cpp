#include <cloud/blockstore/libs/daemon/ydb/bootstrap.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/service/device_handler.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>

#include <cloud/storage/core/libs/daemon/app.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/config.h>

#include <ydb/core/driver_lib/run/factories.h>
#include <ydb/core/security/ticket_parser.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimr::CreateTicketParser;

    auto serverModuleFactories =
        std::make_shared<NServer::TServerModuleFactories>();
    serverModuleFactories->LogbrokerServiceFactory = [] (
        NLogbroker::TLogbrokerConfigPtr config,
        NCloud::ILoggingServicePtr logging)
    {
        Y_UNUSED(config);
        return NLogbroker::CreateServiceNull(logging);
    };

    serverModuleFactories->IamClientFactory = [] (
        NCloud::NIamClient::TIamClientConfigPtr config,
        NCloud::ILoggingServicePtr logging,
        NCloud::ISchedulerPtr scheduler,
        NCloud::ITimerPtr timer)
    {
        Y_UNUSED(config);
        Y_UNUSED(logging);
        Y_UNUSED(scheduler);
        Y_UNUSED(timer);
        return NCloud::NIamClient::CreateIamTokenClientStub();
    };

    serverModuleFactories->SpdkFactory = [] (
        NSpdk::TSpdkEnvConfigPtr config)
    {
        Y_UNUSED(config);
        return NServer::TSpdkParts {
            .Env = NSpdk::CreateEnvStub(),
            .VhostCallbacks = {},
            .LogInitializer = {},
        };
    };

    NServer::TBootstrapYdb bootstrap(
        std::move(moduleFactories),
        std::move(serverModuleFactories),
        CreateDefaultDeviceHandlerFactory());
    return NCloud::DoMain(bootstrap, argc, argv);
}
