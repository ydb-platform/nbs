#include <cloud/blockstore/libs/daemon/ydb/bootstrap.h>
#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/logbroker/pqimpl/logbroker.h>
#include <cloud/blockstore/libs/service/device_handler.h>

#include <cloud/storage/core/libs/daemon/app.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/config.h>
#include <cloud/storage/core/libs/iam/impl/token_client.h>

#include <kikimr/yndx/security/ticket_parser.h>

#include <ydb/core/driver_lib/run/factories.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    auto moduleFactories = std::make_shared<NKikimr::TModuleFactories>();
    moduleFactories->CreateTicketParser = NKikimrYandex::CreateTicketParser;

    auto serverModuleFactories =
        std::make_shared<NServer::TServerModuleFactories>();
    serverModuleFactories->LogbrokerServiceFactory = [] (
        NLogbroker::TLogbrokerConfigPtr config,
        NCloud::ILoggingServicePtr logging)
    {
        return config->GetAddress()
            ? NLogbroker::CreateService(std::move(config), std::move(logging))
            : NLogbroker::CreateServiceNull(std::move(logging));
    };

    serverModuleFactories->IamClientFactory = [] (
        NCloud::NIamClient::TIamClientConfigPtr config,
        NCloud::ILoggingServicePtr logging,
        NCloud::ISchedulerPtr scheduler,
        NCloud::ITimerPtr timer)
    {
        if (config->IsValid()) {
            return NCloud::NIamClient::CreateIamTokenClient(
                std::move(config),
                std::move(logging),
                std::move(scheduler),
                std::move(timer));
        }

        return NCloud::NIamClient::CreateIamTokenClientStub();
    };

    NServer::TBootstrapYdb bootstrap(
        std::move(moduleFactories),
        std::move(serverModuleFactories),
        CreateDefaultDeviceHandlerFactory());
    return NCloud::DoMain(bootstrap, argc, argv);
}
