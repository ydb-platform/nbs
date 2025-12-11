#include <cloud/blockstore/apps/client/lib/app.h>
#include <cloud/blockstore/apps/client/lib/bootstrap.h>

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char* argv[])
{
    using namespace NCloud::NBlockStore::NClient;

    auto clientFactories = std::make_shared<TClientFactories>();

    clientFactories->IamClientFactory =
        [](NCloud::NIamClient::TIamClientConfigPtr config,
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

    ConfigureSignals();
    return TApp::Instance().Run(clientFactories, argc, argv);
}
