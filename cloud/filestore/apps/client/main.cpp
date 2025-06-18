#include <cloud/filestore/apps/client/lib/app.h>
#include <cloud/filestore/apps/client/lib/bootstrap.h>
#include <cloud/storage/core/libs/iam/iface/client.h>

////////////////////////////////////////////////////////////////////////////////
void dummy_use(void* ptr)
{
    asm volatile("" : : "r"(ptr) : "memory");   // Prevent optimization
}
int main(int argc, char** argv)
{
    using namespace NCloud::NFileStore::NClient;

    void* leak = malloc(100);
    int* leak2 = new int[50];
    dummy_use(leak);
    dummy_use(leak2);

    auto clientFactories = std::make_shared<TClientFactories>();

    clientFactories->IamClientFactory = [] (
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

    ConfigureSignals();
    return TApp::Instance().Run(std::move(clientFactories), argc, argv);
}
