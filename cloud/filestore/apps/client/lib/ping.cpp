#include "command.h"

#include <cloud/filestore/libs/client/durable.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPingCommand final:
    public TCommand
{
    TString Endpoint;
    IFileStoreServicePtr FileStoreClient;
    IEndpointManagerPtr EndpointClient;

public:
    TPingCommand()
    {
        Opts.AddLongOption("endpoint")
            .Required()
            .RequiredArgument("ENDPOINT")
            .Choices({"filestore", "vhost"})
            .StoreResult(&Endpoint);
    }

    void Init() override
    {
        TCommand::Init();

        if (Endpoint == "filestore") {
            FileStoreClient = CreateDurableClient(
                Logging,
                Timer,
                Scheduler,
                CreateRetryPolicy(ClientConfig),
                CreateFileStoreClient(ClientConfig, Logging));
        } else {
            EndpointClient = CreateDurableClient(
                Logging,
                Timer,
                Scheduler,
                CreateRetryPolicy(ClientConfig),
                CreateEndpointManagerClient(ClientConfig, Logging));
        }
    }

    void Start() override
    {
        TCommand::Start();

        if (FileStoreClient) {
            FileStoreClient->Start();
        } else {
            EndpointClient->Start();
        }
    }

    void Stop() override
    {
        TCommand::Stop();

        if (FileStoreClient) {
            FileStoreClient->Stop();
        } else {
            EndpointClient->Stop();
        }
    }

private:
    auto CallClient()
    {
        auto callContext = PrepareCallContext();
        auto request = std::make_shared<NProto::TPingRequest>();

        return FileStoreClient ?
            FileStoreClient->Ping(std::move(callContext), std::move(request)):
            EndpointClient->Ping(std::move(callContext),std::move(request));
    }

public:
    bool Execute() override
    {
        auto response = WaitFor(CallClient());

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        Cout << "OK" << Endl;

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewPingCommand()
{
    return std::make_shared<TPingCommand>();
}

}   // namespace NCloud::NFileStore::NClient
