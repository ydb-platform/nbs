#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStopEndpointCommand final: public TEndpointCommand
{
private:
    TString SocketPath;

public:
    TStopEndpointCommand()
    {
        Opts.AddLongOption("socket-path")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&SocketPath);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TStopEndpointRequest>();
        request->SetSocketPath(SocketPath);

        auto response = WaitFor(
            Client->StopEndpoint(std::move(callContext), std::move(request)));

        if (HasError(response)) {
            ythrow TServiceError(response.GetError());
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStopEndpointCommand()
{
    return std::make_shared<TStopEndpointCommand>();
}

}   // namespace NCloud::NFileStore::NClient
