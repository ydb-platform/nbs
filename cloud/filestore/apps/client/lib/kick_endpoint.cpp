#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TKickEndpointCommand final: public TEndpointCommand
{
private:
    ui32 KeyringId;

public:
    TKickEndpointCommand()
    {
        Opts.AddLongOption("keyring-id")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&KeyringId);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TKickEndpointRequest>();
        request->SetKeyringId(KeyringId);

        auto response = WaitFor(
            Client->KickEndpoint(std::move(callContext), std::move(request)));

        if (HasError(response)) {
            ythrow TServiceError(response.GetError());
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewKickEndpointCommand()
{
    return std::make_shared<TKickEndpointCommand>();
}

}   // namespace NCloud::NFileStore::NClient
