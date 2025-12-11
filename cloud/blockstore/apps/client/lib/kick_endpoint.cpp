#include "kick_endpoint.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TKickEndpointCommand final: public TCommand
{
private:
    ui32 KeyringId;

public:
    TKickEndpointCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("keyring-id", "endpoint keyring identifier")
            .RequiredArgument("NUM")
            .StoreResult(&KeyringId);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading KickEndpoint request");
        auto request = std::make_shared<NProto::TKickEndpointRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetKeyringId(KeyringId);
        }

        STORAGE_DEBUG("Sending KickEndpoint request");
        auto result = WaitFor(ClientEndpoint->KickEndpoint(
            MakeIntrusive<TCallContext>(),
            std::move(request)));

        STORAGE_DEBUG("Received KickEndpoint response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << "OK" << Endl;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewKickEndpointCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TKickEndpointCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
