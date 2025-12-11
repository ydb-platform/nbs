#include "list_keyrings.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListKeyringsCommand final: public TCommand
{
public:
    TListKeyringsCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {}

protected:
    bool DoExecute() override
    {
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Sending ListKeyrings request");
        auto result = WaitFor(ClientEndpoint->ListKeyrings(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TListKeyringsRequest>()));

        STORAGE_DEBUG("Received ListKeyrings response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        const auto& endpoints = result.GetEndpoints();
        for (const auto& endpoint: endpoints) {
            const auto& request = endpoint.GetRequest();
            output << "keyring-id: " << endpoint.GetKeyringId()
                   << " socket: " << request.GetUnixSocketPath().Quote()
                   << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListKeyringsCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TListKeyringsCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
