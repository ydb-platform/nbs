#include "query_known_storage.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TQueryKnownStorageCommand final: public TCommand
{
private:
    TVector<TString> AgentIds;

public:
    explicit TQueryKnownStorageCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption(
                "agent-id",
                "agent id (several agents can be added at a time)")
            .RequiredArgument("STR")
            .AppendTo(&AgentIds);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading QueryKnownStorage request");
        auto request = std::make_shared<NProto::TQueryKnownStorageRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->MutableAgentIds()->Assign(
                AgentIds.begin(),
                AgentIds.end());
        }

        STORAGE_DEBUG("Sending QueryKnownStorage request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->QueryKnownStorage(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received QueryKnownStorage response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        TString str;
        google::protobuf::util::MessageToJsonString(result, &str);
        output << str << Endl;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewQueryKnownStorageCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TQueryKnownStorageCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
