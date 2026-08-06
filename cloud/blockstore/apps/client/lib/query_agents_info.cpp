#include "query_agents_info.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TQueryAgentsInfoCommand final: public TCommand
{
private:
    TVector<TString> AgentIds;
    TVector<ui32> States;

public:
    explicit TQueryAgentsInfoCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("agent-id", "agent id (several agents can be added at a time)")
            .RequiredArgument("STR")
            .AppendTo(&AgentIds);

        Opts.AddLongOption("state", "agent state (0=online, 1=warning, 2=unavailable)")
            .RequiredArgument("NUM")
            .AppendTo(&States);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading QueryAgentsInfo request");
        auto request = std::make_shared<NProto::TQueryAgentsInfoRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            auto* filter = request->MutableFilter();
            filter->MutableAgentIds()->Assign(
                AgentIds.begin(),
                AgentIds.end());
            for (auto state: States) {
                filter->MutableStates()->Add(
                    static_cast<NProto::EAgentState>(state));
            }
        }

        STORAGE_DEBUG("Sending QueryAgentsInfo request");
        auto result = WaitFor(ClientEndpoint->QueryAgentsInfo(
            MakeIntrusive<TCallContext>(),
            std::move(request)));

        STORAGE_DEBUG("Received QueryAgentsInfo response");
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

TCommandPtr NewQueryAgentsInfoCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TQueryAgentsInfoCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
