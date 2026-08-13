#include "command.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetNodeXAttrCommand final: public TFileStoreCommand
{
private:
    ui64 NodeId = 0;
    TString Name;

public:
    TGetNodeXAttrCommand()
    {
        Opts.AddLongOption("node-id")
            .Required()
            .RequiredArgument("NODE_ID")
            .StoreResult(&NodeId);

        Opts.AddLongOption("name")
            .Required()
            .RequiredArgument("NAME")
            .StoreResult(&Name);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto request = CreateRequest<NProto::TGetNodeXAttrRequest>();
        request->SetNodeId(NodeId);
        request->SetName(Name);

        auto response = WaitFor(
            session.GetNodeXAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);

        if (JsonOutput) {
            NJson::TJsonValue json;
            json.InsertValue("Name", Name);
            json.InsertValue("Value", response.GetValue());
            json.InsertValue("Version", response.GetVersion());
            Cout << NJson::WriteJson(json) << Endl;
        } else {
            Cout << response.GetValue() << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewGetNodeXAttrCommand()
{
    return std::make_shared<TGetNodeXAttrCommand>();
}

}   // namespace NCloud::NFileStore::NClient
