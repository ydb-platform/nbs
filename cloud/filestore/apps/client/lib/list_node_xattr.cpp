#include "command.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListNodeXAttrCommand final: public TFileStoreCommand
{
private:
    ui64 NodeId = 0;

public:
    TListNodeXAttrCommand()
    {
        Opts.AddLongOption("node-id")
            .Required()
            .RequiredArgument("NODE_ID")
            .StoreResult(&NodeId);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto request = CreateRequest<NProto::TListNodeXAttrRequest>();
        request->SetNodeId(NodeId);

        auto response = WaitFor(
            session.ListNodeXAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);

        if (JsonOutput) {
            NJson::TJsonValue names(NJson::JSON_ARRAY);
            for (const auto& name: response.GetNames()) {
                names.AppendValue(name);
            }
            NJson::TJsonValue json;
            json.InsertValue("Names", std::move(names));
            Cout << NJson::WriteJson(json) << Endl;
        } else {
            for (const auto& name: response.GetNames()) {
                Cout << name << Endl;
            }
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListNodeXAttrCommand()
{
    return std::make_shared<TListNodeXAttrCommand>();
}

}   // namespace NCloud::NFileStore::NClient
