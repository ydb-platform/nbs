#include "command.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSetNodeXAttrCommand final: public TFileStoreCommand
{
private:
    ui64 NodeId = 0;
    TString Name;
    TString Value;
    bool Create = false;
    bool Replace = false;

public:
    TSetNodeXAttrCommand()
    {
        Opts.AddLongOption("node-id")
            .Required()
            .RequiredArgument("NODE_ID")
            .StoreResult(&NodeId);

        Opts.AddLongOption("name")
            .Required()
            .RequiredArgument("NAME")
            .Help("attribute name, e.g. user.attr")
            .StoreResult(&Name);

        Opts.AddLongOption("value")
            .Required()
            .RequiredArgument("VALUE")
            .StoreResult(&Value);

        Opts.AddLongOption("create")
            .Optional()
            .NoArgument()
            .Help("fail if the attribute already exists")
            .StoreTrue(&Create);

        Opts.AddLongOption("replace")
            .Optional()
            .NoArgument()
            .Help("fail if the attribute does not exist")
            .StoreTrue(&Replace);
    }

    bool Execute() override
    {
        Y_ENSURE(
            !(Create && Replace),
            "--create and --replace are mutually exclusive");

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto request = CreateRequest<NProto::TSetNodeXAttrRequest>();
        request->SetNodeId(NodeId);
        request->SetName(Name);
        request->SetValue(Value);

        if (Create) {
            request->SetFlags(
                ProtoFlag(NProto::TSetNodeXAttrRequest::F_CREATE));
        }
        if (Replace) {
            request->SetFlags(
                ProtoFlag(NProto::TSetNodeXAttrRequest::F_REPLACE));
        }

        auto response = WaitFor(
            session.SetNodeXAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewSetNodeXAttrCommand()
{
    return std::make_shared<TSetNodeXAttrCommand>();
}

}   // namespace NCloud::NFileStore::NClient
