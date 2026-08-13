#include "command.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRemoveNodeXAttrCommand final: public TFileStoreCommand
{
private:
    ui64 NodeId = 0;
    TString Name;

public:
    TRemoveNodeXAttrCommand()
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

        auto request = CreateRequest<NProto::TRemoveNodeXAttrRequest>();
        request->SetNodeId(NodeId);
        request->SetName(Name);

        auto response = WaitFor(
            session.RemoveNodeXAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewRemoveNodeXAttrCommand()
{
    return std::make_shared<TRemoveNodeXAttrCommand>();
}

}   // namespace NCloud::NFileStore::NClient
