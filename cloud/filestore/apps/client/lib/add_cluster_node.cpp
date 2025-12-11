#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAddClusterNodeCommand final: public TFileStoreCommand
{
private:
    TString NodeId;

public:
    TAddClusterNodeCommand()
    {
        Opts.AddLongOption("node")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&NodeId);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TAddClusterNodeRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetNodeId(NodeId);

        auto response = WaitFor(
            Client->AddClusterNode(std::move(callContext), std::move(request)));

        if (HasError(response)) {
            ythrow TServiceError(response.GetError());
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAddClusterNodeCommand()
{
    return std::make_shared<TAddClusterNodeCommand>();
}

}   // namespace NCloud::NFileStore::NClient
