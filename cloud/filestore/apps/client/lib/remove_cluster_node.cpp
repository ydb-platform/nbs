#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRemoveClusterNodeCommand final
    : public TFileStoreCommand
{
private:
    TString NodeId;

public:
    TRemoveClusterNodeCommand()
    {
        Opts.AddLongOption("node")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&NodeId);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TRemoveClusterNodeRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetNodeId(NodeId);

        auto response = WaitFor(
            Client->RemoveClusterNode(
                std::move(callContext),
                std::move(request)));

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewRemoveClusterNodeCommand()
{
    return std::make_shared<TRemoveClusterNodeCommand>();
}

}   // namespace NCloud::NFileStore::NClient
