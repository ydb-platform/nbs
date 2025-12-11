#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListClusterNodesCommand final: public TFileStoreCommand
{
public:
    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TListClusterNodesRequest>();
        request->SetFileSystemId(FileSystemId);

        auto response = WaitFor(Client->ListClusterNodes(
            std::move(callContext),
            std::move(request)));

        if (HasError(response)) {
            ythrow TServiceError(response.GetError());
        }

        if (JsonOutput) {
            response.PrintJSON(Cout);
        } else {
            for (const auto& node: response.GetNodes()) {
                Cout << DumpMessage(node) << Endl;
            }
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListClusterNodesCommand()
{
    return std::make_shared<TListClusterNodesCommand>();
}

}   // namespace NCloud::NFileStore::NClient
