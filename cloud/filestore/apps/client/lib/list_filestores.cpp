#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListFileStoresCommand final
    : public TFileStoreServiceCommand
{
public:
    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TListFileStoresRequest>();
        auto response = WaitFor(
            Client->ListFileStores(
                std::move(callContext),
                std::move(request)));

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        if (JsonOutput) {
            response.PrintJSON(Cout);
        } else {
            for (const auto& filestore: response.GetFileStores()) {
                Cout << filestore << Endl;
            }
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewListFileStoresCommand()
{
    return std::make_shared<TListFileStoresCommand>();
}

}   // namespace NCloud::NFileStore::NClient
