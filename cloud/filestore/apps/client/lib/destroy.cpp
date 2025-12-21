#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyCommand final
    : public TFileStoreCommand
{
private:
    bool ForceDestroy = false;

public:
    TDestroyCommand()
    {
        Opts.AddLongOption("force").StoreTrue(&ForceDestroy);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TDestroyFileStoreRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetForceDestroy(ForceDestroy);

        auto response = WaitFor(
            Client->DestroyFileStore(
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

TCommandPtr NewDestroyCommand()
{
    return std::make_shared<TDestroyCommand>();
}

}   // namespace NCloud::NFileStore::NClient
