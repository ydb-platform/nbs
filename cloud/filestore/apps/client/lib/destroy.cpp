#include "command.h"

#include <util/stream/input.h>
#include <util/stream/output.h>

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
        Cerr << "Confirm filesystem destruction by typing filesystem id to stdin"
             << Endl;
        TString confirmation;
        Cin >> confirmation;
        Y_ENSURE(
            confirmation == FileSystemId,
            "Confirmation failed: " << confirmation.Quote()
                << " != " << FileSystemId.Quote());

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
