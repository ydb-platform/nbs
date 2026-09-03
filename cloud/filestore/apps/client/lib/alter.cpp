#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAlterCommand final
    : public TFileStoreCommand
{
private:
    TString CloudId;
    TString FolderId;
    ui32 ConfigVersion = 0;

public:
    TAlterCommand()
    {
        Opts.AddLongOption("cloud")
            .Required()
            .RequiredArgument("STR")
            .Help("new cloud id")
            .StoreResult(&CloudId);

        Opts.AddLongOption("folder")
            .Required()
            .RequiredArgument("STR")
            .Help("new folder id")
            .StoreResult(&FolderId);

        Opts.AddLongOption("config-version")
            .RequiredArgument("NUM")
            .StoreResult(&ConfigVersion);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TAlterFileStoreRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetCloudId(CloudId);
        request->SetFolderId(FolderId);
        request->SetConfigVersion(ConfigVersion);

        auto response = WaitFor(
            Client->AlterFileStore(
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

TCommandPtr NewAlterCommand()
{
    return std::make_shared<TAlterCommand>();
}

}   // namespace NCloud::NFileStore::NClient
