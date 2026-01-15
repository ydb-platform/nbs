#include "command.h"

#include "performance_profile_params.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResizeCommand final
    : public TFileStoreCommand
{
private:
    const TPerformanceProfileParams PerformanceProfileParams;

    ui64 BlocksCount = 0;
    bool Force = false;
    ui32 ShardCount = 0;
    bool EnableStrictFileSystemSizeEnforcement = false;
    bool EnableDirectoryCreationInShards = false;

public:
    TResizeCommand()
        : PerformanceProfileParams(Opts)
    {
        Opts.AddLongOption("blocks-count")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("force")
            .StoreTrue(&Force)
            .Help("force flag allows to decrease the size of the file store");

        Opts.AddLongOption("shard-count")
            .RequiredArgument("NUM")
            .Help("explicitly specifies the required shard count")
            .StoreResult(&ShardCount);

        Opts.AddLongOption("enable-strict")
            .StoreTrue(&EnableStrictFileSystemSizeEnforcement)
            .Help("enable strict file system size enforcement");

        Opts.AddLongOption("enable-directory-creation-in-shards")
            .StoreTrue(&EnableDirectoryCreationInShards)
            .Help("enable directory creation in shards");
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TResizeFileStoreRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetBlocksCount(BlocksCount);
        request->SetForce(Force);
        request->SetShardCount(ShardCount);
        request->SetEnableStrictFileSystemSizeEnforcement(
            EnableStrictFileSystemSizeEnforcement);
        request->SetEnableDirectoryCreationInShards(EnableDirectoryCreationInShards);

        PerformanceProfileParams.FillRequest(*request);

        auto response = WaitFor(
            Client->ResizeFileStore(
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

TCommandPtr NewResizeCommand()
{
    return std::make_shared<TResizeCommand>();
}

}   // namespace NCloud::NFileStore::NClient
