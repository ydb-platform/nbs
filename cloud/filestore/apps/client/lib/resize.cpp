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

public:
    TResizeCommand()
        : PerformanceProfileParams(Opts)
    {
        Opts.AddLongOption("blocks-count")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TResizeFileStoreRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetBlocksCount(BlocksCount);

        PerformanceProfileParams.FillRequest(*request);

        auto response = WaitFor(
            Client->ResizeFileStore(
                std::move(callContext),
                std::move(request)));

        if (HasError(response)) {
            ythrow TServiceError(response.GetError());
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
