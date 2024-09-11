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
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TResizeFileStoreRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetBlocksCount(BlocksCount);
        request->SetForce(Force);

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
