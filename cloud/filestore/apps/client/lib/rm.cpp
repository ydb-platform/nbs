#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRmCommand final
    : public TFileStoreCommand
{
private:
    TString Path;
    bool RemoveDir = false;

public:
    TRmCommand()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);

        Opts.AddLongOption('r', "recursive")
            .NoArgument()
            .SetFlag(&RemoveDir);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        const auto resolved = ResolvePath(session, Path, false);

        Y_ENSURE(resolved.size() >= 2, "can't rm root node");

        auto request = CreateRequest<NProto::TUnlinkNodeRequest>();

        request->SetNodeId(resolved[resolved.size() - 2].Node.GetId());
        request->SetName(ToString(resolved.back().Name));
        request->SetUnlinkDirectory(RemoveDir);

        auto response = WaitFor(session.UnlinkNode(
            PrepareCallContext(),
            std::move(request)));

        CheckResponse(response);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewRmCommand()
{
    return std::make_shared<TRmCommand>();
}

}   // namespace NCloud::NFileStore::NClient
