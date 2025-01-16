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
    ui64 NodeId = 0;

public:
    TRmCommand()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);

        Opts.AddLongOption("node")
            .DefaultValue(0)
            .RequiredArgument("ID")
            .StoreResult(&NodeId);

        Opts.AddLongOption('r', "recursive")
            .NoArgument()
            .SetFlag(&RemoveDir);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        // If nodeId is set, treat it as a nodeId and path as a name. Otherwise,
        // resolve path to get nodeId and name.
        if (!NodeId) {
            const auto resolved = ResolvePath(session, Path, false);
            Y_ENSURE(resolved.size() >= 2, "can't rm root node");

            NodeId = resolved[resolved.size() - 2].Node.GetId();
            Path = ToString(resolved.back().Name);
        }

        auto request = CreateRequest<NProto::TUnlinkNodeRequest>();

        request->SetNodeId(NodeId);
        request->SetName(Path);
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
