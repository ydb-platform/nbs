#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMkDirCommand final
    : public TFileStoreCommand
{
private:
    TString Path;
    bool ShouldCreateParents = false;

public:
    TMkDirCommand()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);

        Opts.AddLongOption('p', "parents")
            .NoArgument()
            .SetFlag(&ShouldCreateParents);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto makeDir = [&] (ui64 nodeId, TStringBuf name) {
            auto request = CreateRequest<NProto::TCreateNodeRequest>();
            request->SetNodeId(nodeId);
            request->SetName(ToString(name));
            request->MutableDirectory()->SetMode(MODE0777);

            auto response = WaitFor(session.CreateNode(
                PrepareCallContext(),
                std::move(request)));

            CheckResponse(response);

            return response.GetNode();
        };

        auto resolved = ResolvePath(session, Path, true);

        if (resolved.size() == 1) {
            return true;
        }

        Y_ABORT_UNLESS(resolved.size() >= 2);

        if (ShouldCreateParents) {
            ui64 nodeId = resolved[0].Node.GetId();
            for (ui32 i = 1; i < resolved.size() - 1; ++i) {
                if (resolved[i].Node.GetType() == NProto::E_INVALID_NODE) {
                    resolved[i].Node = makeDir(nodeId, resolved[i].Name);
                }

                nodeId = resolved[i].Node.GetId();
            }
        }

        Y_ENSURE(
            resolved.back().Node.GetType() == NProto::E_INVALID_NODE,
            "target node already exists"
        );

        const auto& parent = resolved[resolved.size() - 2];

        Y_ENSURE(
            parent.Node.GetType() == NProto::E_DIRECTORY_NODE,
            TStringBuilder() << "target parent is not a directory"
        );

        makeDir(parent.Node.GetId(), resolved.back().Name);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewMkDirCommand()
{
    return std::make_shared<TMkDirCommand>();
}

}   // namespace NCloud::NFileStore::NClient
