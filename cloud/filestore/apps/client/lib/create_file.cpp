#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateFileCommand final: public TFileStoreCommand
{
private:
    TString Path;
    bool ShouldCreateParents = false;

public:
    TCreateFileCommand()
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
        CreateSession();

        auto makeNode = [&](ui64 nodeId, TStringBuf name, bool isDir)
        {
            auto request = CreateRequest<NProto::TCreateNodeRequest>();
            request->SetNodeId(nodeId);
            request->SetName(ToString(name));
            if (isDir) {
                request->MutableDirectory()->SetMode(MODE0777);
            } else {
                request->MutableFile()->SetMode(MODE0777);
            }

            auto response = WaitFor(
                Client->CreateNode(PrepareCallContext(), std::move(request)));

            CheckResponse(response);

            return response.GetNode();
        };

        auto resolved = ResolvePath(Path, true);

        if (resolved.size() == 1) {
            return true;
        }

        Y_ABORT_UNLESS(resolved.size() >= 2);

        if (ShouldCreateParents) {
            ui64 nodeId = resolved[0].Node.GetId();
            for (ui32 i = 1; i < resolved.size() - 1; ++i) {
                if (resolved[i].Node.GetType() == NProto::E_INVALID_NODE) {
                    resolved[i].Node = makeNode(nodeId, resolved[i].Name, true);
                }

                nodeId = resolved[i].Node.GetId();
            }
        }

        Y_ENSURE(
            resolved.back().Node.GetType() == NProto::E_INVALID_NODE,
            "target node already exists");

        const auto& parent = resolved[resolved.size() - 2];

        Y_ENSURE(
            parent.Node.GetType() == NProto::E_DIRECTORY_NODE,
            TStringBuilder() << "target parent is not a directory");

        makeNode(parent.Node.GetId(), resolved.back().Name, false);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateFileCommand()
{
    return std::make_shared<TCreateFileCommand>();
}

}   // namespace NCloud::NFileStore::NClient
