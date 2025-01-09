#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMvCommand final
    : public TFileStoreCommand
{
private:
    TString SrcPath;
    TString DstPath;

public:
    TMvCommand()
    {
        Opts.AddLongOption("src-path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&SrcPath);

        Opts.AddLongOption("dst-path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&DstPath);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        const auto src = ResolvePath(session, SrcPath, false);
        const auto dst = ResolvePath(session, DstPath, true);

        Y_ENSURE(src.size() >= 2, "root node can't be mv source");
        Y_ENSURE(dst.size() >= 2, "root node can't be mv destination");

        auto request = CreateRequest<NProto::TRenameNodeRequest>();

        request->SetNodeId(src[src.size() - 2].Node.GetId());
        request->SetName(ToString(src.back().Name));
        request->SetNewParentId(dst[dst.size() - 2].Node.GetId());
        request->SetNewName(ToString(dst.back().Name));

        auto response = WaitFor(session.RenameNode(
            PrepareCallContext(),
            std::move(request)));

        CheckResponse(response);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewMvCommand()
{
    return std::make_shared<TMvCommand>();
}

}   // namespace NCloud::NFileStore::NClient
